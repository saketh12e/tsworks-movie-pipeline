"""MovieLens + OMDb pipeline: clean two CSV files, enrich the movies, load SQLite.

Run it from the repository folder:

    python etl.py --offline            # clean and load the CSV files, no API calls
    python etl.py --max-api-calls 50   # clean, look movies up on OMDb, then load

By default the CSV files, .env, database, OMDb cache and reports live in
../runtime next to this repository. Run one ETL process at a time.
"""

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sqlite3
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import requests
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_WORK_DIR = SCRIPT_DIR.parent / "runtime"
SCHEMA_PATH = SCRIPT_DIR / "schema.sql"
DATABASE_VERSION = 2  # must match PRAGMA user_version in schema.sql

MOVIE_COLUMNS = ("movieId", "title", "genres")
RATING_COLUMNS = ("userId", "movieId", "rating", "timestamp")
MAX_SQLITE_INTEGER = 2**63 - 1
ALLOWED_RATINGS = tuple(Decimal(n) / 2 for n in range(1, 11))  # 0.5, 1, 1.5, ..., 5
FUTURE_TOLERANCE = timedelta(hours=24)
DETAIL_LENGTH = 80

OMDB_URL = "https://www.omdbapi.com/"
RUN_REQUEST_LIMIT = 200
DAILY_REQUEST_LIMIT = 900  # rolling 24 hours; the free key allows 1,000 a day
MOVIE_REQUEST_LIMIT = 6
ATTEMPTS_PER_REQUEST = 3
REQUEST_TIMEOUT = (3, 15)  # connect and read seconds
RETRY_WAITS = (2, 5)  # seconds to wait after the first and later failed attempts
MAX_WAIT_SECONDS = 30
MAX_RESPONSE_BYTES = 1024 * 1024
SUCCESS_TTL = timedelta(days=30)
NOT_FOUND_TTL = timedelta(hours=24)
UNREACHABLE_MOVIE_LIMIT = 3
CONNECTION_ERRORS = ("timeout", "connection_error")
MEDIA_TYPES = ("movie", "series", "episode")
NEGATIVE_ERRORS = ("Movie not found!", "Too many results.")  # genuine "no answer" replies; the second only for searches
CACHE_KEY_FORMAT = "1"  # part of every cache key; kept so existing cache files stay usable

# Tables and columns the pipeline reads or writes; an existing database must have them all.
REQUIRED_COLUMNS = {
    "movies": ("movie_id", "source_title", "title", "release_year", "imdb_id", "omdb_year", "media_type",
               "released_on", "plot", "box_office_usd", "enrichment_status", "enrichment_error"),
    "ratings": ("user_id", "movie_id", "rating", "rated_at"),
    "genres": ("genre_id", "name", "name_key"),
    "movie_genres": ("movie_id", "genre_id"),
    "directors": ("director_id", "name", "name_key"),
    "movie_directors": ("movie_id", "director_id"),
    "dataset_manifest": ("singleton_id", "movies_sha256", "ratings_sha256", "validation_as_of_utc"),
}

EXIT_COMPLETE, EXIT_FATAL, EXIT_UNFINISHED, EXIT_REPORT_FAILED = 0, 1, 2, 3

PLAIN_DECIMAL = re.compile(r"[0-9]+(?:\.[0-9]+)?")
TITLE_WITH_YEAR = re.compile(r"(?P<title>.*?)\s*\((?P<year>[1-9][0-9]{3})\)", re.DOTALL)
IMDB_ID = re.compile(r"tt[0-9]{1,12}")
OMDB_YEAR = re.compile(r"(?P<start>[0-9]{4})(?P<range>[\u2013-](?:[0-9]{4})?)?")  # 1995, 1994-1998, 2005-
RELEASED = re.compile(r"(?P<day>[0-9]{1,2}) (?P<month>[A-Z][a-z]{2}) (?P<year>[0-9]{4})")
MONTHS = {name: number for number, name in enumerate(
    ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), start=1)}
DOLLARS = re.compile(r"\$[0-9]{1,3}(?:,[0-9]{3})*")
ARTICLES = ("The", "A", "An", "L'", "La", "Le", "Les", "Il", "El", "Los", "Las", "Der", "Die", "Das", "Un", "Une")
TRAILING_ARTICLE = re.compile(r"(?P<title>.+), (?P<article>" + "|".join(ARTICLES) + ")")
ALTERNATE_TITLE = re.compile(r"(?P<main>.+?) \((?P<alternate>[^()]+)\)")
KNOWN_GENRES = {
    "action", "adventure", "animation", "children", "comedy", "crime", "documentary", "drama",
    "fantasy", "film-noir", "horror", "imax", "musical", "mystery", "romance", "sci-fi",
    "thriller", "war", "western",
}

CACHE_SCHEMA = """
CREATE TABLE IF NOT EXISTS responses (
    cache_key TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('success', 'not_found')),
    body TEXT NOT NULL,
    fetched_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS requests (
    request_id INTEGER PRIMARY KEY,
    reserved_at_utc TEXT NOT NULL,
    outcome TEXT
);
"""


class PipelineError(Exception):
    """A problem that stops the run before anything new is published."""


class FieldError(ValueError):
    """One field of one CSV row is invalid."""

    def __init__(self, field: str, reason: str):
        super().__init__(f"{field}: {reason}")
        self.field = field
        self.reason = reason


# Configuration


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean MovieLens CSV files, enrich movies with OMDb, load SQLite.")
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR,
                        help="folder with raw/ CSV files and .env; outputs go here too (default: ../runtime)")
    parser.add_argument("--db", type=Path, help="database file (default: WORK_DIR/movies.sqlite)")
    parser.add_argument("--offline", action="store_true", help="make no OMDb requests; cached answers are still used")
    parser.add_argument("--max-api-calls", type=int, default=RUN_REQUEST_LIMIT,
                        help=f"HTTP attempts this run may make, retries included (default {RUN_REQUEST_LIMIT})")
    return parser.parse_args(argv)


def prepare_paths(args: argparse.Namespace) -> dict:
    """Resolve and check every path before any work starts.

    The default work folder comes from the script location; relative paths given
    on the command line are resolved from the caller's current directory.
    """
    work_dir = args.work_dir.resolve()
    paths = {
        "work_dir": work_dir,
        "movies_csv": work_dir / "raw" / "movies.csv",
        "ratings_csv": work_dir / "raw" / "ratings.csv",
        "database": (args.db or work_dir / "movies.sqlite").resolve(),
        "cache": work_dir / "cache" / "omdb.sqlite",
        "reports": work_dir / "reports",
    }
    database = paths["database"]
    for path in (work_dir, database):
        if path == SCRIPT_DIR or SCRIPT_DIR in path.parents:
            raise PipelineError(f"{path} is inside the code folder; keep data and outputs outside it")
    if database in (paths["movies_csv"], paths["ratings_csv"], paths["cache"]) or database.is_dir():
        raise PipelineError(f"--db {database} points at an input file, the OMDb cache or a folder")
    for folder in (work_dir, database.parent):
        if not os.access(folder, os.W_OK):
            raise PipelineError(f"{folder} is missing or not writable")
    if args.max_api_calls < 0:
        raise PipelineError("--max-api-calls cannot be negative")
    paths["cache"].parent.mkdir(exist_ok=True)
    paths["reports"].mkdir(exist_ok=True)
    return paths


def load_api_key(work_dir: Path, offline: bool) -> str:
    """Load work_dir/.env without overriding variables already set in the shell."""
    load_dotenv(work_dir / ".env", override=False)
    api_key = (os.getenv("OMDB_API_KEY") or "").strip()
    if not api_key and not offline:
        raise PipelineError(f"OMDB_API_KEY is not set; add it to {work_dir / '.env'} or use --offline")
    return api_key


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def format_utc(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(text: str) -> datetime:
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError:
        raise PipelineError(f"invalid stored UTC time {text!r}") from None


# CSV reading and field parsing


def read_snapshot(path: Path) -> bytes:
    """Read a whole file once, so the hash and the parsed rows come from the same bytes."""
    try:
        data = path.read_bytes()
    except OSError as error:
        raise PipelineError(f"cannot read {path}: {error.strerror}") from None
    if not data.strip():
        raise PipelineError(f"{path} is empty")
    return data


def new_audit(source: str, data: bytes) -> dict:
    """Counts for one input file. Every data row ends as selected, duplicate,
    superseded or rejected; blank lines are counted separately."""
    return {"source": source, "sha256": hashlib.sha256(data).hexdigest(), "records": 0, "blank_lines": 0,
            "selected": 0, "duplicate": 0, "superseded": 0, "rejected": 0}


def reject(rejections: list, audit: dict, record: dict, field: str | None, reason: str, value: str | None = None):
    """Record one rejected row with its row number, starting line and a short copy of the bad value."""
    if value is None:
        value = record["values"].get(field, "") if field else ""
    rejections.append({"source": audit["source"], "row": record["row"], "line": record["line"],
                       "field": field, "reason": reason, "value": value[:DETAIL_LENGTH]})
    audit["rejected"] += 1


def parse_csv(data: bytes, columns: tuple, audit: dict, rejections: list, warnings: Counter) -> list[dict]:
    """Parse CSV bytes into records {"row", "line", "values"}.

    Strict quoting keeps commas and newlines inside quoted titles. A row with the
    wrong number of fields is rejected because its boundaries are still known;
    broken quoting stops the run because they are not.
    """
    source = audit["source"]
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise PipelineError(f"{source} is not valid UTF-8 (byte {error.start})") from None
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    records, positions, width, previous_line = [], None, 0, 0
    try:
        for row in reader:
            line, previous_line = previous_line + 1, reader.line_num
            if not row:
                audit["blank_lines"] += 1
            elif positions is None:
                positions, width = map_columns(row, columns, source, warnings), len(row)
            else:
                audit["records"] += 1
                record = {"row": audit["records"], "line": line, "values": {}}
                if len(row) == width:
                    record["values"] = {name: row[index] for name, index in positions.items()}
                    records.append(record)
                else:
                    reject(rejections, audit, record, None, "field_count", f"{len(row)} fields, expected {width}")
    except csv.Error as error:
        raise PipelineError(f"{source}: broken CSV near line {reader.line_num}: {error}") from None
    if audit["records"] == 0:
        raise PipelineError(f"{source} has no data rows")
    return records


def map_columns(header: list, columns: tuple, source: str, warnings: Counter) -> dict:
    """Find required columns by exact name, in any order."""
    problems = [f"missing column {name!r}" for name in columns if name not in header]
    problems += [f"repeated column {name!r}" for name in columns if header.count(name) > 1]
    if problems:
        raise PipelineError(f"{source}: " + "; ".join(problems))
    if len(header) > len(columns):
        warnings["extra_csv_columns_ignored"] += 1
    return {name: header.index(name) for name in columns}


def parse_id(text: str, field: str, allow_zero: bool = False) -> int:
    """A whole number written with ASCII digits only. Leading zeros are allowed."""
    value = text.strip()
    if not value:
        raise FieldError(field, "missing")
    if not (value.isascii() and value.isdigit()):
        raise FieldError(field, "not_a_whole_number")
    # Strip the zeros first, so a huge zero prefix cannot hit Python's digit limit.
    significant = value.lstrip("0") or "0"
    if len(significant) > len(str(MAX_SQLITE_INTEGER)) or int(significant) > MAX_SQLITE_INTEGER:
        raise FieldError(field, "too_large")
    if significant == "0" and not allow_zero:
        raise FieldError(field, "not_positive")
    return int(significant)


def parse_rating(text: str) -> float:
    """Only the exact half-star values 0.5 to 5.0. Nothing is rounded into range."""
    value = text.strip()
    if not value:
        raise FieldError("rating", "missing")
    if not PLAIN_DECIMAL.fullmatch(value):
        raise FieldError("rating", "not_a_decimal")
    if Decimal(value) not in ALLOWED_RATINGS:
        raise FieldError("rating", "not_a_half_star_value")
    return float(Decimal(value))


def parse_timestamp(text: str, cutoff: datetime) -> int:
    """Unix seconds in UTC. Millisecond values are not converted; they fail as too far ahead."""
    seconds = parse_id(text, "timestamp", allow_zero=True)
    try:
        moment = datetime.fromtimestamp(seconds, UTC)
    except (OverflowError, OSError, ValueError):
        raise FieldError("timestamp", "out_of_range") from None
    if moment > cutoff + FUTURE_TOLERANCE:
        raise FieldError("timestamp", "in_the_future")
    return seconds


def split_title_year(source_title: str) -> tuple[str, int | None]:
    """Take only a final "(YYYY)" off the title; other parentheses stay."""
    text = source_title.strip()
    if not text:
        raise FieldError("title", "missing")
    match = TITLE_WITH_YEAR.fullmatch(text)
    if not match:
        return text, None
    if not match["title"].strip():
        raise FieldError("title", "only_a_year")
    return match["title"].strip(), int(match["year"])


def normalize_name(text: str) -> tuple[str, str]:
    """Return (display name, key). Both use NFC and single spaces; the key is also
    case-folded, so "Sci-Fi" and "sci-fi" share a key while accents are kept."""
    display = " ".join(unicodedata.normalize("NFC", text).split())
    return display, display.casefold()


def parse_genres(raw: str, warnings: Counter) -> dict[str, str]:
    """Split the genres field on "|" into {key: display name}."""
    genres = {}
    tokens = [token.strip() for token in raw.split("|")]
    if raw.strip() and "" in tokens:
        warnings["empty_genre_token"] += 1
    for token in filter(None, tokens):
        name, key = normalize_name(token)
        if key == "(no genres listed)":
            continue
        if key in genres:
            warnings["repeated_genre"] += 1
        elif key not in KNOWN_GENRES:
            warnings["unfamiliar_genre_kept"] += 1
        genres[key] = min(genres.get(key, name), name)
    return genres


# Movie and rating cleaning


def clean_movies(records: list, audit: dict, rejections: list, warnings: Counter) -> tuple[dict, set, dict]:
    """Validate movie rows, then resolve repeated movie IDs among the valid rows.

    Returns (movies, rejected_ids, genre_names). A movie is a dict with movie_id,
    source_title, title, release_year and genres ({key: display name}).
    """
    candidates = defaultdict(list)
    ids_on_bad_rows = set()
    for record in records:
        movie_id = None
        try:
            movie_id = parse_id(record["values"]["movieId"], "movieId")
            title, year = split_title_year(record["values"]["title"])
        except FieldError as error:
            reject(rejections, audit, record, error.field, error.reason)
            if movie_id:
                ids_on_bad_rows.add(movie_id)
            continue
        candidates[movie_id].append({
            "movie_id": movie_id, "source_title": record["values"]["title"], "title": title,
            "release_year": year, "genres": parse_genres(record["values"]["genres"], warnings), "record": record,
        })

    movies, rejected_ids = {}, set()
    for movie_id in sorted(candidates):
        group = candidates[movie_id]
        if len({(c["title"], c["release_year"], frozenset(c["genres"])) for c in group}) > 1:
            # Picking one of several conflicting rows would depend on file order.
            for candidate in group:
                reject(rejections, audit, candidate["record"], "movieId", "conflicting_duplicate")
            rejected_ids.add(movie_id)
            continue
        movies[movie_id] = min(group, key=lambda c: c["source_title"])
        audit["selected"] += 1
        audit["duplicate"] += len(group) - 1
    rejected_ids |= ids_on_bad_rows - movies.keys()

    spellings = defaultdict(set)
    for movie in movies.values():
        for key, name in movie["genres"].items():
            spellings[key].add(name)
    for names in spellings.values():
        if len(names) > 1:
            warnings["genre_spelling_variants"] += 1
    genre_names = {key: min(names) for key, names in sorted(spellings.items())}
    return movies, rejected_ids, genre_names


def clean_ratings(records: list, movies: dict, rejected_ids: set, cutoff: datetime,
                  audit: dict, rejections: list) -> list[dict]:
    """Validate rating rows, then keep one current rating per (user, movie).

    A rating is a dict with user_id, movie_id, rating and rated_at. The latest
    valid timestamp wins and older rows are superseded. Different scores at
    that latest timestamp cannot be ordered, so the whole pair is rejected.
    """
    pairs = defaultdict(list)
    for record in records:
        values = record["values"]
        try:
            rating = {
                "user_id": parse_id(values["userId"], "userId"),
                "movie_id": parse_id(values["movieId"], "movieId"),
                "rating": parse_rating(values["rating"]),
                "rated_at": parse_timestamp(values["timestamp"], cutoff),
            }
        except FieldError as error:
            reject(rejections, audit, record, error.field, error.reason)
            continue
        if rating["movie_id"] not in movies:
            reason = "movie_was_rejected" if rating["movie_id"] in rejected_ids else "unknown_movie"
            reject(rejections, audit, record, "movieId", reason)
            continue
        rating["record"] = record
        pairs[(rating["user_id"], rating["movie_id"])].append(rating)

    current = []
    for pair in sorted(pairs):
        group = pairs[pair]
        latest_at = max(r["rated_at"] for r in group)
        latest = [r for r in group if r["rated_at"] == latest_at]
        if len({r["rating"] for r in latest}) > 1:
            for rating in group:
                reject(rejections, audit, rating["record"], "rating", "conflicting_latest_score")
            continue
        current.append(latest[0])
        audit["selected"] += 1
        audit["duplicate"] += len(latest) - 1
        audit["superseded"] += len(group) - len(latest)
    return current


def check_inputs(movies: dict, ratings: list, audits: list[dict]) -> None:
    for audit in audits:
        outcomes = audit["selected"] + audit["duplicate"] + audit["superseded"] + audit["rejected"]
        if outcomes != audit["records"]:
            raise PipelineError(f"{audit['source']}: row outcomes do not add up to the rows read")
    if not movies:
        raise PipelineError("no valid movies are left after cleaning")
    if not ratings:
        raise PipelineError("no valid ratings are left after cleaning")


# OMDb: cache, request budget and matching


def open_omdb(cache_path: Path, api_key: str, offline: bool, max_requests: int, warnings: Counter) -> dict:
    """Everything the OMDb functions share during one run, as one dictionary."""
    try:
        cache = sqlite3.connect(cache_path, isolation_level=None)  # autocommit: each write is saved at once
        cache.executescript(CACHE_SCHEMA)
    except sqlite3.DatabaseError:
        raise PipelineError(f"{cache_path} is damaged; move it aside to start a new cache "
                            "(its request history goes with it)") from None
    return {
        "cache": cache, "session": requests.Session(), "api_key": api_key, "offline": offline,
        "max_requests": max_requests, "now": utc_now(), "warnings": warnings,
        "reserved": 0, "cache_hits": 0, "stale_cache_hits": 0, "responses": 0,
        "movie_requests": 0, "stop_reason": None,
    }


def make_reply(status: str, code: str | None = None, body: dict | None = None,
               retry_after: str | None = None) -> dict:
    """The shape every OMDb answer takes. status is success, not_found, ambiguous,
    unavailable, auth_error, rate_limited, transient_error or invalid_response."""
    return {"status": status, "code": code, "body": body, "retry_after": retry_after}


def cache_key(params: dict) -> str:
    """Public query parameters only; the API key is never part of the key."""
    return json.dumps({"matching_version": CACHE_KEY_FORMAT, "params": params, "r": "json"}, sort_keys=True)


def read_cache(omdb: dict, params: dict) -> dict | None:
    """Return a cached reply that may still be used, or None. Unusable entries are removed."""
    key = cache_key(params)
    row = omdb["cache"].execute(
        "SELECT kind, body, fetched_at_utc FROM responses WHERE cache_key = ?", (key,)).fetchone()
    if row is None:
        return None
    kind, text, fetched_at = row
    try:
        if kind not in ("success", "not_found") or len(text.encode()) > MAX_RESPONSE_BYTES:
            raise ValueError("unusable cache entry")
        body, age = json.loads(text), omdb["now"] - parse_utc(fetched_at)
        if not isinstance(body, dict):
            raise ValueError("cached body is not an object")
        # The stored kind must agree with OMDb's own envelope in the body.
        if kind == "success" and body.get("Response") != "True":
            raise ValueError("cached success without a success envelope")
        if kind == "not_found" and (body.get("Response") != "False" or body.get("Error") not in NEGATIVE_ERRORS):
            raise ValueError("cached negative without a negative envelope")
    except (ValueError, TypeError, AttributeError, RecursionError, PipelineError):
        omdb["cache"].execute("DELETE FROM responses WHERE cache_key = ?", (key,))
        omdb["warnings"]["unusable_cache_entry_removed"] += 1
        return None
    fresh = age <= (SUCCESS_TTL if kind == "success" else NOT_FOUND_TTL)
    if not fresh and not (omdb["offline"] and kind == "success"):
        return None
    omdb["cache_hits"] += 1
    if not fresh:
        omdb["stale_cache_hits"] += 1
    return make_reply(kind, body=body)


def reserve_request(omdb: dict) -> bool:
    """Save a reservation before each HTTP attempt, so a crash mid-request still counts."""
    since = format_utc(utc_now() - timedelta(hours=24))
    used = omdb["cache"].execute("SELECT count(*) FROM requests WHERE reserved_at_utc > ?", (since,)).fetchone()[0]
    if omdb["reserved"] >= omdb["max_requests"]:
        omdb["stop_reason"] = f"run budget of {omdb['max_requests']} requests used"
    elif used >= DAILY_REQUEST_LIMIT:
        omdb["stop_reason"] = f"rolling 24-hour budget of {DAILY_REQUEST_LIMIT} requests used"
    if omdb["stop_reason"]:
        return False
    omdb["cache"].execute("INSERT INTO requests (reserved_at_utc) VALUES (?)", (format_utc(utc_now()),))
    omdb["reserved"] += 1
    omdb["movie_requests"] += 1
    return True


def send_request(session: requests.Session, params: dict, api_key: str) -> dict:
    """Make one HTTP attempt and classify the answer.

    The body is streamed and abandoned once it passes MAX_RESPONSE_BYTES. Network
    errors become reply codes; their text is never kept because it can contain
    the request URL and the key.
    """
    try:
        with session.get(OMDB_URL, params={**params, "r": "json", "apikey": api_key},
                         timeout=REQUEST_TIMEOUT, stream=True) as response:
            status_code = response.status_code
            retry_after = response.headers.get("Retry-After")
            raw = bytearray()
            for chunk in response.iter_content(chunk_size=65536):
                raw += chunk
                if len(raw) > MAX_RESPONSE_BYTES:
                    return make_reply("invalid_response", "too_large")
    except requests.Timeout:
        return make_reply("transient_error", "timeout")
    except (requests.ConnectionError, requests.exceptions.ChunkedEncodingError):
        return make_reply("transient_error", "connection_error")
    except requests.RequestException:
        return make_reply("transient_error", "request_failed")

    if status_code == 429:
        return make_reply("rate_limited", "http_429", retry_after=retry_after)
    if status_code in (500, 502, 503, 504):
        return make_reply("transient_error", f"http_{status_code}", retry_after=retry_after)
    try:
        body = json.loads(raw)
    except (ValueError, RecursionError):
        return make_reply("invalid_response", "not_json")
    if not isinstance(body, dict):
        return make_reply("invalid_response", "not_an_object")
    error = body.get("Error")
    if body.get("Response") == "True" and status_code == 200:
        return make_reply("success", body=body)
    if error in NEGATIVE_ERRORS:
        return make_reply("not_found", body=body)
    if error in ("Invalid API key!", "No API key provided."):
        return make_reply("auth_error", "invalid_api_key")
    if error == "Request limit reached!":
        return make_reply("rate_limited", "provider_quota")
    return make_reply("invalid_response", f"http_{status_code}" if status_code != 200 else "unexpected_reply")


def retry_wait(reply: dict, attempt: int) -> int | None:
    """Seconds to wait before the next attempt, or None to stop calling OMDb this run.

    A numeric Retry-After is obeyed when it is at most MAX_WAIT_SECONDS. HTTP-date
    values are not supported, so a 429 without a usable number stops the run's requests.
    """
    header = (reply["retry_after"] or "").strip()
    if header.isascii() and header.isdigit():
        seconds = int(header) if len(header) <= 6 else MAX_WAIT_SECONDS + 1
        return seconds if seconds <= MAX_WAIT_SECONDS else None
    if reply["status"] == "rate_limited":
        return None
    return RETRY_WAITS[min(attempt, len(RETRY_WAITS) - 1)]


def omdb_get(omdb: dict, params: dict) -> dict:
    """Answer one OMDb query from the cache, or over HTTP within the request budgets."""
    cached = read_cache(omdb, params)
    if cached is not None:
        return cached
    if omdb["offline"] or omdb["stop_reason"]:
        return make_reply("unavailable")
    reply = make_reply("unavailable")
    for attempt in range(ATTEMPTS_PER_REQUEST):
        if omdb["movie_requests"] >= MOVIE_REQUEST_LIMIT:
            return reply if attempt else make_reply("transient_error", "movie_request_limit")
        if not reserve_request(omdb):
            return reply
        reply = send_request(omdb["session"], params, omdb["api_key"])
        if reply["code"] not in CONNECTION_ERRORS:
            omdb["responses"] += 1
        if reply["status"] in ("success", "not_found"):
            omdb["cache"].execute(
                """
                INSERT INTO responses (cache_key, kind, body, fetched_at_utc) VALUES (?, ?, ?, ?)
                ON CONFLICT (cache_key) DO UPDATE SET
                    kind = excluded.kind, body = excluded.body, fetched_at_utc = excluded.fetched_at_utc
                """,
                (cache_key(params), reply["status"], json.dumps(reply["body"]), format_utc(utc_now())),
            )
            return reply
        if reply["status"] == "auth_error":
            raise PipelineError("OMDb rejected the API key; check OMDB_API_KEY")
        if reply["code"] == "provider_quota":
            omdb["stop_reason"] = "OMDb reported that the key's request limit is reached"
            return reply
        if reply["status"] not in ("transient_error", "rate_limited"):
            return reply
        wait = retry_wait(reply, attempt)
        if wait is None:
            omdb["stop_reason"] = "OMDb asked to wait longer than this run will wait"
            return reply
        time.sleep(wait)  # also after the last attempt, so the next request respects the wait
    return reply


def optional_text(body: dict, name: str, warnings: Counter) -> str | None:
    """An optional OMDb text field. "N/A", blanks and absent values become None;
    lists, numbers and booleans are refused rather than turned into text."""
    value = body.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        warnings[f"non_text_{name}_ignored"] += 1
        return None
    value = value.strip()
    return None if value in ("", "N/A") else value


def parse_released(text: str | None, warnings: Counter) -> str | None:
    """OMDb writes dates like "22 Nov 1995". English month names are read from a
    table, so the result does not depend on the computer's locale."""
    if text is None:
        return None
    match = RELEASED.fullmatch(text)
    try:
        return date(int(match["year"]), MONTHS[match["month"]], int(match["day"])).isoformat()
    except (TypeError, KeyError, ValueError):
        warnings["unparsed_release_date"] += 1
        return None


def parse_box_office(text: str | None, warnings: Counter) -> int | None:
    """Only OMDb's "$1,234,567" format, read as US dollars and checked against SQLite's integer range."""
    if text is None:
        return None
    if not DOLLARS.fullmatch(text):
        warnings["unparsed_box_office"] += 1
        return None
    digits = text[1:].replace(",", "").lstrip("0") or "0"
    if len(digits) > len(str(MAX_SQLITE_INTEGER)) or int(digits) > MAX_SQLITE_INTEGER:
        warnings["box_office_too_large"] += 1
        return None
    return int(digits)


def front_article(title: str) -> str:
    """MovieLens moves articles to the end: "American President, The" becomes "The American President"."""
    match = TRAILING_ARTICLE.fullmatch(title)
    if not match:
        return title
    separator = "" if match["article"].endswith("'") else " "
    return match["article"] + separator + match["title"]


def title_variants(title: str) -> list[str]:
    """The source title, then at most three justified alternatives.

    MovieLens often adds an alternate title in final parentheses, as in
    "Shanghai Triad (Yao a yao yao dao waipo qiao)". Only that final, un-nested
    pair is split off; any other parentheses stay part of the title.
    """
    variants = [title, front_article(title)]
    match = ALTERNATE_TITLE.fullmatch(title)
    if match:
        variants += [front_article(match["main"]), front_article(match["alternate"].removeprefix("a.k.a. "))]
    return list(dict.fromkeys(variants))[:4]


def title_key(title: str) -> str:
    """Titles are compared ignoring case, spacing and punctuation, so "Die Hard: With
    a Vengeance" equals "Die Hard with a Vengeance". Letters and digits must agree."""
    return " ".join(re.sub(r"[\W_]+", " ", normalize_name(title)[1]).split())


def accept_candidate(body: dict, movie: dict, titles: list[str], warnings: Counter) -> dict | None:
    """Return the accepted details of an OMDb record, or None if its identity does not agree.

    Details: imdb_id, omdb_year, media_type, released_on, plot, box_office_usd
    and directors ({key: display name}).
    """
    imdb_id, title, media_type = body.get("imdbID"), body.get("Title"), body.get("Type")
    if not (isinstance(imdb_id, str) and IMDB_ID.fullmatch(imdb_id) and isinstance(title, str)
            and media_type in MEDIA_TYPES and title_key(title) in {title_key(t) for t in titles}):
        return None
    year_match = OMDB_YEAR.fullmatch(optional_text(body, "Year", warnings) or "")
    start_year = int(year_match["start"]) if year_match else None
    if movie["release_year"] is not None and start_year != movie["release_year"]:
        warnings["year_mismatch"] += 1  # the MovieLens year wins and the candidate is refused
        return None
    single_year = start_year if year_match and not year_match["range"] and start_year >= 1000 else None

    directors = {}
    for part in (optional_text(body, "Director", warnings) or "").split(","):
        name, key = normalize_name(part)
        if key:
            directors[key] = min(directors.get(key, name), name)
    return {
        "imdb_id": imdb_id, "omdb_year": single_year, "media_type": media_type,
        "released_on": parse_released(optional_text(body, "Released", warnings), warnings),
        "plot": optional_text(body, "Plot", warnings),
        "box_office_usd": parse_box_office(optional_text(body, "BoxOffice", warnings), warnings),
        "directors": directors,
    }


def search_unique(omdb: dict, title: str, titles: list[str]) -> dict:
    """Search for a movie that has no year in its title.

    Only a complete, consistent result list with exactly one exact-title candidate
    counts. That candidate is fetched again by IMDb ID so the full record can be
    verified, and the record must carry the ID that was asked for.
    """
    reply = omdb_get(omdb, {"s": title})
    if reply["status"] == "not_found" and reply["body"].get("Error") == "Too many results.":
        return make_reply("ambiguous")
    if reply["status"] != "success":
        return reply
    results, total = reply["body"].get("Search"), reply["body"].get("totalResults")
    if not (isinstance(results, list) and isinstance(total, str) and total.isascii()
            and total.isdigit() and len(total) <= 9):
        return make_reply("invalid_response", "unexpected_search_reply")
    # A malformed entry is never skipped, because skipping it could hide a second candidate.
    for item in results:
        if not (isinstance(item, dict) and isinstance(item.get("imdbID"), str)
                and IMDB_ID.fullmatch(item["imdbID"]) and isinstance(item.get("Title"), str)):
            return make_reply("invalid_response", "unexpected_search_reply")
    if int(total) < len(results):
        return make_reply("invalid_response", "search_count_mismatch")
    if int(total) > len(results):
        return make_reply("ambiguous")  # only the first page was read
    keys = {title_key(t) for t in titles}
    ids = {item["imdbID"] for item in results if title_key(item["Title"]) in keys}
    if len(ids) != 1:
        return make_reply("ambiguous" if ids else "not_found")
    requested_id = ids.pop()
    details = omdb_get(omdb, {"i": requested_id})
    if details["status"] == "success" and details["body"].get("imdbID") != requested_id:
        return make_reply("invalid_response", "imdb_id_mismatch")
    return details


def resolve_movie(omdb: dict, movie: dict) -> dict:
    """Try each title variant until one verifies. HTTP success alone is never a match.

    Returns {"status", "error", "details"}, where status is matched, not_found,
    ambiguous, error, or unchanged when the budget or offline mode stopped the lookup.
    """
    omdb["movie_requests"] = 0
    titles = title_variants(movie["title"])
    decision = "not_found"
    for title in titles:
        if movie["release_year"] is None:
            reply = search_unique(omdb, title, titles)
        else:
            reply = omdb_get(omdb, {"t": title, "y": str(movie["release_year"])})
        if reply["status"] == "success":
            details = accept_candidate(reply["body"], movie, titles, omdb["warnings"])
            if details:
                return {"status": "matched", "error": None, "details": details}
            decision = "ambiguous"
        elif reply["status"] == "ambiguous":
            decision = "ambiguous"
        elif reply["status"] == "unavailable":
            return {"status": "unchanged", "error": None, "details": None}
        elif reply["status"] != "not_found":
            return {"status": "error", "error": reply["code"], "details": None}
    return {"status": decision, "error": None, "details": None}


def enrich_movies(omdb: dict, movies: dict, prior: dict) -> dict:
    """Look up pending movies first and earlier errors second, in movie ID order.

    Movies with a final decision (matched, not_found, ambiguous) are skipped. After
    several movies in a row fail only with connection errors the remote phase stops.
    """
    queue = [m for m in movies.values() if prior.get(m["movie_id"], "pending") in ("pending", "error")]
    queue.sort(key=lambda m: (prior.get(m["movie_id"], "pending") == "error", m["movie_id"]))
    results, unreachable_streak = {}, 0
    for movie in queue:
        if omdb["stop_reason"]:
            break
        responses_before = omdb["responses"]
        result = resolve_movie(omdb, movie)
        results[movie["movie_id"]] = result
        if omdb["responses"] > responses_before:
            unreachable_streak = 0
        elif result["error"] in CONNECTION_ERRORS:
            unreachable_streak += 1
            if unreachable_streak == UNREACHABLE_MOVIE_LIMIT:
                omdb["stop_reason"] = f"OMDb unreachable for {UNREACHABLE_MOVIE_LIMIT} movies in a row"
    return results


# SQLite loading


def open_database(path: Path) -> sqlite3.Connection:
    """Open with manual transactions and foreign keys switched on and checked."""
    connection = sqlite3.connect(path, isolation_level=None)
    connection.execute("PRAGMA foreign_keys = ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        connection.close()
        raise PipelineError("SQLite could not enable foreign keys")
    return connection


def inspect_database(path: Path) -> tuple[dict | None, dict]:
    """Check an existing database before any HTTP request or write.

    Returns (manifest, lookup status per movie), or (None, {}) for a missing or
    empty file. The file is opened read-only, so an old-format, unrelated, damaged
    or unbound non-empty database is refused without being changed.
    """
    if not path.exists() or path.stat().st_size == 0:
        return None, {}
    try:
        connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
        try:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version != DATABASE_VERSION:
                raise PipelineError(f"{path} uses database format {version}, but this pipeline writes "
                                    f"format {DATABASE_VERSION}; use a new --db path")
            for table, columns in REQUIRED_COLUMNS.items():
                present = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
                if not present:
                    raise PipelineError(f"{path} is missing table {table}; use a new --db path")
                missing = [column for column in columns if column not in present]
                if missing:
                    raise PipelineError(f"{path}: {table} is missing column {', '.join(missing)}; use a new --db path")
            row = connection.execute(
                "SELECT movies_sha256, ratings_sha256, validation_as_of_utc FROM dataset_manifest").fetchone()
            if row is None:
                domain_tables = [table for table in REQUIRED_COLUMNS if table != "dataset_manifest"]
                if any(connection.execute(f"SELECT EXISTS (SELECT 1 FROM {table})").fetchone()[0]
                       for table in domain_tables):
                    raise PipelineError(f"{path} has data but no manifest; refusing to change it")
                return None, {}
            manifest = dict(zip(("movies_sha256", "ratings_sha256", "validation_as_of_utc"), row))
            return manifest, dict(connection.execute("SELECT movie_id, enrichment_status FROM movies"))
        finally:
            connection.close()
    except sqlite3.DatabaseError as error:
        raise PipelineError(f"{path} is not a usable pipeline database ({error})") from None


def write_source_rows(connection: sqlite3.Connection, movies: dict, ratings: list, genre_names: dict) -> None:
    """Upsert the accepted CSV rows. Updating a movie never touches its OMDb columns."""
    connection.executemany(
        """
        INSERT INTO movies (movie_id, source_title, title, release_year) VALUES (?, ?, ?, ?)
        ON CONFLICT (movie_id) DO UPDATE SET
            source_title = excluded.source_title, title = excluded.title, release_year = excluded.release_year
        """,
        [(m["movie_id"], m["source_title"], m["title"], m["release_year"]) for m in movies.values()],
    )
    # Sorted inserts give new genres the same IDs on every fresh load.
    connection.executemany(
        "INSERT INTO genres (name, name_key) VALUES (?, ?) ON CONFLICT (name_key) DO UPDATE SET name = excluded.name",
        [(name, key) for key, name in genre_names.items()],
    )
    genre_ids = dict(connection.execute("SELECT name_key, genre_id FROM genres"))
    connection.executemany(
        "INSERT INTO movie_genres (movie_id, genre_id) VALUES (?, ?) ON CONFLICT (movie_id, genre_id) DO NOTHING",
        [(m["movie_id"], genre_ids[key]) for m in movies.values() for key in sorted(m["genres"])],
    )
    connection.executemany(
        """
        INSERT INTO ratings (user_id, movie_id, rating, rated_at) VALUES (?, ?, ?, ?)
        ON CONFLICT (user_id, movie_id) DO UPDATE SET rating = excluded.rating, rated_at = excluded.rated_at
        """,
        [(r["user_id"], r["movie_id"], r["rating"], r["rated_at"]) for r in ratings],
    )


def write_enrichment(connection: sqlite3.Connection, results: dict) -> None:
    """Store this run's lookup decisions. Only pending or failed movies are updated,
    so details accepted by an earlier run are never overwritten."""
    for movie_id, result in sorted(results.items()):
        details = result["details"]
        if result["status"] == "matched":
            connection.execute(
                """
                UPDATE movies SET enrichment_status = 'matched', enrichment_error = NULL, imdb_id = ?,
                    omdb_year = ?, media_type = ?, released_on = ?, plot = ?, box_office_usd = ?
                WHERE movie_id = ? AND enrichment_status IN ('pending', 'error')
                """,
                (details["imdb_id"], details["omdb_year"], details["media_type"], details["released_on"],
                 details["plot"], details["box_office_usd"], movie_id),
            )
            for key, name in sorted(details["directors"].items()):
                connection.execute(
                    "INSERT INTO directors (name, name_key) VALUES (?, ?) "
                    "ON CONFLICT (name_key) DO UPDATE SET name = min(name, excluded.name)", (name, key))
                connection.execute(
                    "INSERT INTO movie_directors (movie_id, director_id) SELECT ?, director_id FROM directors "
                    "WHERE name_key = ? ON CONFLICT (movie_id, director_id) DO NOTHING", (movie_id, key))
        elif result["status"] != "unchanged":
            connection.execute(
                "UPDATE movies SET enrichment_status = ?, enrichment_error = ? "
                "WHERE movie_id = ? AND enrichment_status IN ('pending', 'error')",
                (result["status"], result["error"], movie_id),
            )


def verify_before_commit(connection: sqlite3.Connection, movie_count: int, rating_count: int) -> None:
    def number(sql: str) -> int:
        return connection.execute(sql).fetchone()[0]

    problems = []
    if number("SELECT count(*) FROM movies") != movie_count:
        problems.append("movie count differs from the accepted rows")
    if number("SELECT count(*) FROM ratings") != rating_count:
        problems.append("rating count differs from the accepted rows")
    if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
        problems.append("foreign-key violations")
    if number("SELECT count(*) FROM movie_directors JOIN movies USING (movie_id) "
              "WHERE enrichment_status <> 'matched'"):
        problems.append("director credits on movies without a match")
    if number("SELECT count(*) FROM movies WHERE enrichment_status <> 'matched' AND COALESCE("
              "imdb_id, omdb_year, media_type, released_on, plot, box_office_usd) IS NOT NULL"):
        problems.append("OMDb details on movies without a match")
    if problems:
        raise PipelineError("check before commit failed: " + "; ".join(problems))


def collect_stats(connection: sqlite3.Connection) -> dict:
    def number(sql: str) -> int:
        return connection.execute(sql).fetchone()[0]

    return {
        "movies": number("SELECT count(*) FROM movies"),
        "ratings": number("SELECT count(*) FROM ratings"),
        "lookup_states": dict(connection.execute(
            "SELECT enrichment_status, count(*) FROM movies GROUP BY enrichment_status ORDER BY enrichment_status")),
        "movies_with_known_director": number("SELECT count(DISTINCT movie_id) FROM movie_directors"),
        "movies_without_year": number("SELECT count(*) FROM movies WHERE COALESCE(release_year, omdb_year) IS NULL"),
        "ratings_without_year": number("SELECT count(*) FROM ratings JOIN movies USING (movie_id) "
                                       "WHERE COALESCE(release_year, omdb_year) IS NULL"),
        # Different MovieLens movies verified to one IMDb title are reported, never merged.
        "shared_imdb_ids": [row[0] for row in connection.execute(
            "SELECT imdb_id FROM movies WHERE imdb_id IS NOT NULL GROUP BY imdb_id HAVING count(*) > 1")],
    }


def publish(path: Path, schema_sql: str, movies: dict, ratings: list, genre_names: dict,
            results: dict, new_manifest: dict | None, state: dict) -> dict:
    """Write all rows and lookup results in one transaction, or nothing at all.

    A new file first gets the schema in a separate step; an empty schema is not a
    published dataset because it has no manifest.
    """
    if not path.exists() or path.stat().st_size == 0:
        setup = sqlite3.connect(path, isolation_level=None)
        try:
            setup.executescript(schema_sql)
        finally:
            setup.close()
    connection = open_database(path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        try:
            write_source_rows(connection, movies, ratings, genre_names)
            write_enrichment(connection, results)
            if new_manifest:
                connection.execute(
                    "INSERT INTO dataset_manifest (singleton_id, movies_sha256, ratings_sha256, validation_as_of_utc) "
                    "VALUES (1, :movies_sha256, :ratings_sha256, :validation_as_of_utc)", new_manifest)
            verify_before_commit(connection, len(movies), len(ratings))
            stats = collect_stats(connection)
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")
        state["published"] = True
        return stats
    finally:
        connection.close()


# Summary, reports and main()


def print_summary(summary: dict) -> None:
    print(f"Published {summary['database']}")
    print(f"Validation cutoff: {summary['validation_as_of_utc']}")
    for source, audit in summary["inputs"].items():
        print(f"{source}: {audit['records']} rows, {audit['blank_lines']} blank lines -> selected {audit['selected']}, "
              f"duplicate {audit['duplicate']}, superseded {audit['superseded']}, rejected {audit['rejected']}")
        for reason, count in audit["rejection_reasons"].items():
            print(f"  rejected {reason}: {count}")
    for code, count in summary["warnings"].items():
        print(f"  warning {code}: {count}")
    omdb, stats = summary["omdb"], summary["database_contents"]
    print(f"OMDb: {omdb['requests_reserved']} requests reserved this run, "
          f"{omdb['cache_hits']} answers from the cache ({omdb['stale_cache_hits']} stale)")
    if omdb["stop_reason"]:
        print(f"  requests stopped: {omdb['stop_reason']}")
    print("Lookups: " + ", ".join(f"{status} {count}" for status, count in stats["lookup_states"].items()))
    print(f"Directors known for {stats['movies_with_known_director']} of {stats['movies']} movies; "
          f"{stats['movies_without_year']} movies ({stats['ratings_without_year']} ratings) have no known year")
    if stats["shared_imdb_ids"]:
        print(f"  warning: {len(stats['shared_imdb_ids'])} IMDb IDs belong to more than one MovieLens movie")


def write_reports(folder: Path, stem: str, summary: dict, rejections: list) -> None:
    """Replace each report file in one step. Reports are separate from the database transaction."""
    files = {
        f"{stem}.report.json": json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        f"{stem}.rejections.jsonl": "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rejections),
    }
    for name, text in files.items():
        temporary = folder / f"{name}.tmp"
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, folder / name)


def run(args: argparse.Namespace, state: dict) -> int:
    # Configure: paths, key, schema and the existing database, all before any HTTP.
    paths = prepare_paths(args)
    api_key = load_api_key(paths["work_dir"], args.offline)
    schema_sql = read_snapshot(SCHEMA_PATH).decode("utf-8")
    manifest, prior = inspect_database(paths["database"])

    # Read both CSV files once.
    movies_data, ratings_data = read_snapshot(paths["movies_csv"]), read_snapshot(paths["ratings_csv"])
    movie_audit, rating_audit = new_audit("movies.csv", movies_data), new_audit("ratings.csv", ratings_data)
    if manifest and (manifest["movies_sha256"], manifest["ratings_sha256"]) != (movie_audit["sha256"],
                                                                                 rating_audit["sha256"]):
        raise PipelineError("the CSV files differ from the ones this database was built from; use a new --db path")
    # The first load pins the cutoff for future timestamps; reruns reuse it.
    cutoff = parse_utc(manifest["validation_as_of_utc"]) if manifest else utc_now()
    rejections, warnings = [], Counter()
    movie_records = parse_csv(movies_data, MOVIE_COLUMNS, movie_audit, rejections, warnings)
    rating_records = parse_csv(ratings_data, RATING_COLUMNS, rating_audit, rejections, warnings)

    # Clean movies first, so ratings can be checked against valid movie IDs.
    movies, rejected_ids, genre_names = clean_movies(movie_records, movie_audit, rejections, warnings)
    ratings = clean_ratings(rating_records, movies, rejected_ids, cutoff, rating_audit, rejections)
    check_inputs(movies, ratings, [movie_audit, rating_audit])

    # Enrich, outside any database transaction.
    omdb = open_omdb(paths["cache"], api_key, args.offline, args.max_api_calls, warnings)
    try:
        results = enrich_movies(omdb, movies, prior)
    finally:
        omdb["session"].close()
        omdb["cache"].close()

    # Load everything in one transaction.
    new_manifest = None if manifest else {"movies_sha256": movie_audit["sha256"],
                                          "ratings_sha256": rating_audit["sha256"],
                                          "validation_as_of_utc": format_utc(cutoff)}
    stats = publish(paths["database"], schema_sql, movies, ratings, genre_names, results, new_manifest, state)

    # Report.
    unfinished = stats["lookup_states"].get("pending", 0) + stats["lookup_states"].get("error", 0)
    for audit in (movie_audit, rating_audit):
        reasons = Counter(f"{r['field']}.{r['reason']}" if r["field"] else r["reason"]
                          for r in rejections if r["source"] == audit["source"])
        audit["rejection_reasons"] = dict(sorted(reasons.items()))
    summary = {
        "database": str(paths["database"]),
        "exit_code": EXIT_UNFINISHED if unfinished else EXIT_COMPLETE,
        "validation_as_of_utc": format_utc(cutoff),
        "inputs": {audit["source"]: audit for audit in (movie_audit, rating_audit)},
        "warnings": dict(sorted(warnings.items())),
        "omdb": {"requests_reserved": omdb["reserved"], "cache_hits": omdb["cache_hits"],
                 "stale_cache_hits": omdb["stale_cache_hits"], "stop_reason": omdb["stop_reason"]},
        "database_contents": stats,
    }
    print_summary(summary)
    write_reports(paths["reports"], paths["database"].stem, summary, rejections)
    return summary["exit_code"]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    state = {"published": False}
    try:
        return run(args, state)
    except PipelineError as error:
        message = f"error: {error}"
    except sqlite3.Error as error:
        message = f"error: database problem: {error}"
    except requests.RequestException:
        message = "error: network problem"  # the exception text can contain the request URL and key
    except OSError as error:
        message = f"error: file problem: {error}"
    except KeyboardInterrupt:
        message = "interrupted"
    if state["published"]:
        print(f"{message}; the database was already published, but the reports may be missing", file=sys.stderr)
        return EXIT_REPORT_FAILED
    print(f"{message}; nothing new was published", file=sys.stderr)
    return EXIT_FATAL


if __name__ == "__main__":
    sys.exit(main())
