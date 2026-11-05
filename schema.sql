-- Movie pipeline database, format version 2 (SQLite 3.37 or newer).
--
-- etl.py runs this file once, on a new empty database, before loading data.
-- STRICT tables make SQLite reject values whose type does not match the column.
-- Foreign keys are only enforced when a connection turns them on, so etl.py
-- enables and checks them on every connection it opens.
PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;

-- One MovieLens movie. movie_id is the MovieLens ID, never an IMDb ID.
-- The OMDb columns stay NULL until the movie is matched; a matched movie can
-- still lack a plot, release date or box office figure.
CREATE TABLE movies (
    movie_id INTEGER PRIMARY KEY CHECK (movie_id > 0),
    source_title TEXT NOT NULL CHECK (length(trim(source_title)) > 0),
    title TEXT NOT NULL CHECK (length(trim(title)) > 0),
    release_year INTEGER CHECK (release_year BETWEEN 1000 AND 9999),
    imdb_id TEXT,
    omdb_year INTEGER CHECK (omdb_year BETWEEN 1000 AND 9999),
    media_type TEXT CHECK (media_type IN ('movie', 'series', 'episode')),
    released_on TEXT,
    plot TEXT,
    box_office_usd INTEGER CHECK (box_office_usd >= 0),
    enrichment_status TEXT NOT NULL DEFAULT 'pending' CHECK (
        enrichment_status IN ('pending', 'matched', 'not_found', 'ambiguous', 'error')
    ),
    enrichment_error TEXT,
    CHECK (enrichment_status <> 'matched' OR imdb_id IS NOT NULL)
) STRICT;

-- The current rating one user gave one movie.
CREATE TABLE ratings (
    user_id INTEGER NOT NULL CHECK (user_id > 0),
    movie_id INTEGER NOT NULL REFERENCES movies (movie_id),
    rating REAL NOT NULL CHECK (
        rating IN (0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0)
    ),
    rated_at INTEGER NOT NULL CHECK (rated_at >= 0),
    PRIMARY KEY (user_id, movie_id)
) STRICT;

-- name_key is the normalized name used to find the same genre again.
CREATE TABLE genres (
    genre_id INTEGER PRIMARY KEY CHECK (genre_id > 0),
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    name_key TEXT NOT NULL UNIQUE CHECK (length(trim(name_key)) > 0)
) STRICT;

CREATE TABLE movie_genres (
    movie_id INTEGER NOT NULL REFERENCES movies (movie_id),
    genre_id INTEGER NOT NULL REFERENCES genres (genre_id),
    PRIMARY KEY (movie_id, genre_id)
) STRICT;

CREATE TABLE directors (
    director_id INTEGER PRIMARY KEY CHECK (director_id > 0),
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    name_key TEXT NOT NULL UNIQUE CHECK (length(trim(name_key)) > 0)
) STRICT;

CREATE TABLE movie_directors (
    movie_id INTEGER NOT NULL REFERENCES movies (movie_id),
    director_id INTEGER NOT NULL REFERENCES directors (director_id),
    PRIMARY KEY (movie_id, director_id)
) STRICT;

-- At most one row: the CSV files this database was built from, and the
-- cutoff used to reject future timestamps. Reruns must match it.
CREATE TABLE dataset_manifest (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    movies_sha256 TEXT NOT NULL CHECK (length(movies_sha256) = 64),
    ratings_sha256 TEXT NOT NULL CHECK (length(ratings_sha256) = 64),
    validation_as_of_utc TEXT NOT NULL
) STRICT;

-- The primary keys already index the first column of each pair.
CREATE INDEX idx_ratings_movie_id ON ratings (movie_id);
CREATE INDEX idx_movie_genres_genre_id ON movie_genres (genre_id);
CREATE INDEX idx_movie_directors_director_id ON movie_directors (director_id);

-- A compatibility marker checked by etl.py, not a migration system.
PRAGMA user_version = 2;
COMMIT;
