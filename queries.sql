-- The four assignment questions (SQLite 3.37 or newer).
--
-- Every average uses individual rating rows, so each user's current rating of a
-- movie counts once. Leaders are picked on the unrounded average; ROUND() only
-- shortens the number that is displayed.

-- 1. Which movie has the highest average rating?
-- movie_scores is a CTE: a named temporary result with one row per rated movie.
-- Comparing with its maximum returns every movie tied for first place. Movies
-- without ratings have no average and do not appear. rating_count shows how
-- many ratings each average is based on.
WITH movie_scores AS (
    SELECT m.movie_id,
           m.title,
           m.release_year,
           AVG(r.rating) AS average_rating,
           COUNT(*) AS rating_count
    FROM movies AS m
    JOIN ratings AS r ON r.movie_id = m.movie_id
    GROUP BY m.movie_id
)
SELECT movie_id, title, release_year, ROUND(average_rating, 3) AS average_rating, rating_count
FROM movie_scores
WHERE average_rating = (SELECT MAX(average_rating) FROM movie_scores)
ORDER BY title, movie_id;

-- 2. Which five genres have the highest average rating?
-- A rating counts once for each genre of its movie, so a movie rated [5] and a
-- movie rated [1, 1, 1] in the same genre average 2.0 together, not 3.0.
-- Genres with the same average are ordered by name, so the fifth place is stable.
SELECT g.name AS genre,
       ROUND(AVG(r.rating), 3) AS average_rating,
       COUNT(*) AS rating_count,
       COUNT(DISTINCT r.movie_id) AS rated_movie_count
FROM genres AS g
JOIN movie_genres AS mg ON mg.genre_id = g.genre_id
JOIN ratings AS r ON r.movie_id = mg.movie_id
GROUP BY g.genre_id
ORDER BY AVG(r.rating) DESC, g.name
LIMIT 5;

-- 3. Which director has the most movies?
-- Each movie_directors row is one unique credit (the primary key is the movie
-- and director pair), so COUNT(*) is the number of movies, rated or not.
-- Co-directors each get the credit, and every tied director is returned.
-- HAVING keeps only the groups whose count equals the largest count.
-- Directors exist only for movies matched on OMDb, so the last two columns
-- show how many movies this answer is based on.
SELECT d.name AS director,
       COUNT(*) AS movie_count,
       (SELECT COUNT(DISTINCT movie_id) FROM movie_directors) AS movies_with_known_director,
       (SELECT COUNT(*) FROM movies) AS total_movies
FROM directors AS d
JOIN movie_directors AS md ON md.director_id = d.director_id
GROUP BY d.director_id
HAVING COUNT(*) = (
    SELECT MAX(credit_count)
    FROM (SELECT COUNT(*) AS credit_count FROM movie_directors GROUP BY director_id)
)
ORDER BY d.name;

-- 4. What is the average rating for each release year?
-- COALESCE returns its first non-NULL value: the year from the MovieLens title,
-- otherwise the year from a verified OMDb match. Movies with neither are left
-- out; the ETL summary reports how many there are.
SELECT COALESCE(m.release_year, m.omdb_year) AS year,
       ROUND(AVG(r.rating), 3) AS average_rating,
       COUNT(*) AS rating_count,
       COUNT(DISTINCT m.movie_id) AS rated_movie_count
FROM movies AS m
JOIN ratings AS r ON r.movie_id = m.movie_id
WHERE COALESCE(m.release_year, m.omdb_year) IS NOT NULL
GROUP BY COALESCE(m.release_year, m.omdb_year)
ORDER BY year;
