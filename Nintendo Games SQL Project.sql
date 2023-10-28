/*
CONTEXT:
Person responsible for video games' development in Nintendo Games Company (let's call this person Mike) wants to explore the topic of 
games released by intra-company game design studios, that have been critically acclaimed and also universally acclaimed by the end users.
As a meter of gamers' satisfaction per specific game, Mike wants to use Metacritic site, which is a webpage aggregating ratings and reviews of video games.
Mike managed to receive data report, scraped from the web, from company data engineer, but raw report contains several issues, that need to be dealt with
(cleaned) before proceeding with proper data analsysis. Fortunately, raw report includes only games released by 
Nintendo-owned game studios and it consists of one table.

Link to the database:
https://www.kaggle.com/datasets/joebeachcapital/nintendo-games

Issues identified:
- field containing multiple values in one record, separated by commas (field 'genres', 'developers') - atomicity issue
- dates stored in string format instead of date type
- data containing games that have not been released yet or were cancelled in the process of design
- column unnecessary for further analysis
- records containing empty strings and not NULL values
- numeric values strored with text data type

Skill set used:
- DDL commands - CREATE, DROP, ALTER
- DML commands - UPDATE, DELETE
- TCL commands - COMMIT, ROLLBACK
- DQL commands - SELECT (using keywords WHERE, LIMIT, ORDER BY, LIKE, GROUP, HAVING, CASE WHEN)
- COMMON TABLE EXPRESSIONS (CTE)
- JOINS
- STRING FUCTIONS (STR_TO_DATE, POSITION, LENGTH, SUBSTRING_INDEX)
- AGGREGATE FUNCTIONS (AVERAGE)
- WILDCARDS

Below script contains necessary steps, along with the explanations, that need to be performed in order to clean raw data.

*/

###### PART 1: DATA UPLOADING TO SQL DATABASE
-- Steps include: creating database, uploading csv file through 'table data import wizard', 'use database' command.

CREATE DATABASE Nintendo_Project;
USE Nintendo_Project;

###### PART 2: DATA CLEANING
-- Steps include: changing name of the column ('date' is SQL keyword, so it is not preferred as a column name).

ALTER TABLE nintendo_games
RENAME COLUMN date TO issuance_date;

-- Steps include: looking at table structure and dropping column unnecessary for purpose of further analysis.

SELECT * FROM nintendo_games;
ALTER TABLE nintendo_games DROP COLUMN link;

-- Steps include: finding and removing games that were cancelled or are to be annouced yet (using 'commit' command for safety).

COMMIT;

SELECT 
    *
FROM
    nintendo_games
WHERE
    issuance_date NOT LIKE '%, 20__'
        AND issuance_date NOT LIKE '%, 19__';

DELETE FROM nintendo_games 
WHERE
    issuance_date NOT LIKE '%, 20__'
    AND issuance_date NOT LIKE '%, 19__';

-- Steps include: converting string 'date' into proper date format:
-- adding new column, converting string into date in new column, comparing both fields, dropping old column and rename.

ALTER TABLE nintendo_games ADD correct_issuance_date DATE;

UPDATE nintendo_games 
SET 
    correct_issuance_date = DATE_FORMAT(STR_TO_DATE(issuance_date, '%M %d, %Y'),
            '%Y-%m-%d');

SELECT 
    issuance_date, correct_issuance_date
FROM
    nintendo_games;
    
ALTER TABLE nintendo_games
DROP issuance_date;

ALTER TABLE nintendo_games
RENAME COLUMN correct_issuance_date TO issuance_date;

-- Steps include: checking if new 'issuance_date' column works fine by testing time filter.

SELECT 
    title, issuance_date
FROM
    nintendo_games
WHERE
    issuance_date > '2017-01-01'
ORDER BY issuance_date ASC;

-- Steps include: replacing empty strings "" for 'meta_score', 'user_score' and 'esrb_rating' with NULL values.

UPDATE nintendo_games 
SET 
    meta_score = NULL
WHERE
    meta_score = '';

UPDATE nintendo_games 
SET 
    user_score = NULL
WHERE
    user_score = '';

UPDATE nintendo_games 
SET 
    esrb_rating = NULL
WHERE
    esrb_rating = '';

-- Steps include: changing 'meta_score' and 'user_score' from string to integer and double precision type.

ALTER TABLE nintendo_games CHANGE meta_score meta_score INT;
ALTER TABLE nintendo_games CHANGE user_score user_score DOUBLE PRECISION(3,1);

-- Steps include: removing unncessary signs of []' in developers column;

UPDATE nintendo_games 
SET 
    developers = REPLACE(developers, '[', '');

UPDATE nintendo_games 
SET 
    developers = REPLACE(developers, ']', '');

UPDATE nintendo_games 
SET 
    developers = REPLACE(developers, '\'', '');

-- Steps include: splitting 'developers' column into separate columns 'main developer', 'sub-developer_1' and 'sub-developer_2'.

SELECT 
    developers,
    SUBSTRING_INDEX(developers, ',', 1) AS main_developer,
    CASE
        WHEN (POSITION(',' IN developers)) = 0 THEN NULL
        ELSE SUBSTRING_INDEX(RIGHT(developers,
                    LENGTH(developers) - POSITION(',' IN developers) - 2),
                ',',
                1)
    END AS sub_developer_1,
    CASE
        WHEN (POSITION(',' IN developers)) = 0 THEN NULL
        WHEN CHAR_LENGTH(developers) - CHAR_LENGTH(REPLACE(developers, ', ', '1')) = 1 THEN NULL
        ELSE SUBSTRING_INDEX(RIGHT(developers,
                    LENGTH(developers) - LENGTH(SUBSTRING_INDEX(developers, ',', 2)) - 3),
                ',',
                2)
    END AS sub_developer_2
FROM
    nintendo_games;

-- Steps include: adding new columns, populating them and removing original 'developers' column.

ALTER TABLE nintendo_games
ADD COLUMN main_developer VARCHAR(255);
ALTER TABLE nintendo_games
ADD COLUMN sub_developer_1 VARCHAR(255);
ALTER TABLE nintendo_games
ADD COLUMN sub_developer_2 VARCHAR(255);

UPDATE nintendo_games 
SET 
    main_developer = SUBSTRING_INDEX(developers, ',', 1);

UPDATE nintendo_games 
SET 
    sub_developer_1 = CASE
        WHEN (POSITION(',' IN developers)) = 0 THEN NULL
        ELSE SUBSTRING_INDEX(RIGHT(developers,
                    LENGTH(developers) - POSITION(',' IN developers) - 2),
                ',',
                1)
    END;

UPDATE nintendo_games 
SET 
    sub_developer_2 = CASE
        WHEN (POSITION(',' IN developers)) = 0 THEN NULL
        WHEN CHAR_LENGTH(developers) - CHAR_LENGTH(REPLACE(developers, ', ', '1')) = 1 THEN NULL
        ELSE SUBSTRING_INDEX(RIGHT(developers,
                    LENGTH(developers) - LENGTH(SUBSTRING_INDEX(developers, ',', 2)) - 3),
                ',',
                2)
    END;
    
ALTER TABLE nintendo_games
DROP COLUMN developers;

-- Steps include: removing unncessary signs of []' in 'genres' column;

UPDATE nintendo_games 
SET 
    genres = REPLACE(genres, '[', '');

UPDATE nintendo_games 
SET 
    genres = REPLACE(genres, ']', '');

UPDATE nintendo_games 
SET 
    genres = REPLACE(genres, '\'', '');


-- Steps include: splitting 'genres' column into separate columns 'main_genre', 'subgenre_1' and 'subgenre_2'.

SELECT 
    title,
    genres,
    SUBSTRING_INDEX(genres, ',', 1) AS main_genre,
    CASE
        WHEN (POSITION(',' IN genres)) = 0 THEN NULL
        ELSE SUBSTRING_INDEX(RIGHT(genres,
                    LENGTH(genres) - POSITION(',' IN genres) - 1),
                ',',
                1)
    END AS sub_genre_1,
    CASE
        WHEN (POSITION(',' IN genres)) = 0 THEN NULL
        WHEN CHAR_LENGTH(genres) - CHAR_LENGTH(REPLACE(genres, ', ', '1')) = 1 THEN NULL
        ELSE SUBSTRING_INDEX(RIGHT(genres,
                    LENGTH(genres) - LENGTH(SUBSTRING_INDEX(genres, ',', 2)) - 2),
                ',',
                1)
    END AS sub_genre_2,
    CASE
        WHEN (POSITION(',' IN genres)) = 0 THEN NULL
        WHEN CHAR_LENGTH(genres) - CHAR_LENGTH(REPLACE(genres, ', ', '1')) = 1 THEN NULL
        WHEN CHAR_LENGTH(genres) - CHAR_LENGTH(REPLACE(genres, ', ', '1')) = 2 THEN NULL
        ELSE SUBSTRING_INDEX(RIGHT(genres,
                    LENGTH(genres) - LENGTH(SUBSTRING_INDEX(genres, ',', 3)) - 2),
                ',',
                2)
    END AS sub_genre_3
FROM
    nintendo_games;
    
-- Steps include: adding new columns, populating them and removing original 'genres' column.

ALTER TABLE nintendo_games
ADD COLUMN main_genre VARCHAR(255);
ALTER TABLE nintendo_games
ADD COLUMN sub_genre_1 VARCHAR(255);
ALTER TABLE nintendo_games
ADD COLUMN sub_genre_2 VARCHAR(255);
ALTER TABLE nintendo_games
ADD COLUMN sub_genre_3 VARCHAR(255);

UPDATE nintendo_games 
SET 
    main_genre = SUBSTRING_INDEX(genres, ',', 1);

UPDATE nintendo_games 
SET 
    sub_genre_1 = CASE
        WHEN (POSITION(',' IN genres)) = 0 THEN NULL
        ELSE SUBSTRING_INDEX(RIGHT(genres,
                    LENGTH(genres) - POSITION(',' IN genres) - 1),
                ',',
                1)
    END;

UPDATE nintendo_games 
SET 
    sub_genre_2 = CASE
        WHEN (POSITION(',' IN genres)) = 0 THEN NULL
        WHEN CHAR_LENGTH(genres) - CHAR_LENGTH(REPLACE(genres, ', ', '1')) = 1 THEN NULL
        ELSE SUBSTRING_INDEX(RIGHT(genres,
                    LENGTH(genres) - LENGTH(SUBSTRING_INDEX(genres, ',', 2)) - 2),
                ',',
                1)
    END;

UPDATE nintendo_games 
SET 
    sub_genre_3 = CASE
        WHEN (POSITION(',' IN genres)) = 0 THEN NULL
        WHEN CHAR_LENGTH(genres) - CHAR_LENGTH(REPLACE(genres, ', ', '1')) = 1 THEN NULL
        WHEN CHAR_LENGTH(genres) - CHAR_LENGTH(REPLACE(genres, ', ', '1')) = 2 THEN NULL
        ELSE SUBSTRING_INDEX(RIGHT(genres,
                    LENGTH(genres) - LENGTH(SUBSTRING_INDEX(genres, ',', 3)) - 2),
                ',',
                2)
    END;
    
ALTER TABLE nintendo_games
DROP COLUMN genres;

-- Note: at this stage of data preparation, there can be said that primary key of 'nintendo games' table consists of two fields:
-- 'title' and 'platform', as one game can be released for different gaming platforms (Switch, iOS, 3DS etc.).

###### PART 3: INITIAL DATA ANALYSIS (LOOKING FOR FIRST INSIGHTS)
-- a) Steps include: finding intra-group studios which games were rated highest by the critics;

SELECT 
    main_developer, ROUND(AVG(meta_score), 0) AS avg_meta_score
FROM
    nintendo_games
GROUP BY main_developer
HAVING avg_meta_score IS NOT NULL
ORDER BY avg_meta_score DESC
LIMIT 10;

-- b) Steps include: finding platforms which correspond with games rated highest by the gamers.

SELECT 
    platform, ROUND(AVG(user_score), 2) AS avg_user_score
FROM
    nintendo_games
GROUP BY platform
HAVING avg_user_score IS NOT NULL
ORDER BY avg_user_score DESC;

-- c) Steps include: divide games into four groups (pokemons, mario, zelda and others) and compare ratings of critics.

WITH cte1 AS (
SELECT 
    title,
    platform,
    CASE
        WHEN title LIKE '%pokemon%' THEN 'Pokemon game'
        WHEN title LIKE '%mario%' THEN 'Mario game'
        WHEN title LIKE '%zelda%' THEN 'Zelda game'
        ELSE 'Other-topic game'
    END as popular_aspect
FROM
    nintendo_games)
SELECT 
    c.popular_aspect,
    ROUND(AVG(ng.meta_score), 2) AS avg_meta_score
FROM
    nintendo_games ng
JOIN cte1 c ON c.title=ng.title AND c.platform=ng.platform
GROUP BY c.popular_aspect
ORDER BY avg_meta_score DESC;

-- d) Steps include: finding out if there is any correlation between month of issuance of particular game and rating received in general.

SELECT DISTINCT
    MONTH(issuance_date) AS month_number,
    MONTHNAME(issuance_date) AS month_of_issuance,
    ROUND(AVG(user_score), 2) AS avg_user_score,
    ROUND(AVG(meta_score), 0) AS avg_meta_score
FROM
    nintendo_games
GROUP BY MONTHNAME(issuance_date) , MONTH(issuance_date)
ORDER BY month_number;

###### PART 4: SUMMARY AND FINAL INSIGHTS
/* At the beginning, in order to perform initial analysis and also to make deeper observations, the data in raw form had to be properly prepared, 
by performing several steps of cleaning and transforming it. Transformations allowed to correctly filter by date, remove unnecessary columns, records
and to isolate single game developers and genres. SQL commands helped to change original data, manipulate it and show the meaningful result sets at the end. 

INSIGHTS FROM INITIAL ANALYSIS:
- games produced by studios - Game Arts, Silicon Knights, Retro Studios - receive the best notes from professional critics 
- games with port to N64 platform receive the best reviews from gamers, in comparison to other platforms
- games in Zelda world are best rated by professional critics, with strong advantage over Mario, Pokemon and Other-topic games
- there is no significant correlation between month in which particular game is released and rates that it gets both from critics and gamers
*/