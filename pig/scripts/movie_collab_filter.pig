/*
 * movie_collab_filter.pig
 * Finds movies to recommend based on collaborative filtering.
 * Each movie is mapped to a user and then each user is mapped to other
 * movies rated highly by the user. The other movies are candidates for
 * recommendation order by rating desc.
 */
-- Register custom UDF jar
REGISTER ./pigudfs-1.0-SNAPSHOT.jar;

-- load data
-- The field delimiter here is "::" which can't be parsed by PigStorage, so
-- we need to sed the input to replace "::" to "\t". Output:
-- (1,100,4,20120724)
-- (2,100,5,20120724)
-- (3,100,4,20120724)
-- (1,200,4,20120724)
-- (3,200,5,20120724)
-- (1,300,1,20120724)
-- (2,300,4,20120724)
ratings = LOAD 'input/ml-ratings.dat' USING PigStorage('\t') 
  AS (uid:int, mid:int, rating:int, timestamp:chararray);

-- since this is a recommender system we want to only consider entries whose
-- ratings >= 4 (on a 5-point rating scale). Also remove extraneous cols.
-- (1,100)
-- (2,100)
-- (3,100)
-- (1,200)
-- (3,200)
-- (2,300)
ratings2 = FILTER ratings BY rating > 3;
ratings3 = FOREACH ratings2 GENERATE uid, mid;

-- Build copy of ratings3 for self JOINs below
ratings3_copy = FOREACH ratings3 GENERATE *;

-- For each movie, first find all other users who have rated the movie
-- highly, then for each such movie, find all the other movies for the
-- same user. The other movies are the ones related to the original 
-- movie through the magic of collaborative filtering.
ratings_join_mid = JOIN ratings3 BY mid, ratings3_copy BY mid;
ratings_join_mid2 = FOREACH ratings_join_mid 
  GENERATE $0 AS uid, $1 AS mid, $3 AS tmid;
ratings_join_uid = JOIN ratings_join_mid2 BY uid, ratings3 BY uid;

-- Remove rows where the original movie and the "other" movie are the
-- same (because we don't want to recommend the same movie to the user).
-- Finally remove extraneous columns. Final output after this block:
-- (100,200)
-- (200,100)
-- (100,200)
-- (200,100)
-- (100,200)
-- (300,100)
-- (100,300)
-- (100,300)
-- (100,300)
-- (100,200)
-- (100,200)
-- (200,100)
-- (200,100)
-- (100,200)
ratings_join_uid2 = FILTER ratings_join_uid BY $1 != $4;
ratings_join_uid3 = FOREACH ratings_join_uid2 
  GENERATE $1 AS mid, $4 AS rmid;

-- Group the related movies so we can generate a count.
-- (100,{(100,200),(100,200),(100,200),(100,300),(100,300),
--        (100,300),(100,200),(100,200),(100,200)})
-- (200,{(200,100),(200,100),(200,100),(200,100)})
-- (300,{(300,100)})
ratings_cnt = group ratings_join_uid3 BY mid;

-- Use custom UDF to dedup and rerank related movie tuples by count.
-- (100,{(100,200),(100,300)})
-- (200,{(200,100)})
-- (300,{(300,100)})
ratings_cnt_ordrd = FOREACH ratings_cnt 
  GENERATE group AS mid, 
  com.mycompany.pigudfs.OrderByCountDesc($1) AS ordered_mids;

-- Flatten the result so data can be fed into a relational table.
-- (100,200)
-- (100,300)
-- (200,100)
-- (300,100)
ratings_cnt_flat = FOREACH ratings_cnt_ordrd
  GENERATE mid,
  FLATTEN(ordered_mids.mid_r) AS rmid;

-- Store output into HDFS
STORE ratings_cnt_flat INTO 'movie_collab_filter_output' USING PigStorage('\t');

