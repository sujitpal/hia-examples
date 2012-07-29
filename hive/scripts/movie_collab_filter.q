--
-- movie_collab_filter.q
-- Builds up a table of similar movie pairs ordered by count.
--

-- Create table to hold input data and load input data
-- Output:
-- 1	100	4	20120724
-- 2	100	5	20120724
-- 3	100	4	20120724
-- 1	200	4	20120724
-- 3	200	5	20120724
-- 1	300	1	20120724
CREATE TABLE ratings (uid INT, mid INT, rating INT, tstamp INT)
  ROW FORMAT DELIMITED                             
  FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/tmp/test.txt' OVERWRITE INTO TABLE ratings;

-- Only use ratings which are > 3
CREATE TABLE ratings2 (uid INT, mid INT);
INSERT OVERWRITE TABLE ratings2
  SELECT uid, mid FROM ratings
  WHERE rating > 3;

-- For each (uid,mid) pair, find all users who have the same mid
-- Then for each such record, find all movies with the same uid.
-- Output:
-- 100	200
-- 100	300
-- 300	100
-- 300	200
-- 100	200
-- 100	300
-- 200	100
-- 200	300
-- 200	100
-- 200	300
-- 100	200
-- 100	300
-- 300	100
-- 100	300
-- 100	300
-- 100	300
-- 200	100
-- 100	200
-- 200	100
-- 100	200
-- 100	200
CREATE TABLE mid_pairs (mid INT, rmid INT);
INSERT OVERWRITE TABLE mid_pairs 
  SELECT a.mid, c.mid 
  FROM ratings2 a JOIN ratings2 b ON (a.mid = b.mid) 
                  JOIN ratings2 c ON (b.uid = c.uid);

-- Eliminate pairs where the source and related mid are identical.
CREATE TABLE mid_pairs2 (mid INT, rmid INT);
INSERT OVERWRITE TABLE mid_pairs2
  SELECT mid, rmid
  FROM mid_pairs
  WHERE mid != rmid;

-- Group by (mid, rmid) and count occurrences
-- 100	200	6
-- 100	300	6
-- 200	100	4
-- 200	300	2
-- 300	100	2
-- 300	200	1
CREATE TABLE mid_counts (mid INT, rmid INT, cnt INT);
INSERT OVERWRITE TABLE mid_counts
  SELECT mid, rmid, COUNT(rmid)
  FROM mid_pairs2
  GROUP BY mid, rmid;

DROP TABLE ratings2;
DROP TABLE mid_pairs;
DROP TABLE mid_pairs2;

