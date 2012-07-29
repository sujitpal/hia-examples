/* 
 * patent_similarity.pig
 * Given an input file of (citing_patent_number,cited_patent_number), this
 * script computes the Jaccard similarity between individual patents by
 * considering the overlap of the cited patents.
 */

-- pigudfs-1.0-SNAPSHOT.jar contains custom UDF JaccardSimilarity
REGISTER ./pigudfs-1.0-SNAPSHOT.jar; 

-- Load the text into a relation. Example:
-- (1,2)
-- (1,3)
-- (2,3)
-- (2,4)
-- (3,5)
-- citings = LOAD 'test.txt'
citings = LOAD 'input/cite75_99.txt' 
  USING PigStorage(',') AS (citing:int, cited:int);

-- Group by citing patent number. Example:
-- (1,{(1,2),(1,3)})
-- (2,{(2,3),(2,4)})
-- (3,{(3,5)})
citings_grpd = GROUP citings BY citing; 

-- Join previous 2 relations to include the cited patent in the relation
-- Tuple. Example:
-- (1,2,1,{(1,2),(1,3)})
-- (1,3,1,{(1,2),(1,3)})
-- (2,3,2,{(2,3),(2,4)})
-- (2,4,2,{(2,3),(2,4)})
-- (3,5,3,{(3,5)})
citings_joined = JOIN citings BY citing, citings_grpd BY group;

-- Eliminate the extra citings_grpd.group column and rename for sanity.
-- (1,2,{(1,2),(1,3)})
-- (1,3,{(1,2),(1,3)})
-- (2,3,{(2,3),(2,4)})
-- (2,4,{(2,3),(2,4)})
-- (3,5,{(3,5)})
citings_joined2 = FOREACH citings_joined 
  GENERATE $0 AS citing, $1 AS cited, $3 AS cite_pairs;

-- JOIN previous relation with citings_grpd to add the patents list
-- for the cited patent. We already have the patent list for the citing
-- patent. For reference, these relations are as follows:
-- citings_joined2: {citing: int,cited: int,cite_pairs: {(citing: int,cited: int)}}
-- citings_grpd: {group: int,citings: {(citing: int,cited: int)}}
-- Resulting data looks like this:
-- (1,2,{(1,2),(1,3)},2,{(2,3),(2,4)})
-- (1,3,{(1,2),(1,3)},3,{(3,5)})
-- (2,3,{(2,3),(2,4)},3,{(3,5)})
citings_joined3 = JOIN citings_joined2 BY cited, citings_grpd BY group;

-- Eliminate the extra citings_grpd.group value and rename cols for sanity.
-- Also eliminate the citing part of the tuples in both left_citeds and
-- right_citeds, so we can calculate similarity.
-- (1,2,{(2),(3)},{(3),(4)})
-- (1,3,{(2),(3)},{(5)})
-- (2,3,{(3),(4)},{(5)})
citings_joined4 = FOREACH citings_joined3 
  GENERATE $0 AS citing, $1 AS cited, 
           $2.cited AS left_citeds, $4.cited AS right_citeds;

-- Remove sim(A,A) because we know its always 1.0
citings_joined5 = FILTER citings_joined4 BY citing != cited;

-- Project the relation through a UDF
-- (1,2,0.0)
-- (1,3,0.0)
-- (2,3,0.0)
citings_similarity = FOREACH citings_joined5 
  GENERATE citing, cited, 
  com.mycompany.pigudfs.JaccardSimilarity(left_citeds, right_citeds) AS jsim;

-- Remove entries with 0 similarity
citings_similarity2 = FILTER citings_similarity BY jsim > 0.0;

-- Order the output by descending order of similarity
citings_similarity_ordrd = ORDER citings_similarity2 BY jsim DESC;

-- Store the output in a comma-separated format into output
STORE citings_similarity_ordrd INTO 'patent_sim_output' USING PigStorage(',');
