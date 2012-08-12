package com.mycompany.impatient

import com.twitter.scalding._
import cascading.pipe.joiner.LeftJoin

/**
 * Scala version of "Cascading for the Impatient, Part V"
 * Article: http://www.cascading.org/2012/07/31/cascading-for-the-impatient-part-5/
 * Run: scald.rb --local src/main/scala/com/mycompany/impatient/Part5.scala --input data/rain.txt --output data/output4.txt --stop data/en.stop 
 */
class Part5(args : Args) extends Job(args) {
  
  def scrub(text : String) : String = {
    text.trim.
      toLowerCase.
      replaceAll("""[\[\]\(\),-]""", " ")
  }
  
  val input = Tsv(args("input"), ('docId, 'text))
  val output = Tsv(args("output"))

  val stop = Tsv(args("stop"), ('stopword)).read

  val tokens = input.read.
    map('text -> 'stext) { text : String => scrub(text) }.
    flatMap('stext -> 'word) { stext : String => stext.split("""\s+""") }.
    project('docId, 'word).
    joinWithSmaller('word -> 'stopword, stop, joiner = new LeftJoin).
    filter('stopword) { stopword : String => (stopword == null || stopword.isEmpty) }.
    project('docId, 'word)
    
  // TF
  val tf = tokens.
    groupBy(('docId, 'word)) { group => group.size }.
    rename('word -> 'tf_word).
    rename('size -> 'tf_count).
    project('docId, 'tf_word, 'tf_count)
    
  // D
  val d = tokens.
    unique('docId).
    groupAll { _.size }.
    rename('size -> 'n_docs)
    
  // DF
  val df = tokens.
    groupBy('word) { group => group.size }.
    rename('word -> 'df_word).
    rename('size -> 'df_count).
    project('df_word, 'df_count)
   
  // IDF
  val idf = df.crossWithTiny(d)
  
  // TF-IDF
  // tf_count * Math.log(n_docs / (1.0 + df_count))
  val tfidf = tf.joinWithSmaller('tf_word -> 'df_word, idf).
    map(('tf_count, 'n_docs, 'df_count) -> 'tfidf) {
      x : (Double, Double, Double) =>
        val (tfCount, nDocs, dfCount) = x
        tfCount * Math.log(nDocs / (1.0 + dfCount))
    }.
    project('docId, 'tf_word, 'tfidf).
    write(output)
}
