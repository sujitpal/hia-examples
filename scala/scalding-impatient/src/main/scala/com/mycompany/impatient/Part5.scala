package com.mycompany.impatient

import com.twitter.scalding._
import cascading.pipe.joiner.LeftJoin

object Part5 {
  def main(args: Array[String]) {
    (new Part5(Args(List("--local","","--input","data/rain.txt","--output","data/output.txt","--stop","data/en.stop")))).run

    import io.Source
    for (line <- Source.fromFile("data/output.txt").getLines())
      println(line)
  }
}
/**
 * Scala version of "Cascading for the Impatient, Part V"
 * Article: http://www.cascading.org/2012/07/31/cascading-for-the-impatient-part-5/
 * Run: scald.rb --local src/main/scala/com/mycompany/impatient/Part5.scala --input data/rain.txt --output data/output4.txt --stop data/en.stop 
 */
class Part5(args : Args) extends Job(args) {
  
  def scrub(text : String) : String = {
    text.trim.
      toLowerCase.
      replaceAll("""[\[\]\(\).,-]""", " ")
  }
  
  val input = Tsv(args("input"), ('docId, 'text), sh = true)
  val output = Tsv(args("output"), wh=true)

  val stop = Tsv(args("stop"), ('stopword)).read

  val tokens = input.read.
    map('text -> 'stext) { text : String => scrub(text) }.
    flatMap('stext -> 'word) { stext : String => stext.split("""[\s]+""") }.
    project('docId, 'word).
    joinWithSmaller('word -> 'stopword, stop, joiner = new LeftJoin).
    filter('stopword) { stopword : String => (stopword == null || stopword.isEmpty) }.
    project('docId, 'word)


  // TF
  val wordCountInDoc = tokens.
    groupBy(('docId, 'word)) { group => group.size }.
    rename('word -> 'tf_word).
    rename('size -> 'tf_count).
    project('docId, 'tf_word, 'tf_count)

  val totalWordCountPerDoc = wordCountInDoc.
    groupBy('docId) { _.sum('tf_count -> 'totalCountInDoc)}.
    project('docId,'totalCountInDoc)

  val tf = wordCountInDoc.joinWithSmaller('docId -> 'docId, totalWordCountPerDoc).
    map(('tf_count,'totalCountInDoc) -> 'tf_count) {x : (Double,Double) => (x._1/x._2)}.
    project('docId,'tf_count,'tf_word)

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

  def log2(x: Double) = math.log10(x) / math.log10(2)

  // TF-IDF
  val tfidf = tf.joinWithSmaller('tf_word -> 'df_word, idf).
    map(('tf_count, 'n_docs, 'df_count) -> 'tfidf) {
      x : (Double, Double, Double) =>
        val (tfCount, nDocs, dfCount) = x

        val idf  = log2(nDocs / (dfCount))

        (tfCount * idf)
    }.
    project('docId, 'tfidf, 'tf_word).
    write(output)
}
