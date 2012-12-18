package com.mycompany.impatient

import com.twitter.scalding._
import cascading.pipe.joiner.LeftJoin

object Part4 {
  def main(args: Array[String]) {
    (new Part4(Args(List("--local", "", "--input", "data/rain.txt", "--output", "data/output.txt", "--stop", "data/en.stop")))).run

    import io.Source
    for (line <- Source.fromFile("data/output.txt").getLines())
      println(line)
  }
}

/**
 * Scala version of "Cascading for the Impatient, Part IV"
 * Article: http://www.cascading.org/2012/07/24/cascading-for-the-impatient-part-4/
 * Run: scald.rb --local src/main/scala/com/mycompany/impatient/Part4.scala --input data/rain.txt --output data/output3.txt --stop data/en.stop 
 */
class Part4(args : Args) extends Job(args) {
  
  def scrub(text : String) : String = {
    text.trim.
      toLowerCase.
      replaceAll("""[\[\]\(\),-]""", " ")
  }
  
  val input = Tsv(args("input"), ('docId, 'text))
  val output = Tsv(args("output"))

  val stop = Tsv(args("stop"), ('stopword)).read

  input.read.
    mapTo('text -> 'stext) { text : String => scrub(text) }.
    flatMap('stext -> 'word) { stext : String => stext.split("""\s+""") }.
    project('word).
    joinWithSmaller('word -> 'stopword, stop, joiner = new LeftJoin).
    filter('stopword) { stopword : String => (stopword == null || stopword.isEmpty) }.
    groupBy('word) { group => group.size }.
    write(output)
}