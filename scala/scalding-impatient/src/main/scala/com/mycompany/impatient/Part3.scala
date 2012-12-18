package com.mycompany.impatient

import com.twitter.scalding._

object Part3 {
  def main(args: Array[String]) {
    (new Part3(Args(List("--local", "", "--input", "data/rain.txt", "--output", "data/output.txt")))).run

    import io.Source
    for (line <- Source.fromFile("data/output.txt").getLines())
      println(line)
  }
}

/**
 * Scalding version of "Cascading for the Impatient, Part III"
 * Article: http://www.cascading.org/2012/07/17/cascading-for-the-impatient-part-3/
 * Run: scald.rb --local src/main/scala/com/mycompany/impatient/Part3.scala --input data/rain.txt --output data/output2.txt
 */
class Part3(args : Args) extends Job(args) {
  
  def scrub(text : String) : String = {
    text.trim.
      toLowerCase
  }
  
  val input = Tsv(args("input"), ('docId, 'text))
  val output = Tsv(args("output"))
  input.read.
    mapTo('text -> 'stext) { text : String => scrub(text) }.
    flatMap('stext -> 'word) { stext : String => stext.split("[^a-z0-9]") }.
    groupBy('word) { group => group.size }.
    write(output)
}