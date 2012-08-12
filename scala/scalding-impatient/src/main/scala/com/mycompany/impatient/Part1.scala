package com.mycompany.impatient

import com.twitter.scalding._

/**
 * Scalding version of "Cascading for the Impatient, Part I"
 * Article: http://www.cascading.org/2012/07/02/cascading-for-the-impatient-part-1/
 * scald.rb --local src/main/scala/com/mycompany/impatient/Part1.scala --input data/rain.txt --output data/output0.txt
 */
class Part1(args : Args) extends Job(args) {
  val input = Tsv(args("input"))
  val output = Tsv(args("output"))
  input.read.write(output)
}