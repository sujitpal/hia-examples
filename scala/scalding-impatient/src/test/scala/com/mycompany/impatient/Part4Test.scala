package com.mycompany.impatient

import com.twitter.scalding._
import org.specs2.mutable.Specification

class Part4Test extends Specification with TupleConversions with FieldConversions {

  val inputFileName = "inputFile.txt"
  val outputFileName = "outputFile.txt"
  val stopwordFileName = "stopvalues.tsv"
  val stopwords = List(Tuple1("i"), Tuple1("you"))

  "Word count with scrub and stop words" should {
    JobTest("com.mycompany.impatient.Part4").
      arg("input", inputFileName).
      arg("output", outputFileName).
      arg("stop", stopwordFileName).
      source(Tsv(inputFileName, ('docId, 'text)), List(("docId", "Hello HeLlo"), ("docId","i ScRub"))).
      source(Tsv(stopwordFileName, ('stopword)), stopwords).
      sink[(String, Int)](Tsv(outputFileName)) {
      ob =>
        val map = ob.toMap[String, Int]
        "find 2 'hello', 1 'scrub' and no 'I'" in {
          map("hello") must be_==(2)
          map("scrub") must be_==(1)
          map.size must be_==(2)
        }
    }.run.finish
  }

}