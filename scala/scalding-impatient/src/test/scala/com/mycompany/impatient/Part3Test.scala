package com.mycompany.impatient

import com.twitter.scalding.{FieldConversions, Tsv, JobTest, TupleConversions}
import org.specs2.mutable.Specification
import cascading.tuple.Fields

class Part3Test extends Specification with TupleConversions with FieldConversions {

  val inputFileName = "inputFile.txt"
  val outputFileName = "outputFile.txt"

  "Word count with scrub" should {
    JobTest("com.mycompany.impatient.Part3").
      arg("input", inputFileName).
      arg("output", outputFileName).
      source(Tsv(inputFileName, ('docId, 'text)), List(("docId", "Hello WoRlD"),("docId", "HeLlo ScRub"))).
      sink[(String, Int)](Tsv(outputFileName)) {
      ob =>
        val map = ob.toMap[String, Int]
        "find 2 'hello', 1 'world' and 1 'scrub'" in {
          map("hello") must be_==(2)
          map("world") must be_==(1)
          map("scrub") must be_==(1)
        }
    }.run.finish
  }
}