package com.mycompany.impatient

import org.specs2.mutable.Specification
import com.twitter.scalding.{FieldConversions, Tsv, JobTest, TupleConversions}

class Part2Test extends Specification with TupleConversions with FieldConversions {

  val inputFileName = "input.tsv"
  val outputFileName = "output.tsv"

  "Word count job" should {
    JobTest("com.mycompany.impatient.Part2").
      arg("input", inputFileName).
      arg("output", outputFileName).
      source(Tsv(inputFileName, ('docId, 'text)), List(("docId", "Hello world"), ("docId", "Hello you"))).
      sink[(String, Int)](Tsv(outputFileName)) {
      output =>
        val map = output.toMap[String, Int]
        "find 2 'Hello', 1 'world' and 1 'you'" in {
          map("Hello") must be_==(2)
          map("world") must be_==(1)
          map("you") must be_==(1)
        }
    }.
      run.
      finish
  }
}