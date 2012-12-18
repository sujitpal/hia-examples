package com.mycompany.impatient

import org.specs2.mutable._
import com.twitter.scalding.{TupleConversions, Tsv, JobTest}

class Part1Test extends Specification with TupleConversions{

  val inputFileName = "inputFile.tsv"
  val outputFileName = "outputFile.tsv"

  "Tsv file copy job " should {
    JobTest("com.mycompany.impatient.Part1").
      arg("input", inputFileName).
      arg("output", outputFileName).
      source(Tsv(inputFileName), List(("Tab", "Separated", "Values"))).
      sink[(String, String, String)](Tsv(outputFileName)) {
      output =>
        "Take a TSV file as input and create a new TSV file with the same content" in {
          output(0)._1 must be_==("Tab")
          output(0)._2 must be_==("Separated")
          output(0)._3 must be_==("Values")
        }
    }.
      run.
      finish
  }
}