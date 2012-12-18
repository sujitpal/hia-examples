package com.mycompany.impatient

import cascading.tuple.Fields
import com.twitter.scalding._
import org.specs2.mutable.Specification

class Part5Test extends Specification with TupleConversions with FieldConversions {

  val documentCollection = "inputFile.txt"
  val stopwordFileName = "stopvalues.tsv"
  val tfidfOutput = "outputFile.txt"
  val stopwords = List(Tuple1("is"), Tuple1("i"))

  "TF-IDF job on a 2 document corpus, containing 4 words in total" should {
    JobTest("com.mycompany.impatient.Part5").
      arg("input", documentCollection).
      arg("output", tfidfOutput).
      arg("stop", stopwordFileName).
      source(Tsv(documentCollection, ('docId, 'text),sh=true), List(("doc01", "hello is world"), ("doc02", "hello I again"))).
      source(Tsv(stopwordFileName, ('stopword)), stopwords).
      sink[(String, Double, String)](Tsv(tfidfOutput, wh=true)) { ob =>
        "have an output of 4 terms" in {
          ob.size must be_==(4)
        }
        "have correct TF-IDF values for the 4 terms" in {
          ob(0) must be_==(("doc02", 0.5, "again"))
          ob(1) must be_==(("doc01", 0.0, "hello"))
          ob(2) must be_==(("doc02", 0.0, "hello"))
          ob(3) must be_==(("doc01", 0.5, "world"))
        }
      }.
      run.finish
  }

}