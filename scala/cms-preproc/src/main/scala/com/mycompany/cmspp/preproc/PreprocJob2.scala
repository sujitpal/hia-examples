package com.mycompany.cmspp.preproc

import scala.io.Source
import com.twitter.scalding.Args
import com.twitter.scalding.Csv
import com.twitter.scalding.Job
import com.twitter.scalding.mathematics.Matrix.pipeExtensions

class PreprocJob2(args: Args) extends Job(args) {

  val benefits = Csv(args("benefits"), 
    fields=Schemas.BenefitSummary)
  val claims = Csv(args("claims"), 
    fields=List('DESYNPUF_ID, 'CLAIM_CODE, 'NUM_CLAIMS))
  val memberDict = Csv(args("members"),
    fields=List('MEM_IDX, 'DESYNPUF_ID))
  val codesDict = Csv(args("codes"), 
    fields=List('COD_IDX, 'CLAIM_CODE))
  
  claims.joinWithSmaller('DESYNPUF_ID -> 'DESYNPUF_ID, memberDict)
    .joinWithSmaller('CLAIM_CODE -> 'CLAIM_CODE, codesDict)
    .toMatrix[Long,Long,Double]('MEM_IDX, 'COD_IDX, 'NUM_CLAIMS)
    .rowL2Normalize
    .pipe
    .mapTo(('row, 'col, 'val) -> ('row, 'colval)) { 
      row: (Long, Long, Double) => 
        (row._1, row._2.toString + ":" + row._3.toString)
    }
    .groupBy('row) { grp => grp.mkString('colval, ",") }
    .write(Csv(args("xmatrix")))
    
  benefits.project(Schemas.Diseases)
    .joinWithSmaller('DESYNPUF_ID -> 'DESYNPUF_ID, memberDict)
    .discard('DESYNPUF_ID)
    .project('MEM_IDX :: Schemas.Diseases.tail)
    .write(Csv(args("yvectors")))
}

object PreprocJob2 {
  def main(args: Array[String]): Unit = {
    // input files
    val benefits = "data/benefit_summary.csv"
    val triples = "data/claim_triples.csv"
    val memberDict = "data/members_dict.csv"
    val codeDict = "data/codes_dict.csv"
    // output files
    val xmatrix = "data/x_matrix.csv"
    val yvectors = "data/y_vectors.csv"
    (new PreprocJob2(Args(List(
      "--local", "",
      "--benefits", benefits,
      "--claims", triples,
      "--members", memberDict,
      "--codes", codeDict,
      "--xmatrix", xmatrix,
      "--yvectors", yvectors)))
    ).run
    Console.println("==== xmatrix.csv ====")
    Source.fromFile(xmatrix).getLines()  //.slice(0, 3)
      .foreach(Console.println(_))
    Console.println("==== yvectors.csv ====")
    Source.fromFile(yvectors).getLines().slice(0, 3)
      .foreach(Console.println(_))
  }
}