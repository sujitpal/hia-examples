package com.mycompany.cmspp

import scala.io.Source

import com.twitter.scalding.Args
import com.twitter.scalding.Csv
import com.twitter.scalding.Job
import com.twitter.scalding.RichPipe
import com.twitter.scalding.TextLine

class PreprocJob1(args: Args) extends Job(args) {

  def normalizeClaims(line: String): List[(String,String)] = {
    val inputColnames = Schemas.InpatientClaims.map(
      sym => sym.name)
    val outputColnames = Schemas.Codes.map(sym => sym.name)
    val ocolset = outputColnames.toSet
    val colvals = line.split(",")
    val memberId = colvals.head
    inputColnames.zip(colvals)
      .filter(nv => ocolset.contains(nv._1))
      .filter(nv => (! nv._2.isEmpty()))
      .map(nv => nv._1.split("_").head + ":" + nv._2)
      .map(code => (memberId, code))
  }
  
  val claims = TextLine(args("claims"))
    .flatMap(('line) -> ('DESYNPUF_ID, 'CLAIM_CODE)) {
      line: String => normalizeClaims(line)
    }
    .project(('DESYNPUF_ID, 'CLAIM_CODE))
    .groupBy(('DESYNPUF_ID, 'CLAIM_CODE)) { 
      grp => grp.size('NUM_CLAIMS) 
    }

  val members = RichPipe(claims)
    .project('DESYNPUF_ID)
    .unique('DESYNPUF_ID)
    .write(Csv(args("members")))

  val codes = RichPipe(claims)
    .project('CLAIM_CODE)
    .unique('CLAIM_CODE)
    .write(Csv(args("codes")))
    
  claims.write(Csv(args("output")))
}

object PreprocJob1 {
  def main(args: Array[String]): Unit = {
    // input files
    val claims = "data/inpatient_claims.csv"
    // output files
    val members = "data/members_list.csv"
    val codes = "data/codes_list.csv"
    val output = "data/claim_triples.csv"
    (new PreprocJob1(Args(List(
        "--local", "", 
        "--claims", claims,
        "--members", members,
        "--codes", codes,
        "--output", output)))
    ).run
    Console.println("==== members_list ====")
    Source.fromFile(members).getLines().slice(0, 3)
      .foreach(Console.println(_))
    Console.println("==== codes_list ====")
    Source.fromFile(codes).getLines().slice(0, 3)
      .foreach(Console.println(_))
    Console.println("==== claim_triples ====")
    Source.fromFile(output).getLines().slice(0, 3)
      .foreach(Console.println(_))
  }
}
