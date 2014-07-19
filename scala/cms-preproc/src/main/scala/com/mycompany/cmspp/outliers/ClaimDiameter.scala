package com.mycompany.cmspp.outliers

import com.twitter.scalding.Job
import com.twitter.scalding.Args
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tsv
import scala.io.Source

/**
 * Calculate claim diameters. The output of this job is fed to a Python
 * script that calculates the median and interquartile range and finds
 * outliers as a function of the interquartile range.
 */
class ClaimDiameter(args: Args) extends Job(args) {

  def extractPairs(line: String): List[(String,String)] = {
    val cols = line.split(",").toList
    val codes = (cols.slice(22, 27)  // ICD9 procedure code cols
      .map(x => if (x.isEmpty) x else "ICD9:" + x) 
      ::: cols.slice(31, 75)         // HCPCS (CPT4) procedure cols
      .map(x => if (x.isEmpty) x else "HCPCS:" + x))  		    
      .filter(x => (! x.isEmpty))
    val cjoin = for {codeA <- codes; codeB <- codes} yield (codeA, codeB)
    cjoin.filter(x => x._1 < x._2)
  }

  val output = Tsv(args("output"))

  val sqdists = TextLine(args("input"))
    .read
  	.flatMapTo('line -> ('codeA, 'codeB)) { line: String => extractPairs(line) }
    .groupBy('codeA, 'codeB) { group => group.size }
    .map('size -> 'sqdist) { x: Int => (1.0D / Math.pow(x, 2)) }
    .discard('size)

  val claims = TextLine(args("input"))
    .read
    .map('line -> 'claimID) { x: String => x.split(",")(0) }
    .flatMap('line -> ('codeA, 'codeB)) { x: String => extractPairs(x) }
    .discard('line)

  claims.joinWithSmaller(('codeA, 'codeB) -> ('codeA, 'codeB), sqdists)
    .discard('codeA, 'codeB)
    .groupBy('claimID) { group => group.average('sqdist -> 'mean) }
    .map('mean -> 'dist) { x: Double => Math.sqrt(x) }
    .discard('mean)
    .write(output)

}

object ClaimDiameter {
  def main(args: Array[String]): Unit = {
    // populate redis cache
    new ClaimDiameter(Args(List(
      "--local", "",
      "--input", "data/outpatient_claims.csv",
      "--output", "data/diameters.csv"
    ))).run
    Source.fromFile("data/diameters.csv")
      .getLines()
      .foreach(Console.println(_))
  }
}