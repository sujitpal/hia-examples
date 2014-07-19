package com.mycompany.cmspp.clusters

import com.twitter.scalding.Job
import com.twitter.scalding.Args
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tsv
import scala.io.Source

/**
 * Calculate code clusters. Algorithm used is distributed DBSCAN to
 * find clusters of code given the distances as calculated as a function
 * of the co-occurrence of codes in claims.
 */
class CodeCluster(args: Args) extends Job(args) {

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

  val Epsilon = args("epsilon").toDouble
  val MinPoints = args("minpoints").toInt
  val NumClusters = args("nclusters").toInt
  
  val output = Tsv(args("output"))

  val dists = TextLine(args("input"))
    .read
    // compute pair-wise distances between procedure codes
  	.flatMapTo('line -> ('codeA, 'codeB)) { line: String => extractPairs(line) }
    .groupBy('codeA, 'codeB) { group => group.size('sim) }
    .map('sim -> 'radius) { x: Int => (1.0D / x) }
    .discard('sim)
    // group by codeA and retain only records which are within epsilon distance
    .groupBy('codeA) { group => group.sortBy('radius).reverse }
    .filter('radius) { x: Double => x < Epsilon }
    
  val codeCounts = dists
    .groupBy('codeA) { group => 
      group.sizeAveStdev('radius -> ('count, 'mean, 'std)) 
    }
    // only retain codes that have at least MinPoints points within Epsilon
    .filter('count) { x: Int => x > MinPoints }
    .discard('std)

  val densities = dists.joinWithSmaller(('codeA -> 'codeA), codeCounts)
    .map(('mean, 'count) -> 'density) { x: (Double,Int) => 
      1.0D * Math.pow(x._2, 2) / Math.pow(x._1, 2)
    }
    .discard('radius, 'count)
    
  // sort the result by density descending and find the top N clusters
  val densestCodes = densities.groupAll { group => 
      group.sortBy('density).reverse }
    .unique('codeA)
    .limit(NumClusters)
    
  // join code densities with densest codes to find final clusters
  densities.joinWithTiny(('codeA -> 'codeA), densestCodes)
    .groupBy('codeA) { group => group.mkString('codeB, ",")}
    .write(output)
}

object CodeCluster {
  def main(args: Array[String]): Unit = {
    // populate redis cache
    new CodeCluster(Args(List(
      "--local", "",
      "--epsilon", "0.3",
      "--minpoints", "10",
      "--nclusters", "10",
      "--input", "data/outpatient_claims.csv",
      "--output", "data/clusters.csv"
    ))).run
//    Source.fromFile("data/clusters.csv")
//      .getLines()
//      .foreach(Console.println(_))
  }
}
