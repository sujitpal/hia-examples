package com.mycompany.diseasegraph

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import SparkContext._

object GraphDataGenerator {

  private val AppName = "GraphDataGeneratorJob"

  def execute(master: String, args: List[String], 
      jars: Seq[String]=Nil): Unit = {
    
    val sc = new SparkContext(master, AppName, null, jars)
    
    // dedup member so the record with the highest number
    // of comorbidities is retained
    val membersDeduped = dedupMemberInfo(sc.textFile(args(0)))
    // normalize member to (member_id,(disease,weight))
    val membersNormalized = normalizeMemberInfo(membersDeduped)

    // normalize inpatient and outpatient claims to 
    // (member_id,(code,weight))
    // This involves grouping by (member_id, claim_id) and
    // computing the code weights, then removing claim_id.
    // Codes used in inpatient claims are ICD-9 and the
    // ones used for outpatient claims are HCPCS
    val claimsNormalized = normalizeClaimInfo(
      sc.textFile(args(1)), (30, 35)) ++
      normalizeClaimInfo(sc.textFile(args(2)), (31, 75))
    
    // join the membersNormalized and claimsNormalized RDDs
    // on memberId to get mapping of disease to code
    // membersNormalized: (member_id, (disease, d_weight))
    // claimsNormalized: (member_id, (code, c_weight))
    // diseaseCodes: (disease, code, weight)
    val diseaseCodes = joinMembersWithClaims(
      membersNormalized, claimsNormalized)
    diseaseCodes.map(t => format(t)).saveAsTextFile(args(3))
    
    // finally do a self join with the diseaseCodes RDD joining
    // on code to compute a measure of disease-disease similarity
    // by the weight of the shared procedure codes to produce
    // (disease_A, disease_B, weight)
    val diseaseAllPairs = selfJoinDisease(diseaseCodes)
    diseaseAllPairs.map(t => format(t)).saveAsTextFile(args(4))
  }
  
  def format(t: (String,String,Double)): String = {
    "%s,%s,%.3f".format(t._1, t._2, t._3)
  }
  
  val columnDiseaseMap = Map(
    12 -> "ALZM",
    13 -> "CHF",
    14 -> "CKD",
    15 -> "CNCR",
    16 -> "COPD",
    17 -> "DEP",
    18 -> "DIAB",
    19 -> "IHD",
    20 -> "OSTR",
    21 -> "ARTH",
    22 -> "TIA"
  )

  def dedupMemberInfo(input: RDD[String]): 
      RDD[(String,List[String])] = {
    input.map(line => {
      val cols = line.split(",")
      val memberId = cols(0)
      val comorbs = columnDiseaseMap.keys
        .toList.sorted
        .map(e => cols(e))
      (memberId, comorbs.toList)
    })
    .reduceByKey((v1, v2) => {
      // 1 == Yes, 2 == No
      val v1fsize = v1.filter(_ == 1).size
      val v2fsize = v2.filter(_ == 1).size
      if (v1fsize > v2fsize) v1 else v2
    })
  }
  
  def normalizeMemberInfo(input: RDD[(String,List[String])]): 
      RDD[(String,(String,Double))]= {
    input.flatMap(elem => {
      val diseases = elem._2.zipWithIndex
        .map(di => if (di._1.toInt == 1) columnDiseaseMap(di._2 + 12) else "")
        .filter(d => ! d.isEmpty())
      val weight = 1.0 / diseases.size
      diseases.map(disease => (elem._1, (disease, weight)))
    })
  }
  
  def normalizeClaimInfo(input: RDD[String], 
      pcodeIndex: (Int,Int)): RDD[(String,(String,Double))] = {
    input.flatMap(line => {
      val cols = line.split(",")
      val memberId = cols(0)
      val claimId = cols(1)
      val procCodes = cols.slice(pcodeIndex._1, pcodeIndex._2)
      procCodes.filter(pcode => ! pcode.isEmpty)
        .map(pcode => ("%s:%s".format(memberId, claimId), pcode))
    })
    .groupByKey()
    .flatMap(grouped => {
      val memberId = grouped._1.split(":")(0)
      val weight = 1.0 / grouped._2.size
      val codes = grouped._2.toList
      codes.map(code => {
        (memberId, (code, weight))
      })
    })
  }
  
  def joinMembersWithClaims(members: RDD[(String,(String,Double))],
      claims: RDD[(String,(String,Double))]): 
      RDD[(String,String,Double)] = {
    members.join(claims)
      .map(rec => {
        val disease = rec._2._1._1
        val code = rec._2._2._1
        val weight = rec._2._1._2 * rec._2._2._2
        (List(disease, code).mkString(":"), weight)
      })
      .reduceByKey(_ + _)
      .sortByKey(true)
      .map(kv => {
        val Array(k,v) = kv._1.split(":")
        (k, v, kv._2)
      })
  }
  
  def selfJoinDisease(dcs: RDD[(String,String,Double)]): 
      RDD[(String,String,Double)] = {
    val dcsKeyed = dcs.map(t => (t._2, (t._1, t._3)))
    dcsKeyed.join(dcsKeyed)
      // join on code and compute edge weight 
      // between disease pairs
      .map(rec => {
        val ldis = rec._2._1._1
        val rdis = rec._2._2._1
        val diseases = Array(ldis, rdis).sorted
        val weight = rec._2._1._2 * rec._2._2._2
        (diseases(0), diseases(1), weight)
      })
      // filter out cases where LHS == RHS
      .filter(t => ! t._1.equals(t._2))
      // group on (LHS + RHS)
      .map(t => (List(t._1, t._2).mkString(":"), t._3))
      .reduceByKey(_ + _)
      .sortByKey(true)
      // convert back to triple format
      .map(p => {
        val Array(lhs, rhs) = p._1.split(":")
        (lhs, rhs, p._2)
      })
  }
}