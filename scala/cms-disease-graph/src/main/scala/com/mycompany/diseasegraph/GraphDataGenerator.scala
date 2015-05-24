package com.mycompany.diseasegraph

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

import SparkContext._

object GraphDataGenerator {

	// column number => comorbidity in benefits_summary
	val ColumnDiseaseMap = Map(
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

	def main(args: Array[String]): Unit = {
		if (args.size != 5) {
			Console.println("""
Usage: GraphDataGeneratorJob \
	s3://path/to/benefit_summary.csv \
	s3://path/to/inpatient_claims.csv \
	s3://path/to/outpatient_claims.csv \
	s3://path/to/disease_code_output \
	s3://path/to/disease_pairs_output""")
		} else {
			val conf = new SparkConf().setAppName("GraphDataGenerator")
			val sc = new SparkContext(conf)

			// permissions to read and write data on S3
			sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", sys.env("AWS_ACCESS_KEY"))
			sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", sys.env("AWS_SECRET_KEY"))

			// dedupe member so only record with highest number of
			// comorbidities is retained
			val membersDeduped = sc.textFile(args(0))
				// remove heading line (repeats)
				.filter(line => ! line.startsWith("\""))
				// extract (memberId, [comorbidity_indicators])
				.map(line => {
					val cols = line.split(",")
					val memberId = cols(0)
					val comorbs = ColumnDiseaseMap.keys.toList.sorted
						.map(e => cols(e).toInt)
					(memberId, comorbs)
				})
				.reduceByKey((v1, v2) => {
					// 1 == Yes, 2 == No
					val v1size = v1.filter(_ == 1).size
					val v2size = v2.filter(_ == 1).size
					if (v1size > v2size) v1 else v2
				})
				// normalize to (member_id, (disease, weight)) 
				.flatMap(x => {
					val diseases = x._2.zipWithIndex
					  .filter(di => di._1 == 1) // retain only Yes
					  .map(di => ColumnDiseaseMap(di._2 + 12)) 
					val weight = 1.0 / diseases.size
					diseases.map(disease => (x._1, (disease, weight)))
				})

			// normalize inpatient and outpatient claims to 
			// (member_id, (code, weight))
			val inpatientClaims = sc.textFile(args(1))
				// remove heading line (repeats)
			  	.filter(line => ! line.startsWith("\""))
			  	// extracts (member_id:claims_id, proc_codes)
			  	.map(line => {
			  		val cols = line.split(",")
			  		val memberId = cols(0)
			  		val claimsId = cols(1)
			  		val procCodes = cols.slice(30, 35)
			  		  .filter(pc => ! pc.isEmpty())
			  		val memberClaimId = Array(memberId, claimsId).mkString(":")
			  		(memberClaimId, procCodes)
			  	})
			  	// remove encounters with no procedure codes (they
			  	// may have other codes we are not interested in)
			  	.filter(claimProcs => claimProcs._2.size > 0)
			  	// find list of procedures done per encounter
			  	.groupByKey()
			  	// reweight codes per encounter. If many codes were
			  	// administered, then weigh them lower (assuming doctors
			  	// have limited time per patient, so more codes mean
			  	// that each code is less important - this assumption is
			  	// not necessarily correct, but lets go with it).
			  	.flatMap(grouped => {
			  		val memberId = grouped._1.split(":")(0)
			  		val codes = grouped._2.flatMap(x => x).toList
			  		val weight = 1.0 / codes.size
			  		codes.map(code => (memberId, (code, weight)))
			  	})
			  	
			val outpatientClaims = sc.textFile(args(1))
			  	.filter(line => ! line.startsWith("\""))
			  	.map(line => {
			  		val cols = line.split(",")
			  		val memberId = cols(0)
			  		val claimsId = cols(1)
			  		val procCodes = cols.slice(31, 75)
			  		  .filter(pc => ! pc.isEmpty())
			  		val memberClaimId = Array(memberId, claimsId).mkString(":")
			  		(memberClaimId, procCodes)
			  	})
			  	.filter(claimProcs => claimProcs._2.size > 0)
			  	.groupByKey()
			  	.flatMap(grouped => {
			  		val memberId = grouped._1.split(":")(0)
			  		val codes = grouped._2.flatMap(x => x).toList
			  		val weight = 1.0 / codes.size
			  		codes.map(code => (memberId, (code, weight)))
			  	})
			  	
			// combine the two RDDs into one
			inpatientClaims.union(outpatientClaims)

			// join membersDeduped and inpatientClaims on member_id
			// to get (code, (disease, weight))
			val codeDisease = membersDeduped.join(inpatientClaims)
				.map(joined => {
					val disease = joined._2._1._1
					val procCode = joined._2._2._1
					val weight = joined._2._1._2 * joined._2._2._2
					(Array(disease, procCode).mkString(":"), weight)
				})
				// combine weights for same disease + procedure code
				.reduceByKey(_ + _)
				// key by procedure code for self join
				.map(reduced => {
					val Array(disease, procCode) = reduced._1.split(":")
					(procCode, (disease, reduced._2))
				})
				.cache()
				
			// save a copy for future analysis
			codeDisease.map(x => "%s\t%s\t%.5f".format(x._2._1, x._1, x._2._2))
				.saveAsTextFile(args(3))
			
			// finally self join on procedure code. The idea is to 
			// compute the relationship between diseases by the weighted
			// sum of procedures that have been observed to have been 
			// done for people with the disease
			val diseaseDisease = codeDisease.join(codeDisease)
				// eliminate cases where LHS == RHS
				.filter(dd => ! dd._2._1._1.equals(dd._2._2._1))
				// compute disease-disease relationship weights
				.map(dd => {
					val lhsDisease = dd._2._1._1
					val rhsDisease = dd._2._2._1
					val diseases = Array(lhsDisease, rhsDisease).sorted
					val weight = dd._2._1._2 * dd._2._2._2
					(diseases.mkString(":"), weight)
				})
				// combine the disease pair weights
				.reduceByKey(_ + _)
				// bring it into a single file for convenience
				.coalesce(1, false)
				// sort them (purely cosmetic reasons, and they should
				// be small enough at this point to make this feasible
				.sortByKey()
				// split it back out for rendering
				.map(x => {
					val diseases = x._1.split(":")
					"%s\t%s\t%.5f".format(diseases(0), diseases(1), x._2)
				})
				.saveAsTextFile(args(4))

			sc.stop()
		}
	}
}	
