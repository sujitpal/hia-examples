package com.mycompany.diseasegraph

import org.junit.Test
import java.io.File
import com.google.common.io.Files
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import scala.io.Source
import org.junit.Assert
import org.junit.AfterClass

class GraphDataGeneratorTest {


  @Test
  def testDedupMemberInfo(): Unit = {
    val infile = "src/test/resources/member_dup.csv"
    val outdir = "src/test/resources/outputs_1"
    forceDeleteIfExists(outdir)
    val sc = new SparkContext(
      "local", "GraphDataGeneratorTest", null, null)
    try {
      val input = sc.textFile(infile)
      val output = GraphDataGenerator.dedupMemberInfo(input)
        .map(e => List(e._1, e._2.mkString(",")).mkString(","))
      output.saveAsTextFile(outdir)
      val outlines = Source
        .fromFile(new File(outdir, "part-00000"))
        .getLines
        .filter(line => line.startsWith("0001448457F2ED81"))
        .toList
      Assert.assertEquals(1, outlines.size)
      Assert.assertTrue(outlines.head.endsWith("1,1,1,2,2,1,1,1,2,2,2"))
    } finally {
      sc.stop
    }
  }
  
  @Test
  def testNormalizeMemberInfo(): Unit = {
    val infile = "src/test/resources/member_denorm.csv"
    val outdir = "src/test/resources/outputs_2"
    forceDeleteIfExists(outdir)
    val sc = new SparkContext(
      "local", "GraphDataGeneratorTest", null, null)
    try {
      val input = sc.textFile(infile)
        .map(line => {
          val cols = line.split(",")
          (cols(0), cols.slice(1,12).toList)
        })
      val output = GraphDataGenerator.normalizeMemberInfo(input)
        .map(t => "%s,%s,%.3f".format(t._1, t._2._1, t._2._2))
      output.saveAsTextFile(outdir)
      val outlines = Source.fromFile(new File(outdir, "part-00000"))
        .getLines
        .filter(line => line.contains(",DEP,"))
      Assert.assertEquals(3, outlines.size)
    } finally {
      sc.stop
    }
  }
  
  @Test
  def testNormalizeInpatientInfo(): Unit = {
    val infile = "src/test/resources/inpatient_denorm.csv"
    val outdir = "src/test/resources/outputs_3"
    forceDeleteIfExists(outdir)
    val sc = new SparkContext(
      "local", "GraphDataGeneratorTest", null, null)
    try {
      val input = sc.textFile(infile)
      val output = GraphDataGenerator.normalizeClaimInfo(
        input, (30, 35))
        .map(t => "%s,%s,%.3f".format(t._1, t._2._1, t._2._2))
      output.saveAsTextFile(outdir)
      val outlines = Source.fromFile(new File(outdir, "part-00000"))
        .getLines
        .filter(line => line.contains(",V433,"))
        .toList
      Assert.assertEquals(1, outlines.size)
      Assert.assertEquals(0.2, 
        outlines.head.split(",")(2).toDouble, 0.001)
    } finally {
      sc.stop
    }
  }
  
  @Test
  def testNormalizeOutpatientInfo(): Unit = {
    val infile = "src/test/resources/outpatient_denorm.csv"
    val outdir = "src/test/resources/outputs_4"
    forceDeleteIfExists(outdir)
    val sc = new SparkContext(
      "local", "GraphDataGeneratorTest", null, null)
    try {
      val input = sc.textFile(infile)
      val output = GraphDataGenerator.normalizeClaimInfo(
          input, (31, 75))
        .map(t => "%s,%s,%.3f".format(t._1, t._2._1, t._2._2))
      output.saveAsTextFile(outdir)
      val outlines = Source.fromFile(new File(outdir, "part-00000"))
        .getLines
        .filter(line => line.contains(",87086,"))
        .toList
      Assert.assertEquals(3, outlines.size)
    } finally {
      sc.stop
    }
  }
  
  @Test
  def testJoinMembersWithClaims(): Unit = {
    val minfile = "src/test/resources/member_norm.csv"
    val iinfile = "src/test/resources/inpatient_norm.csv"
    val oinfile = "src/test/resources/outpatient_norm.csv"
    val outdir = "src/test/resources/outputs_5"
    forceDeleteIfExists(outdir)
    val sc = new SparkContext(
      "local", "GraphDataGeneratorTest", null, null)
    try {
      val members = sc.textFile(minfile)
        .map(line => {
          val Array(memberId, disease, dweight) = line.split(",")
          (memberId, (disease, dweight.toDouble))
      })
      val claims = (sc.textFile(iinfile) ++ sc.textFile(oinfile))
        .map(line => {
          val Array(memberId, code, cweight) = line.split(",")
          (memberId, (code, cweight.toDouble))
      })
      val output = GraphDataGenerator.joinMembersWithClaims(members, claims)
        .map(t => "%s,%s,%.3f".format(t._1, t._2, t._3))
      output.saveAsTextFile(outdir)
      val outfile = new File(outdir, "part-00000")
      Assert.assertTrue(outfile.exists())
      Assert.assertTrue(Source.fromFile(outfile).getLines.size > 0)
    } finally {
      sc.stop
    }
  }

  @Test
  def testSelfJoinDisease(): Unit = {
    val infile = "src/test/resources/disease_code_pairs.csv"
    val outdir = "src/test/resources/outputs_6"
    forceDeleteIfExists(outdir)
    val sc = new SparkContext(
      "local", "GraphDataGeneratorTest", null, null)
    try {
      val dcpairs = sc.textFile(infile)
        .map(line => {
          val Array(disease, code, weight) = line.split(",")
          (disease, code, weight.toDouble)
        })
      val output = GraphDataGenerator.selfJoinDisease(dcpairs)
        .map(t => "%s,%s,%.3f".format(t._1, t._2, t._3))
      output.saveAsTextFile(outdir)
      val outfile = new File(outdir, "part-00000")
      Assert.assertEquals(15, Source.fromFile(outfile).getLines.size)
    } finally {
      sc.stop
    }
  }
  
  @Test
  def testExecute(): Unit = {
    val args = List(
      "data/benefit_summary_1000.csv",
      "data/inpatient_claims_1000.csv",
      "data/outpatient_claims_1000.csv",
      "src/test/resources/final_outputs/disease_codes",
      "src/test/resources/final_outputs/diseases"
    )
    forceDeleteIfExists(args(3))
    forceDeleteIfExists(args(4))
    GraphDataGenerator.execute(master="local", args=args)
  }

  def forceDeleteIfExists(dir: String): Unit = {
    val f = new File(dir)
    if (f.exists() && f.isDirectory()) FileUtils.forceDelete(f)
  }
}
