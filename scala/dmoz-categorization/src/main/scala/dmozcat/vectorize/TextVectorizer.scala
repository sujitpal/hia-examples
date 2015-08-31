package dmozcat.vectorize

import scala.Array.canBuildFrom

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import breeze.linalg.DenseVector
import breeze.linalg.InjectNumericOps
import breeze.linalg.norm

object TextVectorizer {

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
        
        // arguments
        val awsAccessKey = args(0)
        val awsSecretKey = args(1)
        val inputFile = args(2)
        val word2vecFile = args(3)
        val outputDir = args(4)
        
        val conf = new SparkConf()
        conf.setAppName("TextVectorizer")
        
        val sc = new SparkContext(conf)
        
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", awsAccessKey)
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", awsSecretKey)
        
        val input = sc.textFile(inputFile)
            .map(line => parseLine(line))
            .mapPartitions(p => extractNGrams(p)) // (key, ngram)
            .map(kv => (kv._2, kv._1))            // (ngram, key)
            
        val wordVectors = sc.textFile(word2vecFile)
            .map(line => {
                val Array(word, vecstr) = line.split("\t")
                val vector = new DenseVector(vecstr.split(",").map(_.toDouble))
                (word.toLowerCase, vector)        // (ngram, vector)
            })
        
        // join input to wordVectors by word
        val inputVectors = input.join(wordVectors)  // (ngram, (key, vector))
            .map(nkv => (nkv._2._1, nkv._2._2))     // (key, vector)
            .aggregateByKey((0, DenseVector.zeros[Double](300)))(
                (acc, value) => (acc._1 + 1, acc._2 + value),
                (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
            .mapValues(countVec => 
                (1.0D / countVec._1) * countVec._2) // (key, mean(vector))
            
        // save document (id, vector) pair as flat file
        inputVectors.map(kvec => {
            val key = kvec._1
            val value = (kvec._2 / norm(kvec._2, 2))
                .toArray
                .map("%.5f".format(_))
                .mkString(",")
            "%s\t%s".format(key, value)
        }).saveAsTextFile(outputDir)
    }
        
    def parseLine(line: String): (String, String) = {
        val cols = line.split("\t")
        val key = cols.head
        val text = cols.last
        (key, text)
    }
    
    def extractNGrams(p: Iterator[(String, String)]): 
            Iterator[(String, String)] = {
        val t2n = new NGramExtractor()
        p.flatMap(keyText => t2n.ngrams(keyText))
    }
}
