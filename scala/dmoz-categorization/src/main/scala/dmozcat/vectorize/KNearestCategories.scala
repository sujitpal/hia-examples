package dmozcat.vectorize

import scala.Array.canBuildFrom
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import breeze.linalg.DenseVector
import breeze.linalg.norm
import breeze.linalg.DenseMatrix

object KNearestCategories {
    
    def main(args: Array[String]): Unit = {
        
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)
        
        // arguments
        val awsAccessKey = args(0)
        val awsSecretKey = args(1)
        val refVectorsDir = args(2)
        val testVectorsDir = args(3)
        val bestcatsFile = args(4)
        
        val conf = new SparkConf()
        conf.setAppName("KNearestCategories")
        
        val sc = new SparkContext(conf)
        
        sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", awsAccessKey)
        sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", awsSecretKey)
    
        // read reference vectors and broadcast them to workers for
        // replicated join
        val refVectors = sc.textFile(refVectorsDir)
            .map(line => {
                val Array(catStr, vecStr) = line.split("\t")
                val cat = catStr.split("/").last
                val vec = new DenseVector(vecStr.split(",").map(_.toDouble))
                val l2 = norm(vec, 2.0)
                (cat, vec / l2)
            }).collect
        val categories = refVectors.map(_._1)
        // we want to take the Array[DenseVector[Double]] and convert
        // it to DenseMatrix[Double] so we can do matrix-vector multiplication
        // for computing similarities later
        val nrows = categories.size
        val ncols = refVectors.map(_._2).head.length
        val catVectors = refVectors.map(_._2)
            .reduce((a, b) => DenseVector.vertcat(a, b))
            .toArray
        val catMatrix = new DenseMatrix[Double](ncols, nrows, catVectors).t
        // broadcast it
        val bCategories = sc.broadcast(categories)
        val bCatMatrix = sc.broadcast(catMatrix) 
        
        // read test vectors representing each test document
        val testVectors = sc.textFile(testVectorsDir)
            .map(line => {
                val Array(filename, vecStr) = line.split("\t")
                val vec = DenseVector(vecStr.split(",").map(_.toDouble))
                val l2 = norm(vec, 2.0)
                (filename, vec / l2)
            })
            .map(fileVec => 
                (fileVec._1, topCategories(fileVec._2, 3,
                    bCatMatrix.value, bCategories.value)))
            
        testVectors.map(fileCats => "%s\t%s".format(
                fileCats._1, fileCats._2.mkString(", ")))
            .coalesce(1)
            .saveAsTextFile(bestcatsFile)
    }

    def topCategories(vec: DenseVector[Double], n: Int,
            catMatrix: DenseMatrix[Double],
            categories: Array[String]): List[String] = {
        val cosims = catMatrix * vec
        cosims.toArray.zipWithIndex
            .sortWith((a, b) => a._1 > b._1)
            .take(n)                           // argmax(n)
            .map(simIdx => (categories(simIdx._2), simIdx._1)) // (cat,sim)
            .map(catSim => "%s (%.3f)".format(catSim._1, catSim._2))
            .toList
    }
}
