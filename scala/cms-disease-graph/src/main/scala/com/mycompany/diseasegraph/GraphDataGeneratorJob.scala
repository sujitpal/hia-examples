package com.mycompany.diseasegraph

import org.apache.spark.SparkContext

object GraphDataGeneratorJob extends App {

  GraphDataGenerator.execute(
    master = sys.env("MASTER"), 
    args = args.toList,
    jars = SparkContext.jarOfObject(this).toSeq
  )
  System.exit(0)
  
}