name := "dmozcat"

organization := "com.mycompany"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    // spark
    "org.apache.spark" % "spark-core_2.10" % "1.4.1" % "provided",
    "org.apache.spark" % "spark-mllib_2.10" % "1.4.1" % "provided",
    // nlptools+opennlp
    "edu.washington.cs.knowitall.nlptools" % "nlptools-core_2.10" % "2.4.5",
    "edu.washington.cs.knowitall.nlptools" % "nlptools-sentence-opennlp_2.10" % "2.4.5",
    "edu.washington.cs.knowitall.nlptools" % "nlptools-postag-opennlp_2.10" % "2.4.5",
    "edu.washington.cs.knowitall.nlptools" % "nlptools-tokenize-opennlp_2.10" % "2.4.5",
    "edu.washington.cs.knowitall.nlptools" % "nlptools-chunk-opennlp_2.10" % "2.4.5",
    // boilerpipe
    "de.l3s.boilerpipe" % "boilerpipe" % "1.1.0",
    // utilities
    "com.google.guava" % "guava" % "19.0-rc1",
    // junit
    "com.novocode" % "junit-interface" % "0.8" % "test"
)
