package dmozcat.preprocessing

import java.io.File
import scala.io.Source
import java.io.PrintWriter
import java.io.FileWriter

object TestDataReformatter extends App {
    
    val inputDir = new File("/Users/palsujit/Projects/med_data/mtcrawler/texts")
    val outputFile = new PrintWriter(new FileWriter(new File("/tmp/testdata.csv")), true)
    inputDir.listFiles().foreach(inputFile => {
        val filename = inputFile.getName()
        val text = Source.fromFile(inputFile)
            .getLines
            .map(line => if (line.endsWith(".")) line else line + ".")
            .mkString(" ")
            .replaceAll("\\s+", " ")
        outputFile.println("%s\t%s".format(filename, text))
    })
    outputFile.flush()
    outputFile.close()

}
