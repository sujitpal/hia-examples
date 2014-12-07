/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.spark

// Java
import java.io.{FileWriter, File}
import scala.io.Source
import com.google.common.io.Files
import org.junit.Test
import org.junit.Assert

class WordCountTest {

  @Test
  def testCountWordsCorrectly() {
    val tempDir = Files.createTempDir()
    val inputFile = new File(tempDir, "input").getAbsolutePath()
    val inWriter = new FileWriter(inputFile)
    inWriter.write("hack hack hack and hack")
    inWriter.close()
    val outputDir = new File(tempDir, "output").getAbsolutePath()
    WordCount.execute(
      master = "local", 
      args = List(inputFile, outputDir)
    )
    val outputFile = new File(outputDir, "part-00000")
    val actual = Source.fromFile(outputFile).mkString
    Assert.assertEquals(actual, "(hack,4)\n(and,1)\n")
  }
}
