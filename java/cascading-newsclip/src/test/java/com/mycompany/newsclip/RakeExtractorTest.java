package com.mycompany.newsclip;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class RakeExtractorTest {
  
  @Test
  public void testExtractSteps() throws Exception {
    RakeExtractor rake = RakeExtractor.INSTANCE;
    String text = FileUtils.readFileToString(
      new File("/tmp/newsclip_testParse.txt"));
    List<String> sentences = rake.parseSentences(text);
//    System.out.println("sentences=" + sentences);
    Assert.assertNotNull(sentences);
    Assert.assertTrue(sentences.size() > 0);
    List<List<String>> phrases = 
      rake.generateCandidatePhrases(sentences);
//    System.out.println("phrases=" + phrases);
    Assert.assertNotNull(phrases);
    Assert.assertTrue(phrases.size() > 0);
    Map<String,Float> wordScores = rake.calculateWordScores(phrases);
//    System.out.println("wordScores=" + wordScores);
    Assert.assertNotNull(wordScores);
    Assert.assertTrue(wordScores.size() > 0);
    Map<String,Float> phraseScores = 
      rake.calculatePhraseScores(phrases, wordScores);
//    System.out.println("phraseScores=" + phraseScores);
    Assert.assertNotNull(phraseScores);
    Assert.assertTrue(phraseScores.size() > 0);
  }
  
  @Test
  public void testExtract() throws Exception {
    RakeExtractor rake = RakeExtractor.INSTANCE;
    String text = FileUtils.readFileToString(
      new File("/tmp/newsclip_testParse.txt"));
    List<String> keywords = rake.extract(text);
    System.out.println("keywords=" + keywords);
    Assert.assertNotNull(keywords);
    Assert.assertTrue(keywords.size() > 0);
  }
}
