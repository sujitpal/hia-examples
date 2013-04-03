package com.mycompany.newsclip;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import cascading.tuple.Fields;

public class KeywordExtractFunctionTest {

  @Test
  public void testDownload() throws Exception {
    Fields kwordFields = new Fields("kword");
    KeywordExtractFunction func = new KeywordExtractFunction(kwordFields);
    String rawText = func.download(
      "http://drdo.gov.in/drdo/English/index.jsp?pg=armaments_tech1.jsp");
//    System.out.println("rawText=" + rawText);
    FileUtils.writeStringToFile(
      new File("/tmp/newsclip_testDownload.txt"), rawText);
    Assert.assertNotNull(rawText);
  }
  
  @Test
  public void testParse() throws Exception {
    Fields kwordFields = new Fields("kword");
    KeywordExtractFunction func = new KeywordExtractFunction(kwordFields);
    String rawText = FileUtils.readFileToString(
      new File("/tmp/newsclip_testDownload.txt"));
    String plainText = func.parse(rawText);
//    System.out.println("plainText=" + plainText);
    FileUtils.writeStringToFile(
      new File("/tmp/newsclip_testParse.txt"), plainText);
    Assert.assertNotNull(plainText);
  }
  
  @Test
  public void testExtract() throws Exception {
    Fields kwordFields = new Fields("kword");
    KeywordExtractFunction func = new KeywordExtractFunction(kwordFields);
    String plainText = FileUtils.readFileToString(
      new File("/tmp/newsclip_testParse.txt"));
    List<String> keywords = func.extractKeywords(plainText);
//    System.out.println("keywords=" + keywords);
    Assert.assertNotNull(keywords);
    Assert.assertTrue(keywords.size() > 0);
  }
}
