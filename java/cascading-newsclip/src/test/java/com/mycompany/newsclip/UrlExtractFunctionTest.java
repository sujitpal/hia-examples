package com.mycompany.newsclip;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import cascading.tuple.Fields;

public class UrlExtractFunctionTest {

  @Test
  public void testParseJson() throws Exception {
    Fields urlFields = new Fields("url");
    UrlExtractFunction func = new UrlExtractFunction(urlFields);
    List<String> urls = func.parseJson(FileUtils.readFileToString(
      new File("/tmp/result.json")));
    System.out.println("urls=" + urls);
    Assert.assertNotNull(urls);
    Assert.assertTrue(urls.size() > 0);
  }
}
