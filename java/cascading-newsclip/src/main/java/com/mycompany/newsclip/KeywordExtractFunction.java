package com.mycompany.newsclip;

import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.DefaultExtractor;

@SuppressWarnings("rawtypes")
public class KeywordExtractFunction extends BaseOperation 
    implements Function {

  private static final long serialVersionUID = -7122434545764806604L;
  private static final Logger LOGGER = LoggerFactory.getLogger(KeywordExtractFunction.class);

  public KeywordExtractFunction(Fields fields) {
    super(2, fields);
  }
  
  @Override
  public void operate(FlowProcess flowProcess, FunctionCall funCall) {
    TupleEntry args = funCall.getArguments();
    String url = args.getString(1);
    String rawText = download(url);
    String plainText = parse(rawText);
    List<String> keywords = extractKeywords(plainText);
    for (String keyword : keywords) {
      Tuple t = new Tuple();
      t.add(keyword);
      funCall.getOutputCollector().add(t);
    }
  }

  protected String download(String url) {
    try {
      URL u = new URL(url);
      u.openConnection();
      InputStream istream = u.openStream();
      StringBuilder buf = new StringBuilder();
      byte[] b = new byte[1024];
      int bytesRead = 0;
      while ((bytesRead = istream.read(b)) > 0) {
        buf.append(new String(b, 0, bytesRead));
        b = new byte[1024];
      }
      istream.close();
      return buf.toString();
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  protected String parse(String rawText) {
    if (StringUtils.isEmpty(rawText)) return null;
    else {
      try {
        return DefaultExtractor.INSTANCE.getText(rawText);
      } catch (BoilerpipeProcessingException e) {
        LOGGER.error(e.getMessage(), e);
        return null;
      }
    }
  }

  protected List<String> extractKeywords(String plainText) {
    try {
      return RakeExtractor.INSTANCE.extract(plainText);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Collections.emptyList();
    }
  }
}
