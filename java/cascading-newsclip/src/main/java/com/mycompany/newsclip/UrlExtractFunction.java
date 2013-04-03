package com.mycompany.newsclip;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Function to take a keyword and use Google's custom search
 * service to retrieve the top 10 URLs.
 */
@SuppressWarnings("rawtypes")
public class UrlExtractFunction extends BaseOperation implements Function {

  private static final long serialVersionUID = 1622228905563317614L;
  private static final Logger LOGGER = LoggerFactory.getLogger(UrlExtractFunction.class);
  
  private static final String CUSTOM_SEARCH_URL_TEMPLATE =
    "https://www.googleapis.com/customsearch/v1?key={KEY}&cx={CX}&q={Q}";
  private String key;
  private String cx;
  private ObjectMapper objectMapper;
  
  public UrlExtractFunction(Fields fields) {
    super(2, fields);
    Properties props = new Properties();
    try {
      props.load(new FileInputStream("src/main/resources/google.lic"));
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    key = props.getProperty("key");
    cx = props.getProperty("cx");
    objectMapper = new ObjectMapper();
  }
  
  @Override
  public void operate(FlowProcess flowProcess, FunctionCall funCall) {
    TupleEntry args = funCall.getArguments();
    String keyword = args.getString(0);
    List<String> urls = parseSearchResult(keyword);
    for (String url : urls) {
      Tuple t = new Tuple();
      t.add(url);
      funCall.getOutputCollector().add(t);
    }
  }

  protected List<String> parseSearchResult(String keyword) {
    try {
      String url = CUSTOM_SEARCH_URL_TEMPLATE.
        replaceAll("{KEY}", key).
        replaceAll("{CX}", cx).
        replaceAll("{Q}", URLEncoder.encode(keyword, "UTF-8"));
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
      return parseJson(buf.toString());
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Collections.emptyList();
    }
  }

  protected List<String> parseJson(String json) throws Exception {
    List<String> urls = new ArrayList<String>();
    JsonParser parser = objectMapper.getJsonFactory().
      createJsonParser(json);
    JsonNode root = objectMapper.readTree(parser);
    ArrayNode items = (ArrayNode) root.get("items");
    for (JsonNode item : items) {
      urls.add(item.get("link").getTextValue());
    }
    return urls;
  }
}
