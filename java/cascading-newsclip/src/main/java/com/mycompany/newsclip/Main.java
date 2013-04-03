package com.mycompany.newsclip;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class Main {

  @SuppressWarnings("rawtypes")
  public static void main(String[] args) {
    // handle input parameters
    String input = args[0];
    String output = args[1];

    Fields urlFields = new Fields("num", "line");
    Tap iTap = new FileTap(new TextLine(urlFields), input);
    
    Fields kFields = new Fields("kword");
    Tap oTap = new FileTap(new TextLine(kFields), output);

    Pipe pipe = new Pipe("keyword");
    
    // read urls, download, clean and extract keywords (1:n)
    Function kFun = new KeywordExtractFunction(kFields);
    pipe = new Each(pipe, urlFields,  kFun);
    
    // group by word and count it
    pipe = new GroupBy(pipe, kFields);
    Aggregator kCount = new Count(new Fields("count"));
    pipe = new Every(pipe, kCount);
    
    // filter out words with count < 1
    Filter kCountFilter = new ExpressionFilter("$1 <= 1", Integer.class);
    pipe = new Each(pipe, kCountFilter);
    
    // pass the keywords to our custom google search
    Fields kcFields = new Fields("kword", "count");
    Fields uFields = new Fields("url");
    Function uFun = new UrlExtractFunction(uFields);
    pipe = new Each(pipe, kcFields, uFun);
    
    // group by url and count it
    pipe = new GroupBy(pipe, uFields);
    Aggregator uCount = new Count(new Fields("count"));
    pipe = new Every(pipe, uCount);
    
    // filter out urls that occur once
    Filter uCountFilter = new ExpressionFilter("$1 <= 1", Integer.class);
    pipe = new Each(pipe, uCountFilter);
    
    // remove the count value
    pipe = new Each(pipe, Fields.ALL, new Identity(), Fields.FIRST);
    
    FlowDef flowDef = FlowDef.flowDef().
      setName("newsclip").
      addSource(pipe, iTap).
      addTailSink(pipe,  oTap);
    
    Properties props = new Properties();
    AppProps.setApplicationJarClass(props, Main.class);
    FlowConnector flowConnector = new LocalFlowConnector(props);

    Flow flow = flowConnector.connect(flowDef);
    flow.writeDOT("data/newsclip.dot");
    flow.complete();
  }
}
