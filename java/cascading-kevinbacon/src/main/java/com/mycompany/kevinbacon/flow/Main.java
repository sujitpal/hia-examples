package com.mycompany.kevinbacon.flow;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Filter;
import cascading.operation.Identity;
import cascading.operation.aggregator.Min;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.filter.Not;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.Unique;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class Main {
  
  public static void main(String[] args) {

    String input = args[0];
    String detailOutput = args[1];
    String summaryOutput = args[2];
    
    Tap<?,?,?> tin = new Hfs(new TextDelimited(
      Constants.inputFields, false, false, "\t"), input);
    Tap<?,?,?> toutDetail = new Hfs(new TextDelimited(
      Constants.detailFields, false, false, "\t"), detailOutput);
    Tap<?,?,?> toutSummary = new Hfs(new TextDelimited(
      Constants.summaryFields, false, false, "\t"), summaryOutput);
    
    Pipe allPairs = new Pipe("allPairs");
    
    // create a pipe with only Kevin Bacon
    Pipe kevinBacon = new Pipe("kevinBacon", allPairs);
    Filter<?> kevinBaconFilter = new ExpressionFilter(
      "! actor.equals(\"Bacon, Kevin\")", String.class);
    kevinBacon = new Each(kevinBacon, kevinBaconFilter);
    kevinBacon = new Retain(kevinBacon, Constants.actorField);
    kevinBacon = new Unique(kevinBacon, Constants.actorField);
    
    // At each degree of separation, find the costars of 
    // actors in the actor pipe (second arg to FindCostars)
    // by joining on actor to find movies, then joining on
    // movie to find costars.
    Pipe kevinBaconCostars0 = new FindCostars(
      allPairs, kevinBacon, 0);
      
    Pipe kevinBaconCostars1 = new FindCostars(
      allPairs, kevinBaconCostars0, 1);
      
    Pipe kevinBaconCostars2 = new FindCostars(
      allPairs, kevinBaconCostars1, 2);
      
    Pipe kevinBaconCostars3 = new FindCostars(
      allPairs, kevinBaconCostars2, 3);
      
    Pipe kevinBaconCostars4 = new FindCostars(
      allPairs, kevinBaconCostars3, 4);
      
    Pipe kevinBaconCostars5 = new FindCostars(
      allPairs, kevinBaconCostars4, 5);
      
    Pipe kevinBaconCostars6 = new FindCostars(
      allPairs, kevinBaconCostars5, 6);

    // merge pipes together, then filter out Kevin Bacon, 
    // group by actors and choose the minimum Bacon number
    // for each actor, and finally rename the min column to
    // count.
    Pipe merged = new Merge("merged", Pipe.pipes(
      kevinBaconCostars0, kevinBaconCostars1, kevinBaconCostars3,
      kevinBaconCostars4, kevinBaconCostars5, kevinBaconCostars6));
    merged = new Each(merged, new Not(kevinBaconFilter));
    merged = new GroupBy(merged, Constants.actorField);
    merged = new Every(merged, Constants.kbnumField, new Min());
    merged = new Rename(merged, new Fields("min"), Constants.kbnumField);
    
    // split the merged pipe into detail and summary pipes.
    // This is needed to avoid "duplicate pipe" errors from
    // Cascading when trying to set two tail sinks. The 
    // merged pipe already contains the information needed
    // for the detail pipe, and the summary pipe needs a bit
    // more processing, detailed in the comment block below.
    Pipe details = new Pipe("details", merged);
    details = new Each(details, new Identity());
    
    // generate summary stats - retain only the bacon number
    // column and group by it and count the number of costars
    // in each bacon number group.
    Pipe summary = new Pipe("summary", merged);
    summary = new Retain(summary, Constants.kbnumField);
    summary = new CountBy(summary, 
      Constants.kbnumField, Constants.countField);
    
    FlowDef fd = FlowDef.flowDef().
      addSource(allPairs, tin).
      addTailSink(details, toutDetail).
      addTailSink(summary, toutSummary);

    Properties props = new Properties();
    AppProps.setApplicationJarClass(props, Main.class);
    FlowConnector fc = new HadoopFlowConnector(props);
    
    Flow<?> flow = fc.connect(fd);
    flow.writeDOT("data/kevinbacon.dot");
    flow.writeStepsDOT("data/kevinbacon-steps.dot");
    flow.complete();
  }
}

