package com.mycompany.kevinbacon.flow;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.operation.Insert;
import cascading.operation.aggregator.Min;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Retain;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.LocalPlatform;
import cascading.test.PlatformRunner.Platform;
import cascading.tuple.Fields;

@Platform(LocalPlatform.class)
public class MyCascadingOpsTest extends PlatformTestCase {

  private static final long serialVersionUID = -6316742429951031415L;

  @Before
  public void setup() throws Exception {
    File output = new File("src/test/data/output");
    FileUtils.deleteDirectory(output);
  }
  
  @Test
  public void testMerge() throws Exception {
    String mergeLeftFilename = "src/test/data/merge_left.csv";
    String mergeRightFilename = "src/test/data/merge_right.csv";
    String outputPath = "src/test/data/output/merge_result.csv";
    
    getPlatform().copyFromLocal(mergeLeftFilename);
    getPlatform().copyFromLocal(mergeRightFilename);
    
    Fields actorField = new Fields("actor");
    Tap<?, ?, ?> tin1 = getPlatform().getTextFile(actorField, mergeLeftFilename);
    Tap<?, ?, ?> tin2 = getPlatform().getTextFile(actorField, mergeRightFilename);
    Tap<?, ?, ?> tout = getPlatform().getTextFile(actorField, outputPath, SinkMode.REPLACE);

    Pipe left = new Pipe("left");
    Pipe right = new Pipe("right");
    Pipe merged = new Merge(new Pipe[] {left, right});
    merged = new CountBy(merged, actorField, new Fields("count"));
    merged = new Each(merged, new ExpressionFilter("count > 1", Integer.class));
    merged = new Retain(merged, actorField);

    FlowDef flowDef = FlowDef.flowDef().
      addSource(left, tin1).
      addSource(right, tin2).
      addTailSink(merged, tout);
    Flow<?> flow = getPlatform().getFlowConnector().connect(flowDef);
    flow.complete();
    validateLength(flow, 3);
  }

  @Test
  public void testInsert() throws Exception {
    String mergeLeftFilename = "src/test/data/merge_left.csv";
    String outputPath = "src/test/data/output/insert_result.csv";
    
    getPlatform().copyFromLocal(mergeLeftFilename);
    
    Fields actorField = new Fields("actor");
    Tap<?, ?, ?> tin = getPlatform().getTextFile(actorField, mergeLeftFilename);
    Tap<?, ?, ?> tout = getPlatform().getTextFile(actorField, outputPath, SinkMode.REPLACE);

    Pipe pipe = new Pipe("insert");
    Fields kbnumField = new Fields("kbnum");
    Insert insfun = new Insert(kbnumField, 1);
    pipe = new Each(pipe, insfun, Fields.ALL);

    FlowDef flowDef = FlowDef.flowDef().
      addSource(pipe, tin).
      addTailSink(pipe, tout);
    Flow<?> flow = getPlatform().getFlowConnector().connect(flowDef);
    flow.complete();
    validateLength(flow, 3);
  }

  @Test
  public void testMergeGroupMin() throws Exception {
    
    String[] inputPaths = new String[] {
      "src/test/data/merge_min_1.csv",
      "src/test/data/merge_min_2.csv",
      "src/test/data/merge_min_3.csv"};
    String detailOutputPath = 
      "src/test/data/output/mergemin_detail.csv";

    Fields actorKbnumFields = new Fields("actor", "kbnum");
    Fields actorField = new Fields("actor");
    Fields kbnumField = new Fields("kbnum");

    getPlatform().copyFromLocal(inputPaths[0]);
    getPlatform().copyFromLocal(inputPaths[1]);
    getPlatform().copyFromLocal(inputPaths[2]);

    Tap<?, ?, ?> tin1 = getPlatform().getDelimitedFile(
      actorKbnumFields, "\t", inputPaths[0]);
    Tap<?, ?, ?> tin2 = getPlatform().getDelimitedFile(
      actorKbnumFields, "\t", inputPaths[1]);
    Tap<?, ?, ?> tin3 = getPlatform().getDelimitedFile(
      actorKbnumFields, "\t", inputPaths[2]);
    Tap<?, ?, ?> tdout = getPlatform().getTextFile(actorKbnumFields, 
      detailOutputPath, SinkMode.REPLACE);
    
    Pipe[] inpipes = new Pipe[3];
    inpipes[0] = new Pipe("pipe0");
    inpipes[1] = new Pipe("pipe1");
    inpipes[2] = new Pipe("pipe2");
    
    Pipe merged = new Merge(inpipes);
    merged = new GroupBy(inpipes, actorField);
    merged = new Every(merged, kbnumField, new Min());
    
    FlowDef flowDef = FlowDef.flowDef().
      addSource(inpipes[0], tin1).
      addSource(inpipes[1], tin2).
      addSource(inpipes[2], tin3).
      addTailSink(merged, tdout);

    Flow<?> flow = getPlatform().getFlowConnector().connect(flowDef);
    flow.complete();
    validateLength(flow, 4);
  }
  
  @Test
  public void testCounts() throws Exception {
    String inputPath = "src/test/data/summary_input.csv";
    String outputPath = "src/test/data/output/summary_output.csv";
    
    Fields actorKbnumFields = new Fields("actor", "kbnum");
    Fields kbnumField = new Fields("kbnum");
    Fields countField = new Fields("count");
    Fields kbCountFields = new Fields("kbnum", "count");

    getPlatform().copyFromLocal(inputPath);

    Tap<?, ?, ?> tin = getPlatform().getDelimitedFile(
      actorKbnumFields, "\t", inputPath);
    Tap<?, ?, ?> tout = getPlatform().getDelimitedFile(
      kbCountFields, outputPath, SinkMode.REPLACE);
      
    Pipe pin = new Pipe("input");
    Pipe pout = new Retain(pin, kbnumField);
    pout = new CountBy(pout, kbnumField, countField);
    
    FlowDef flowDef = FlowDef.flowDef().
      addSource(pin, tin).
      addTailSink(pout, tout);

    Flow<?> flow = getPlatform().getFlowConnector().connect(flowDef);
    flow.complete();
    validateLength(flow, 3);
  }
}