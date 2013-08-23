package com.mycompany.kevinbacon.flow;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.LocalPlatform;
import cascading.test.PlatformRunner.Platform;

@Platform(LocalPlatform.class)
public class FindCostarsTest extends PlatformTestCase {
  
  private static final long serialVersionUID = 8950872097793074273L;

  @Before
  public void setup() throws Exception {
    File output = new File("src/test/data/output");
    FileUtils.deleteDirectory(output);
  }
  
  @Test
  public void testFindCostars() throws Exception {
    String allPairsFilename = "src/test/data/find_costars_allpairs.csv";
    String actorsFilename = "src/test/data/find_costars_actors.csv";
    String costarFilename = "src/test/data/output/costars.csv";
    
    getPlatform().copyFromLocal(allPairsFilename);
    getPlatform().copyFromLocal(actorsFilename);
    
    Tap<?,?,?> tapAllPairs = getPlatform().getDelimitedFile(
      Constants.inputFields, "\t", allPairsFilename, SinkMode.KEEP);
    Tap<?,?,?> tapActors = getPlatform().getDelimitedFile(
      Constants.detailFields, "\t", actorsFilename, SinkMode.KEEP);
    Tap<?,?,?> tapCostars = getPlatform().getDelimitedFile(
      Constants.detailFields, "\t", costarFilename, SinkMode.REPLACE);
    
    Pipe allPairs = new Pipe("allPairs");
    Pipe actors = new Pipe("actors");
    Pipe costars = new FindCostars(allPairs, actors, 2);
    
    FlowDef flowDef = FlowDef.flowDef().
      addSource(allPairs, tapAllPairs).
      addSource(actors, tapActors).
      addTailSink(costars, tapCostars);
    
    Flow<?> flow = getPlatform().getFlowConnector().connect(flowDef);
    flow.complete();
    validateLength(flow, 7);
  }
}