package com.mycompany.pigudfs;

import java.util.Arrays;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for the JaccardSimilarity Pig UDF.
 */
public class JaccardSimilarityTest {

  @Test
  public void testEval() throws Exception {
    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();
    JaccardSimilarity jsim = new JaccardSimilarity();
    // ({(2),(3)}, {(1),(4)})
    Tuple t1 = tupleFactory.newTuple(Arrays.asList(2));
    Tuple t2 = tupleFactory.newTuple(Arrays.asList(3));
    DataBag b1 = bagFactory.newDefaultBag(Arrays.asList(t1, t2));
    Tuple t3 = tupleFactory.newTuple(Arrays.asList(1));
    Tuple t4 = tupleFactory.newTuple(Arrays.asList(4));
    DataBag b2 = bagFactory.newDefaultBag(Arrays.asList(t3, t4));
    Tuple t = tupleFactory.newTuple(Arrays.asList(b1, b2));
    Assert.assertEquals(0.0D, jsim.exec(t));
    // ({(2),(3)}, {(2),(3)})
    t1 = tupleFactory.newTuple(Arrays.asList(2));
    t2 = tupleFactory.newTuple(Arrays.asList(3));
    b1 = bagFactory.newDefaultBag(Arrays.asList(t1, t2));
    t3 = tupleFactory.newTuple(Arrays.asList(2));
    t4 = tupleFactory.newTuple(Arrays.asList(3));
    b2 = bagFactory.newDefaultBag(Arrays.asList(t3, t4));
    t = tupleFactory.newTuple(Arrays.asList(b1, b2));
    Assert.assertEquals(1.0D, jsim.exec(t));
    // ({(2),(3)}, {(2),(4),(5)})
    t1 = tupleFactory.newTuple(Arrays.asList(2));
    t2 = tupleFactory.newTuple(Arrays.asList(3));
    b1 = bagFactory.newDefaultBag(Arrays.asList(t1, t2));
    t3 = tupleFactory.newTuple(Arrays.asList(2));
    t4 = tupleFactory.newTuple(Arrays.asList(4));
    Tuple t5 = tupleFactory.newTuple(Arrays.asList(5));
    b2 = bagFactory.newDefaultBag(Arrays.asList(t3, t4, t5));
    t = tupleFactory.newTuple(Arrays.asList(b1, b2));
    Assert.assertEquals(0.25D, jsim.exec(t));
  }
}
