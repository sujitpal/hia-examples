package com.mycompany.pigudfs;

import java.util.Arrays;

import junit.framework.Assert;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

/**
 * Test for OrderByCountDesc Pig UDF.
 */
public class OrderByCountDescTest {

  @Test
  public void testEval() throws Exception {
    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();
    OrderByCountDesc obcd = new OrderByCountDesc();
    // {(300,100)}
    Tuple t1 = tupleFactory.newTuple(Arrays.asList(300, 100));
    DataBag ibag = bagFactory.newDefaultBag(Arrays.asList(t1));
    Tuple input = tupleFactory.newTuple(Arrays.asList(ibag));
    DataBag obag = obcd.exec(input);
    Assert.assertEquals("{(300,100)}", obag.toString());
    // {(200,100),(200,100)}
    t1 = tupleFactory.newTuple(Arrays.asList(200, 100));
    Tuple t2 = tupleFactory.newTuple(Arrays.asList(200, 100));
    ibag = bagFactory.newDefaultBag(Arrays.asList(t1, t2));
    input = tupleFactory.newTuple(Arrays.asList(ibag));
    obag = obcd.exec(input);
    Assert.assertEquals("{(200,100)}", obag.toString());
    // {(200,300),(200,100)}
    t1 = tupleFactory.newTuple(Arrays.asList(200, 300));
    t2 = tupleFactory.newTuple(Arrays.asList(200, 100));
    ibag = bagFactory.newDefaultBag(Arrays.asList(t1, t2));
    input = tupleFactory.newTuple(Arrays.asList(ibag));
    obag = obcd.exec(input);
    Assert.assertTrue("{(200,300),(200,100)}".equals(obag.toString()) ||
      "{(200,100),(200,300)}".equals(obag.toString()));
    // {(100,200),(100,300),(100,200)}
    t1 = tupleFactory.newTuple(Arrays.asList(100, 200));
    t2 = tupleFactory.newTuple(Arrays.asList(100, 300));
    Tuple t3 = tupleFactory.newTuple(Arrays.asList(100, 200));
    ibag = bagFactory.newDefaultBag(Arrays.asList(t1, t2, t3));
    input = tupleFactory.newTuple(Arrays.asList(ibag));
    obag = obcd.exec(input);
    Assert.assertEquals("{(100,200),(100,300)}", obag.toString());
  }
}
