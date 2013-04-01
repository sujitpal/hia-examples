package com.mycompany.hiaex;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * MRUnit test (doesn't work).
 */
public class DotProductTest {
  
  private MapDriver<LongWritable,Text,IntWritable,DoubleWritable> map1Driver;
  private ReduceDriver<IntWritable,DoubleWritable,Text,DoubleWritable> reduce1Driver;
  private MapReduceDriver<LongWritable,Text,IntWritable,DoubleWritable,Text,DoubleWritable> mapreduce1Driver;
  
  @Before
  public void setUp() {
    DotProduct.Mapper1 mapper1 = new DotProduct.Mapper1();
    DotProduct.Reducer1 reducer1 = new DotProduct.Reducer1();
    map1Driver = new MapDriver<LongWritable,Text,IntWritable,DoubleWritable>();
    map1Driver.setMapper(mapper1);
    reduce1Driver = new ReduceDriver<IntWritable,DoubleWritable,Text,DoubleWritable>();
    reduce1Driver.setReducer(reducer1);
    mapreduce1Driver = new MapReduceDriver<LongWritable,Text,IntWritable,DoubleWritable,Text,DoubleWritable>();
    mapreduce1Driver.setMapper(mapper1);
    mapreduce1Driver.setReducer(reducer1);
  }
  
  @Test
  public void testMapper1() {
    map1Driver.withInput(new LongWritable(), new Text("1,690.23"));
    map1Driver.withOutput(new IntWritable(1), new DoubleWritable(690.23));
    map1Driver.runTest();
    Assert.assertTrue(true);
  }
  
  @Test
  public void testReducer1() {
    List<DoubleWritable> values = new ArrayList<DoubleWritable>();
    values.add(new DoubleWritable(690.23));
    values.add(new DoubleWritable(200.0));
    reduce1Driver.withInput(new IntWritable(1), values);
    reduce1Driver.withOutput(new Text("PROD"), new DoubleWritable(138046.0));
    reduce1Driver.runTest();
    Assert.assertTrue(true);
  }
}
