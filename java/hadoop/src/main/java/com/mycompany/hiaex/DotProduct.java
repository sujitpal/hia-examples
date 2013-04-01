package com.mycompany.hiaex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HIA 4.7.3 - Dot product of two sparse matrices.
 */
public class DotProduct extends Configured implements Tool {

  public static class Mapper1 extends 
      Mapper<LongWritable,Text,IntWritable,DoubleWritable> {
    
    @Override
    protected void map(LongWritable key, Text value, 
          Context ctx) throws IOException, InterruptedException {
      String[] cols = StringUtils.getStrings(value.toString());
      ctx.write(new IntWritable(Integer.valueOf(cols[0])), 
        new DoubleWritable(Double.valueOf(cols[1])));
    }
  }
  
  public static class Reducer1 extends
      Reducer<IntWritable,DoubleWritable,Text,DoubleWritable> {

    private static final Text OUTPUT_KEY = new Text("PROD");
    
    @Override
    protected void reduce(IntWritable key, 
        Iterable<DoubleWritable> values, Context ctx) 
        throws IOException, InterruptedException {
      double prod = 1.0D;
      int nvalues = 0;
      for (DoubleWritable value : values) {
        prod = prod * value.get();
        nvalues++;
      }
      if (nvalues == 2) {
        ctx.write(OUTPUT_KEY, new DoubleWritable(prod));
      }
    }
  }
  
  public static class Mapper2 extends 
      Mapper<Text,Text,Text,DoubleWritable> {

    @Override
    protected void map(Text key, Text value, 
          Context ctx) throws IOException, InterruptedException {
      DoubleWritable dvalue = new DoubleWritable(Double.valueOf(value.toString()));
      ctx.write(key, dvalue);
    }
  }
  
  public static class Reducer2 extends
      Reducer<Text,DoubleWritable,Text,DoubleWritable> {

    private static final Text OUTPUT_KEY = new Text("DOT_PROD");
    
    @Override
    protected void reduce(Text key, 
        Iterable<DoubleWritable> values, Context ctx) 
        throws IOException, InterruptedException {
      double sum = 0.0D;
      for (DoubleWritable value : values) {
        sum = sum + value.get();
      }
      ctx.write(OUTPUT_KEY, new DoubleWritable(sum));
    }
  }
  
  public int run(String[] args) throws Exception {
    Path input1 = new Path(args[0]);
    Path input2 = new Path(args[1]);
    Path temp1 = new Path("temp");
    Path output = new Path(args[2]);
    Configuration conf = getConf();
    
    Job job1 = new Job(conf, "dot-product-multiply");
    FileInputFormat.addInputPath(job1, input1);
    FileInputFormat.addInputPath(job1, input2);
    FileOutputFormat.setOutputPath(job1, temp1);
    job1.setJarByClass(DotProduct.class);
    job1.setMapperClass(Mapper1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setMapOutputKeyClass(IntWritable.class);
    job1.setMapOutputValueClass(DoubleWritable.class);
    boolean succ = job1.waitForCompletion(true);
    if (! succ) {
      System.out.println("Job1 failed, exiting");
      return -1;
    }
    
    Job job2 = new Job(conf, "dot-product-add");
    FileInputFormat.setInputPaths(job2, temp1);
    FileOutputFormat.setOutputPath(job2, output);
    job2.setJarByClass(DotProduct.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setInputFormatClass(KeyValueTextInputFormat.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(DoubleWritable.class);
    job2.setNumReduceTasks(1);
    succ = job2.waitForCompletion(true);
    if (! succ) {
      System.out.println("Job2 failed, exiting");
      return -1;
    }
    
    return 0;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("Usage: DotProduct /path/to/matrix_1 /path/to/matrix_2 output_dir");
      System.exit(-1);
    }
    int res = ToolRunner.run(new Configuration(), new DotProduct(), args);
    System.exit(res);
  }
}
