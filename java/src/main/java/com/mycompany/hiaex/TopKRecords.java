package com.mycompany.hiaex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * HIA 4.7.1 - Output Top cited patents.
 */
public class TopKRecords extends Configured implements Tool {

  private static final int TOP_K = 10;
  
  private static final IntWritable ONE = new IntWritable(1);
  
  public static class Mapper1 extends
      Mapper<LongWritable,Text,Text,IntWritable> {
    
    @Override
    protected void map(LongWritable key, Text value, 
        Context ctx) throws IOException, InterruptedException {
      String[] cols = StringUtils.getStrings(value.toString());
      ctx.write(new Text(cols[1]), ONE);
    }
  }
  
  public static class Reducer1 extends
      Reducer<Text,IntWritable,Text,IntWritable> {
    
    private class Pair {
      public String str;
      public Integer count;
      
      public Pair(String str, Integer count) {
        this.str = str;
        this.count = count;
      }
    };
    private PriorityQueue<Pair> queue;
    
    @Override
    protected void setup(Context ctx) {
      queue = new PriorityQueue<Pair>(TOP_K, new Comparator<Pair>() {
        public int compare(Pair p1, Pair p2) {
          return p1.count.compareTo(p2.count);
        }
      });
    }
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, 
        Context ctx) throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable value : values) {
        count = count + value.get();
      }
      queue.add(new Pair(key.toString(), count));
      if (queue.size() > TOP_K) {
        queue.remove();
      }
    }
    
    @Override
    protected void cleanup(Context ctx) 
        throws IOException, InterruptedException {
      List<Pair> topKPairs = new ArrayList<Pair>();
      while (! queue.isEmpty()) {
        topKPairs.add(queue.remove());
      }
      for (int i = topKPairs.size() - 1; i >= 0; i--) {
        Pair topKPair = topKPairs.get(i);
        ctx.write(new Text(topKPair.str), 
          new IntWritable(topKPair.count));
      }
    }
  }
  
  public static class Mapper2 extends 
      Mapper<Text,Text,Text,IntWritable> {
  
    @Override
    protected void map(Text key, Text value, Context ctx)
        throws IOException, InterruptedException {
      ctx.write(key, new IntWritable(Integer.valueOf(value.toString())));
    }
  }
  
  public int run(String[] args) throws Exception {
    Path input = new Path(args[0]);
    Path temp1 = new Path("temp");
    Path output = new Path(args[1]);
    Configuration conf = getConf();
    
    Job job1 = new Job(conf, "top-k-pass-1");
    FileInputFormat.addInputPath(job1, input);
    FileOutputFormat.setOutputPath(job1, temp1);
    job1.setJarByClass(TopKRecords.class);
    job1.setMapperClass(Mapper1.class);
    job1.setCombinerClass(Reducer1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    boolean succ = job1.waitForCompletion(true);
    if (! succ) {
      System.out.println("Job1 failed, exiting");
      return -1;
    }
    
    Job job2 = new Job(conf, "top-k-pass-2");
    FileInputFormat.setInputPaths(job2, temp1);
    FileOutputFormat.setOutputPath(job2, output);
    job2.setJarByClass(DotProduct.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer1.class);
    job2.setInputFormatClass(KeyValueTextInputFormat.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setNumReduceTasks(1);
    succ = job2.waitForCompletion(true);
    if (! succ) {
      System.out.println("Job2 failed, exiting");
      return -1;
    }
    
    return 0;

  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: TopKRecords /path/to/citation.txt output_dir");
      System.exit(-1);
    }
    int res = ToolRunner.run(new Configuration(), new TopKRecords(), args);
    System.exit(res);
  }
}
