package com.mycompany.hiaex;

import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HIA 4.7.4 - Time series.
 */
public class TimeSeries extends Configured implements Tool {

  private static final int N = 5;
  
  public static class Mapper1 extends 
      Mapper<LongWritable,Text,LongWritable,DoubleWritable> {
    
    private Queue<Double> readings = new ArrayBlockingQueue<Double>(N);
    
    @Override
    protected void map(LongWritable key, Text value,
        Context ctx) throws IOException, InterruptedException {
      readings.add(Double.valueOf(value.toString()));
      if (readings.size() == N) {
        for (Iterator<Double> it = readings.iterator(); it.hasNext(); ) {
          Double reading = it.next();
          ctx.write(key, new DoubleWritable(reading));
        }
        readings.remove();
      }
    }
  }
  
  public static class Reducer1 extends
      Reducer<LongWritable,DoubleWritable,NullWritable,DoubleWritable> {
  
    @Override
    protected void reduce(LongWritable key,
        Iterable<DoubleWritable> values, Context ctx) 
        throws IOException, InterruptedException {
      double sum = 0.0D;
      for (DoubleWritable value : values) {
        sum = sum + value.get();
      }
      sum = sum / (double) N;
      ctx.write(null, new DoubleWritable(sum));
    }
  }
  
  public static class Partitioner1 extends Partitioner<LongWritable,DoubleWritable> {

    @Override
    public int getPartition(LongWritable key, DoubleWritable value, int numPartitions) {
      return ((int) key.get()) % numPartitions;
    }
  }
  
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf, "timeseries");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJarByClass(TimeSeries.class);
    job.setMapperClass(Mapper1.class);
    job.setReducerClass(Reducer1.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setPartitionerClass(Partitioner1.class);
    job.setNumReduceTasks(1);
    boolean succ = job.waitForCompletion(true);
    if (! succ) {
      System.out.println("Job failed, exiting");
      return -1;
    }
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: TimeSeries /path/to/input output_dir");
      System.exit(-1);
    }
    int res = ToolRunner.run(new Configuration(), new TimeSeries(), args);
    System.exit(res);
  }

}
