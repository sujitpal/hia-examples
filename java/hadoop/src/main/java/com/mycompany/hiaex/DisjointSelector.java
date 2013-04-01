package com.mycompany.hiaex;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HIA 5.4.3 - Disjoint Selection. Find reviewers who
 * have rated less than 50 movies. 
 * Example shows repartitioned / reduce-side joining.
 */
public class DisjointSelector extends Configured implements Tool {

  private static final int MIN_RATINGS = 25;
  private static final String TAG_USER = "U";
  private static final String TAG_RATING = "R";
  
  public static class Mapper1 extends
      Mapper<LongWritable,Text,IntWritable,Text> {
    
    @Override
    protected void map(LongWritable key, Text value, Context ctx) 
        throws IOException, InterruptedException {
      String[] cols = StringUtils.split(value.toString(), "::");
      String type = (cols.length == 5 ? TAG_USER : TAG_RATING);
      Integer uid = Integer.valueOf(cols[0]);
      ctx.write(new IntWritable(uid), new Text(type));
    }
  }
  
  public static class Reducer1 extends
      Reducer<IntWritable,Text,IntWritable,IntWritable> {
    
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, 
        Context ctx) throws IOException, InterruptedException {
      int nusers = 0;
      int nratings = 0;
      for (Text value : values) {
        if (TAG_USER.equals(value.toString())) {
          nusers = nusers + 1;
        } else {
          nratings = nratings + 1;
        }
      }
      if (nratings < MIN_RATINGS) {
        ctx.write(key, new IntWritable(nratings));
      }
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf, "disjoint-selector");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setJarByClass(DisjointSelector.class);
    job.setMapperClass(Mapper1.class);
    job.setReducerClass(Reducer1.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    boolean succ = job.waitForCompletion(true);
    if (! succ) {
      System.out.println("Job failed, exiting");
      return -1;
    }
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("Usage: DisjointSelector users_file ratings_file output");
      System.exit(-1);
    }
    int res = ToolRunner.run(new Configuration(), new DisjointSelector(), args);
    System.exit(res);
  }
}
