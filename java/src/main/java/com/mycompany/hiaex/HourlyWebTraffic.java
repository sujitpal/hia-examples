// $Id$
// $Source$
package com.mycompany.hiaex;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * HIA 4.7.2 - Hourly Web Traffic from Tomcat logs.
 * @author Sujit Pal (spal@healthline.com)
 * @version $Revision$
 */
public class HourlyWebTraffic extends Configured implements Tool {

  public static class Mapper1 extends 
      Mapper<LongWritable,Text,IntWritable,LongWritable> {

    // Jun 16, 2012 5:20:54 PM ...
    private static final Pattern DATE_PATTERN = Pattern.compile(
      "^(\\w{3}\\s\\d{1,2},\\s\\d{4}\\s\\d{2}:\\d{2}:\\d{2}\\s[AP]M).*$");
    private static final SimpleDateFormat DATE_FORMATTER =
      new SimpleDateFormat("MMM dd, yyyy HH:mm:ss a");
    private static final LongWritable ONE = new LongWritable(1L);
    
    @Override
    protected void map(LongWritable key, Text value, Context ctx) 
        throws IOException, InterruptedException {
      Matcher m = DATE_PATTERN.matcher(value.toString());
      if (m.matches()) {
        try {
          Date d = DATE_FORMATTER.parse(m.group(1));
          Calendar cal = Calendar.getInstance();
          cal.setTime(d);
          ctx.write(new IntWritable(cal.get(Calendar.HOUR_OF_DAY) - 1), ONE);
        } catch (ParseException e) {
          // log the error and ignore the line
          System.out.println("Can't parse:" + value.toString());
        }
      }
    }
  }
  
  public static class Reducer1 extends
      Reducer<IntWritable,LongWritable,IntWritable,LongWritable> {
    
    private static long[] TRAFFIC = new long[24];
    
    @Override
    protected void reduce(IntWritable key, 
        Iterable<LongWritable> values, Context ctx) 
        throws IOException, InterruptedException {
      long sum = 0L;
      for (LongWritable value : values) {
        sum = sum + value.get();
      }
      TRAFFIC[key.get()] = sum;
    }
    
    @Override
    protected void cleanup(Context ctx) 
        throws IOException, InterruptedException {
      for (int i = 0; i < TRAFFIC.length; i++) {
        ctx.write(new IntWritable(i), new LongWritable(TRAFFIC[i]));
      }
    }
  }
  
  public int run(String[] args) throws Exception {
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    Configuration conf = getConf();
    Job job = new Job(conf, "hourly-web-traffic");
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    job.setJarByClass(HourlyWebTraffic.class);
    job.setMapperClass(Mapper1.class);
    job.setReducerClass(Reducer1.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    boolean succ = job.waitForCompletion(true);
    if (! succ) {
      System.out.println("Job failed, exiting");
      System.exit(-1);
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: HourlyWebTraffic /path/to/log /path/to/output");
      System.exit(-1);
    }
    int res = ToolRunner.run(new Configuration(), new HourlyWebTraffic(), args);
    System.exit(res);
  }
}
