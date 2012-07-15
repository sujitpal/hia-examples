// $Id$
// $Source$
package com.mycompany.hiaex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HIA 5.4.4 - Calculate ratios (of daily stock prices of two
 * companies). Uses repartitioned / reduce-side joining.
 * http://www.google.com/finance/historical?output=csv&q=[Symbol name]
 */
public class StockPriceRatio extends Configured implements Tool {

  public static class Mapper1 extends
      Mapper<LongWritable,Text,Text,Text> {
    
    @Override
    protected void map(LongWritable key, Text value, Context ctx)
        throws IOException, InterruptedException {
      String[] cols = StringUtils.getStrings(value.toString());
      if (! cols[0].startsWith("#")) {
        // date -> symbol,close
        ctx.write(new Text(cols[1]), new Text(StringUtils.join(",", 
          new String[] {cols[0], cols[5]})));
      }
    }
  }
  
  public static class Reducer1 extends
      Reducer<Text,Text,Text,DoubleWritable> {
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
        throws IOException, InterruptedException {
      Double numerator = 0.0D;
      Double denominator = 1.0D;
      for (Text value : values) {
        String[] cols = StringUtils.getStrings(value.toString());
        if ("GOOG".equals(cols[0])) {
          numerator = Double.valueOf(cols[1]);
        } else if ("IBM".equals(cols[0])) {
          denominator = Double.valueOf(cols[1]);
        }
      }
      ctx.write(key, new DoubleWritable(numerator / denominator));
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf, "stock-price-ratio");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setJarByClass(StockPriceRatio.class);
    job.setMapperClass(Mapper1.class);
    job.setReducerClass(Reducer1.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
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
      System.out.println("Usage: StockPriceRatio prices1 prices2 output");
      System.exit(-1);
    }
    int res = ToolRunner.run(new Configuration(), new StockPriceRatio(), args);
    System.exit(res);
  }
}
