package com.mycompany.hiaex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
 * HIA 5.4.5 - Product of Vector with Matrix. Both Matrix and
 * Vector are specified using sparse representation.
 * Uses map-side join using DistributedCache (to hold the Vector). 
 */
public class MatrixMultiplyVector extends Configured implements Tool {
  
  public static class Mapper1 extends 
      Mapper<LongWritable,Text,LongWritable,DoubleWritable> {
    
    private Map<Long,Double> vector = new HashMap<Long,Double>();
    
    @Override
    protected void setup(Context ctx) 
        throws IOException, InterruptedException {
      Path[] cachedFiles = DistributedCache.getLocalCacheFiles(ctx.getConfiguration());
      if (cachedFiles != null && cachedFiles.length > 0) {
        BufferedReader reader = new BufferedReader(new FileReader(cachedFiles[0].toString()));
        String line = null;
        try {
          while ((line = reader.readLine()) != null) {
            String[] cols = StringUtils.getStrings(line);
            vector.put(Long.valueOf(cols[0]), Double.valueOf(cols[1]));
          }
        } finally {
          reader.close();
        }
      }
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context ctx)
        throws IOException, InterruptedException {
      String[] cols = StringUtils.getStrings(value.toString());
      Long row = Long.valueOf(cols[0]);
      Long col = Long.valueOf(cols[1]);
      Double entry = Double.valueOf(cols[2]);
      if (vector.containsKey(col)) {
        // multiply matrix and vector entry with matching col value
        entry = entry * vector.get(col);
        ctx.write(new LongWritable(row), new DoubleWritable(entry));
      }
    }
  }
  
  public static class Reducer1 extends
      Reducer<LongWritable,DoubleWritable,LongWritable,DoubleWritable> {
    
    @Override
    protected void reduce(LongWritable key, Iterable<DoubleWritable> values, 
        Context ctx) throws IOException, InterruptedException {
      double sum = 0.0D;
      // then sum up all entry values for the given row
      for (DoubleWritable value : values) {
        sum = sum + value.get();
      }
      ctx.write(key, new DoubleWritable(sum));
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf, "matrix-multiply-vector");
    // See Amareshwari Sri Ramadasu's comment in this thread...
    // http://lucene.472066.n3.nabble.com/Distributed-Cache-with-New-API-td722187.html
    // you need to do job.getConfiguration() instead of conf.
    DistributedCache.addCacheFile(new Path(args[1]).toUri(), 
      job.getConfiguration());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setJarByClass(MatrixMultiplyVector.class);
    job.setMapperClass(Mapper1.class);
    job.setReducerClass(Reducer1.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    boolean succ = job.waitForCompletion(true);
    if (! succ) {
      System.out.println("Job failed, exiting");
      return -1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("Usage: VectorMatrixMultiply matrix vector result");
      System.exit(-1);
    }
    int res = ToolRunner.run(new Configuration(), new MatrixMultiplyVector(), args);
    System.exit(res);
  }
}
