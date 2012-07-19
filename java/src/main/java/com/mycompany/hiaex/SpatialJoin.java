package com.mycompany.hiaex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
 * HIA 5.4.6 - Spatial Join
 * Find points within Euclidean distance 1 of each other in
 * a 2-D space where (x,y) is (-1B..1B, -1B..1B). The approach
 * we use is to divide the space into squares of 100x100 and
 * do semi-join (map-side filtering with reduce-side join).
 * TODO: class level javadocs
 * @author Sujit Pal (spal@healthline.com)
 * @version $Revision$
 */
public class SpatialJoin extends Configured implements Tool {

  private static final double DIST_CUTOFF_SQUARED = 1.0D;
  
  private static class Point {
    
    public Double x;
    public Double y;
    
    public Point(Double x, Double y) {
      this.x = x;
      this.y = y;
    }
  }
  
  private static class PointWritable 
      implements WritableComparable<PointWritable> {

    private Point p = new Point(0.0D, 0.0D);

    public PointWritable() {} // Needed by Hadoop
    
    public PointWritable(Point p) {
      this.p = p;
    }

    public Point getPoint() { return p; } // for debugging
    @Override
    public void readFields(DataInput in) throws IOException {
      p.x = in.readDouble();
      p.y = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeDouble(p.x);
      out.writeDouble(p.y);
    }

    @Override
    public int compareTo(PointWritable that) {
      if (this.p.x.equals(that.p.x)) {
        return this.p.y.compareTo(that.p.y);
      } else {
        return this.p.x.compareTo(that.p.x);
      }
    }
  }

  public static class Mapper1 extends
      Mapper<LongWritable,Text,PointWritable,Text> {
    
    @Override
    protected void map(LongWritable key, Text value, Context ctx)
        throws IOException, InterruptedException {
      String[] cols = StringUtils.getStrings(value.toString());
      // cols[0] == FOO/BAR
      Double x = Math.floor(Double.valueOf(cols[1]));
      Double y = Math.floor(Double.valueOf(cols[2]));
      if ("FOO".equals(cols[0])) {
        ctx.write(new PointWritable(new Point(x, y)), value);
      } else {
        // the cell enclosing the point plus its immediate 
        // neighbors
        for (double i = x - 1; i <= x + 1; i++) {
          for (double j = y - 1; j <= y + 1; j++) {
            ctx.write(new PointWritable(new Point(i, j)), value);
          }
        }
      }
    }
  }
  
  public static class Reducer1 extends
      Reducer<PointWritable,Text,NullWritable,Text> {
    
    private List<Point> foos;
    private List<Point> bars;
    
    @Override
    protected void setup(Context ctx) 
        throws IOException, InterruptedException {
      foos = new ArrayList<Point>();
      bars = new ArrayList<Point>();
    }

    @Override
    protected void reduce(PointWritable key, Iterable<Text> values, 
        Context ctx) throws IOException, InterruptedException {
      for (Text value : values) {
        // partition foo and bar
        String[] cols = StringUtils.getStrings(value.toString());
        if ("FOO".equals(cols[0])) {
          foos.add(new Point(
            Double.valueOf(cols[1]), Double.valueOf(cols[2])));
        } else {
          bars.add(new Point(
            Double.valueOf(cols[1]), Double.valueOf(cols[2])));
        }
      }
      for (Point foo : foos) {
        for (Point bar : bars) {
          double distSq = Math.pow(foo.x - bar.x, 2) + Math.pow(foo.y - bar.y, 2);
          if (distSq < DIST_CUTOFF_SQUARED) {
            ctx.write(null, new Text(StringUtils.join(",", 
              new String[] {
              String.valueOf(foo.x), String.valueOf(foo.y), 
              String.valueOf(bar.x), String.valueOf(bar.y)
            })));
          }
        }
      }
      foos.clear();
      bars.clear();
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf, "spatial-join");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setJarByClass(SpatialJoin.class);
    job.setMapperClass(Mapper1.class);
    job.setReducerClass(Reducer1.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(PointWritable.class);
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
      System.out.println("Usage: SpatialJoin foos bars output");
      System.exit(-1);
    }
    int res = ToolRunner.run(new Configuration(), new SpatialJoin(), args);
    System.exit(res);
  }
}
