package com.mycompany.hiaex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * HIA 5.4.7 - Spatial Join enhanced with Bloom Filter.
 * Since bars << foos, the Bloom Filter is on the bars.
 */
public class SpatialJoinWithBloomFilter extends Configured implements Tool {

  private static final double DIST_CUTOFF_SQUARED = 1.0D;
  private static final String BLOOMFILTER_PATH = "temp1/bloomfilter";
  
  private static class Point {
    
    public Double x;
    public Double y;
    
    public Point(Double x, Double y) {
      this.x = x;
      this.y = y;
    }
    
    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append(StringUtils.leftPad(String.valueOf(x), 12, '0')).
        append(",").
        append(StringUtils.leftPad(String.valueOf(y), 12, '0'));
      return buf.toString();
    }
  }
  
  private static class PointWritable 
      implements WritableComparable<PointWritable> {

    private Point p = new Point(0.0D, 0.0D);
    
    public PointWritable() {} /* Needed by Hadoop */
    
    public PointWritable(Point p) {
      this.p = p;
    }
    
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
      Mapper<LongWritable,Text,LongWritable,BloomFilter> {
    
    private static final LongWritable ONE = new LongWritable(1L);
    private BloomFilter bf;
    private AtomicLong counter;
    
    @Override
    protected void setup(Context ctx) 
        throws IOException, InterruptedException {
      // see http://hadoop.apache.org/common/docs/r0.20.2/api/org/apache/hadoop/util/bloom/BloomFilter.html
      // and http://llimllib.github.com/bloomfilter-tutorial/
      bf = new BloomFilter(150000, 2, Hash.MURMUR_HASH);
      counter = new AtomicLong(0L);
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context ctx)
        throws IOException, InterruptedException {
      long currentCount = counter.incrementAndGet();
      if (currentCount % 1000 == 0) {
        ctx.setStatus("Mapping " + currentCount + " records to bloomfilter");
        ctx.progress();
      }
      String[] cols = StringUtils.split(value.toString(), ",");
      // cols[0] = "BAR" (since bars << foos)
      Double x = Math.floor(Double.valueOf(cols[1]));
      Double y = Math.floor(Double.valueOf(cols[2]));
      // the cell enclosing the point plus its immediate
      // neighbors
      for (double i = x - 1; i <= x + 1; i++) {
        for (double j = y - 1; j <= y + 1; j++) {
          Point cell = new Point(i, j); 
          bf.add(new Key(cell.toString().getBytes()));
        }
      }
    }
    
    @Override
    protected void cleanup(Context ctx) 
        throws IOException, InterruptedException {
      ctx.write(ONE, bf);
    }
  }
  
  public static class Reducer1 extends
      Reducer<LongWritable,BloomFilter,NullWritable,NullWritable> {
    
    private BloomFilter bf;

    @Override
    protected void setup(Context ctx) 
        throws IOException, InterruptedException {
      bf = new BloomFilter(150000, 2, Hash.MURMUR_HASH);
    }
    
    @Override
    protected void reduce(LongWritable key, Iterable<BloomFilter> values,
        Context ctx) throws IOException, InterruptedException {
      for (BloomFilter value : values) {
        bf.or(value);
      }
    }
    
    @Override
    protected void cleanup(Context ctx) 
        throws IOException, InterruptedException {
      Configuration conf = ctx.getConfiguration();
      Path path = new Path(BLOOMFILTER_PATH);
      FSDataOutputStream out = path.getFileSystem(conf).create(path);
      bf.write(out);
      out.close();
    }
  }
  
  public static class Mapper2 extends
      Mapper<LongWritable,Text,PointWritable,Text> {
    
    private BloomFilter bf;
    
    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      Path[] cachedFiles = DistributedCache.getLocalCacheFiles(ctx.getConfiguration());
      if (cachedFiles != null && cachedFiles.length > 0) {
        Configuration conf = ctx.getConfiguration();
        Path path = new Path(BLOOMFILTER_PATH);
        FSDataInputStream in = new FSDataInputStream(path.getFileSystem(conf).open(path));
        bf = new BloomFilter(150000, 2, Hash.MURMUR_HASH);
        bf.readFields(in);
      }
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context ctx)
        throws IOException, InterruptedException {
      String[] cols = StringUtils.split(value.toString(), ",");
      Double x = Math.floor(Double.valueOf(cols[1]));
      Double y = Math.floor(Double.valueOf(cols[2]));
      Point p = new Point(x, y);
      if ("FOO".equals(cols)) {
        // only pass FOOs which are "close" to a BAR, ie
        // if the hash of FOO's cell is found in the bloomfilter
        Key bfkey = new Key(p.toString().getBytes());
        if (bf.membershipTest(bfkey)) {
          ctx.write(new PointWritable(p), value);
        }
      } else {
        // pass all BARs through
        ctx.write(new PointWritable(p), value);
      }
    }
  }
  
  public static class Reducer2 extends
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
        String[] cols = StringUtils.split(value.toString(), ",");
        Double x = Double.valueOf(cols[1]);
        Double y = Double.valueOf(cols[2]);
        if ("FOO".equals(cols[0])) {
          foos.add(new Point(x, y));
        } else {
          bars.add(new Point(x, y));
        }
      }
      for (Point foo : foos) {
        for (Point bar : bars) {
          double distSq = Math.pow(foo.x - bar.x, 2) + Math.pow(foo.y - bar.y, 2);
          if (distSq < DIST_CUTOFF_SQUARED) {
            ctx.write(null, new Text(StringUtils.join(new String[] {
                String.valueOf(foo.x), String.valueOf(foo.y),
                String.valueOf(bar.x), String.valueOf(bar.y)
            }, ",")));
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
    // job 1 - build bloom filter
    Job job1 = new Job(conf, "spatial-join-build-bloomfilter");
    FileInputFormat.addInputPath(job1, new Path(args[1])); // bars.txt
    FileOutputFormat.setOutputPath(job1, new Path("temp2"));
    job1.setJarByClass(SpatialJoinWithBloomFilter.class);
    job1.setMapperClass(Mapper1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setMapOutputKeyClass(LongWritable.class);
    job1.setMapOutputValueClass(BloomFilter.class);
    job1.setNumReduceTasks(1); // for the complete bloom filter to be built
    boolean succ = job1.waitForCompletion(true);
    if (! succ) {
      System.out.println("Job1 Failed, exiting");
      return -1;
    }
    // job2 - using the bloom filter
    Job job2 = new Job(conf, "spatial-join-use-bloomfilter");
    DistributedCache.addCacheFile(new Path(BLOOMFILTER_PATH).toUri(), 
      job2.getConfiguration());
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    job2.setJarByClass(SpatialJoinWithBloomFilter.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    job2.setMapOutputKeyClass(PointWritable.class);
    job2.setMapOutputValueClass(Text.class);
    succ = job2.waitForCompletion(true);
    if (! succ) {
      System.out.println("Job 2 failed, exiting");
      return -1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("Usage: SpatialJoinWithBloomFilter foos bars output");
      System.exit(-1);
    }
    int res = ToolRunner.run(new Configuration(), new SpatialJoinWithBloomFilter(), args);
    System.exit(res);
  }
}
