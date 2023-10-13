package TFIDFMapReduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.List;
import java.util.ArrayList;

public class TFIDFMapReduce extends Configured implements Tool {
  public static enum Counters {
    TOTAL_RECORDS
  }

  // Job1: Extract docID and article body
  public static class Job1Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text docID = new Text();
    private Text unigram = new Text();
    private static final IntWritable defaultOne = new IntWritable(1);
    private static final Pattern docIDPattern = Pattern.compile("<====>(\\d+)<====>");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      context.getCounter(Counters.TOTAL_RECORDS).increment(1);

      String line = value.toString();

      Matcher docIDMatcher = docIDPattern.matcher(line);
      if (docIDMatcher.find()) {
        // Extract document ID
        String docIDString = docIDMatcher.group(1).trim();
        docID.set(docIDString);

        // Extract and clean text
        String text = line.substring(docIDMatcher.end()).replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();

        // Split the text into unigrams
        String[] words = text.split(" ");

        for (String word : words) {
          if (!word.isEmpty()) {
            unigram.set(docID + "\t" + word);
            context.write(unigram, defaultOne); // DocID , (Unigram 1)
          }
        }
      }
    }
  }

  public static class Job1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable unigramCount = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      unigramCount.set(sum);
      context.write(key, unigramCount); // DocID , (Unigram Frequency)
    }
  }

  // Job2: Calculate TF values
  public static class Job2Mapper extends Mapper<Object, Text, Text, Text> {
    private Text docID = new Text();
    private Text termFrequency = new Text();
    private Text unigram = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] values = value.toString().split("\t");
      if (values.length >= 3) {
        docID.set(values[0]);
        unigram.set(values[1]);
        termFrequency.set(values[2]);
        context.write(docID, new Text(unigram + "\t" + termFrequency)); // DocID , (Unigram Frequency)
      }
    }
  }

  public static class Job2Reducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      // Calculate max frequency for the article
      int maxFrequency = 0;
      List<String> tfList = new ArrayList<String>();

      for (Text value : values) {
        tfList.add(value.toString());
        int tf = Integer.parseInt(value.toString().split("\t")[1]);

        if (tf > maxFrequency) {
          maxFrequency = tf;
        }
      }

      // Calculate and output TF values
      for (String value : tfList) {
        String[] tfValues = value.toString().split("\t");
        String unigram = tfValues[0];
        double tf = Double.parseDouble(tfValues[1]);
        tf = 0.5 + (0.5 * (tf / maxFrequency));
        context.write(new Text(key), new Text(unigram + "\t" + tf)); // DocID , (Unigram termFrequency)
      }
    }
  }

  // Job3: Calculate IDF and TF-IDF values

  public static class Job3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text docID = new Text();
    private Text termFrequency = new Text();
    private Text unigram = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String[] values = value.toString().split("\t");
      if (values.length >= 3) {
        docID.set(values[0]);
        unigram.set(values[1]);
        termFrequency.set(values[2]);
        context.write(new Text(docID), new Text(unigram + "\t" + termFrequency)); // DocID , (Unigram termFrequency)
      }
    }
  }

  public static class Job3Reducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      long articleCount = context.getConfiguration().getLong(Counters.TOTAL_RECORDS.name(), 0);
      int unigramCount = 0;
      List<String> tfList = new ArrayList<String>();

      for (Text value : values) {
        tfList.add(value.toString());
        unigramCount += 1;
      }

      for (String value : tfList) {
        String[] tfValues = value.toString().split("\t");
        String unigram = tfValues[0];
        double tf = Double.parseDouble(tfValues[1]);
        double idf = Math.log10(articleCount / unigramCount);
        double tfidf = tf * idf;
        context.write(key, new Text(unigram + "\t" + tfidf)); // DocID , (Unigram TF-IDF)
      }

    }

  }

  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    // job1
    Job job1 = Job.getInstance(conf, "Job1");
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    job1.setJarByClass(TFIDFMapReduce.class);
    job1.setMapperClass(Job1Mapper.class);
    job1.setReducerClass(Job1Reducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    job1.waitForCompletion(true);
    Counter counter = job1.getCounters().findCounter(Counters.TOTAL_RECORDS);

    // job2
    Job job2 = Job.getInstance(conf, "Job2");
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    job2.setJarByClass(TFIDFMapReduce.class);
    job2.setMapperClass(Job2Mapper.class);
    job2.setReducerClass(Job2Reducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    job2.waitForCompletion(true);

    // job3
    Job job3 = Job.getInstance(conf, "Job3");
    job3.getConfiguration().setLong(counter.getDisplayName(), counter.getValue());
    FileInputFormat.addInputPath(job3, new Path(args[2]));
    FileOutputFormat.setOutputPath(job3, new Path(args[3]));
    job3.setJarByClass(TFIDFMapReduce.class);
    job3.setMapperClass(Job3Mapper.class);
    job3.setReducerClass(Job3Reducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);

    return (job3.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TFIDFMapReduce(), args);
    System.exit(res);
  }
}
