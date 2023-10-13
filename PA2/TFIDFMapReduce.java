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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TFIDFMapReduce extends Configured implements Tool {
  // Job1: Extract docID and article body
  public static class Job1Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text docID = new Text();
    private Text unigram = new Text();
    private static final IntWritable defaultOne = new IntWritable(1);
    private static final Pattern docIDPattern = Pattern.compile("<====>(\\d+)<====>");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();

      Matcher docIDMatcher = docIDPattern.matcher(line);
      if (docIDMatcher.find()) {
        // Extract document ID
        String docIDString = docIDMatcher.group(1).trim();
        docID.set(docIDString);

        // Extract and clean the text
        String text = line.substring(docIDMatcher.end()).replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();

        // Split the text into unigrams (words)
        String[] words = text.split(" ");

        for (String word : words) {
          if (!word.isEmpty()) {
            unigram.set(docID + "\t" + word);
            context.write(unigram, defaultOne);
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
      context.write(key, unigramCount);
    }
  }

  // Job2: Calculate TF values
  public static class Job2Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text docID = new Text();
    private IntWritable termFrequency = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] values = value.toString().split("\t");
      if (values.length >= 3) {
        docID.set(values[0]);
        int tf = Integer.parseInt(values[2]); // Extract TF from the third column
        termFrequency.set(tf);
        context.write(docID, termFrequency);
      }
    }
  }

  public static class Job2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      // Calculate max frequency for the article
      int maxFrequency = 0;
      for (IntWritable value : values) {
        int tf = value.get();
        if (tf > maxFrequency) {
          maxFrequency = tf;
        }
      }

      // Calculate and output TF values
      for (IntWritable value : values) {
        int tf = value.get();
        context.write(key, new IntWritable(tf)); // This emits the unigram and its term frequency as an IntWritable
      }
    }
  }

  // Job3: Calculate IDF and TF-IDF values
  // public static class Job3Mapper extends Mapper<Text, Text, Text, Text> {
  // public void map(Text key, Text value, Context context) throws IOException,
  // InterruptedException {
  // // Input: docID, TF value
  // // Calculate IDF and TF-IDF values
  // // Output: docID, TF-IDF value
  // }
  // }
  //
  // public static class Job3Reducer extends Reducer<Text, Text, Text, Text> {
  // private long N;
  //
  // protected void setup(Context context) throws IOException,
  // InterruptedException {
  // // Fetch the total number of documents from a Counter (set in a previous job)
  // N = context.getConfiguration().getLong("total_documents", 0);
  // }
  //
  // public void reduce(Text key, Iterable<Text> values, Context context) throws
  // IOException, InterruptedException {
  // // Calculate IDF and TF-IDF values using the provided formula
  // // Output: docID, TF-IDF value
  // pass
  // }
  //
  // }

  public int run(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    // job1
    Job job1 = Job.getInstance(conf, "Job1");
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    job1.setJarByClass(TFIDFMapReduce.class);
    job1.setMapperClass(Job1Mapper.class);
    job1.setReducerClass(Job1Reducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    Configuration conf2 = new Configuration();
    // job2
    Job job2 = Job.getInstance(conf, "Job2");
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    job2.setJarByClass(TFIDFMapReduce.class);
    job2.setMapperClass(Job2Mapper.class);
    job2.setReducerClass(Job2Reducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);

    Job job3 = Job.getInstance(conf, "Job3");

    JobControl jobControl = new JobControl("TFIDFJob");
    ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
    ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
    // ControlledJob controlledJob3 = new ControlledJob(job3.getConfiguration());

    // Add dependencies between jobs
    controlledJob2.addDependingJob(controlledJob1);
    // controlledJob3.addDependingJob(controlledJob2);

    // Add the controlled jobs to the JobControl
    jobControl.addJob(controlledJob1);
    jobControl.addJob(controlledJob2);
    // jobControl.addJob(controlledJob3);

    // Start the JobControl thread
    Thread jobControlThread = new Thread(jobControl);
    jobControlThread.start();

    // Wait for the JobControl thread to finish
    while (!jobControl.allFinished()) {
      Thread.sleep(1000);
    }
    System.exit(jobControl.getFailedJobList().size());
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TFIDFMapReduce(), args);
    System.exit(res);
  }
}
