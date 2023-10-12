package TFIDFMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class TFIDFMapReduce {

  // Job 1: Calculate TF, IDF, and TF-IDF values for terms
  public static class TFIDFMapper1 extends Mapper<LongWritable, Text, Text, MapWritable> {
    private final Text word = new Text();
    private final MapWritable outputValue = new MapWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Tokenize the input text
      StringTokenizer tokenizer = new StringTokenizer(value.toString());
      // Calculate TF (Term Frequency)
      Map<String, Integer> termFrequency = new HashMap<>();
      int totalTerms = 0;

      while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        termFrequency.put(token, termFrequency.getOrDefault(token, 0) + 1);
        totalTerms++;
      }

      // Calculate TF for each term
      for (Map.Entry<String, Integer> entry : termFrequency.entrySet()) {
        word.set(entry.getKey());
        outputValue.clear();
        outputValue.put(new Text("TF"), new IntWritable(entry.getValue()));
        outputValue.put(new Text("TotalTerms"), new IntWritable(totalTerms));
        context.write(word, outputValue);
      }
    }
  }

  public static class TFIDFReducer1 extends Reducer<Text, MapWritable, Text, MapWritable> {
    private final Map<String, Integer> documentFrequency = new HashMap<>();
    private final MapWritable outputValue = new MapWritable();

    public void reduce(Text key, Iterable<MapWritable> values, Context context)
        throws IOException, InterruptedException {
      int df = 0;
      for (MapWritable value : values) {
        df++;
      }
      documentFrequency.put(key.toString(), df);

      outputValue.clear();
      outputValue.put(new Text("DF"), new IntWritable(df));
      context.write(key, outputValue);
    }
  }

  // Job 2: Generate a summary using TF-IDF values
  public static class TFIDFMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    private final Text word = new Text();
    private final Text outputValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Parse input values
      String[] parts = value.toString().split("\t");
      if (parts.length == 2) {
        word.set(parts[0]);
        outputValue.set(parts[1]);
        context.write(word, outputValue);
      }
    }
  }

  public static class TFIDFReducer2 extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // Generate the summary using TF-IDF values (no need to re-calculate IDF here)
      StringBuilder summary = new StringBuilder();
      for (Text value : values) {
        summary.append(value.toString()).append("\t");
      }

      context.write(key, new Text(summary.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "TF-IDF Calculation");
    job1.setJarByClass(TFIDFMapReduce.class);

    // Set the input and output paths for Job 1
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));

    // Set the mapper and reducer classes for Job 1
    job1.setMapperClass(TFIDFMapper1.class);
    job1.setReducerClass(TFIDFReducer1.class);

    // Set the input and output formats for Job 1
    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    // Set the output key and value classes for Job 1
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(MapWritable.class);

    // Run Job 1
    job1.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Summary Generation");
    job2.setJarByClass(TFIDFMapReduce.class);

    // Set the input and output paths for Job 2
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));

    // Set the mapper and reducer classes for Job 2
    job2.setMapperClass(TFIDFMapper2.class);
    job2.setReducerClass(TFIDFReducer2.class);

    // Set the input and output formats for Job 2
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    // Set the output key and value classes for Job 2
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    // Run Job 2
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
