package TFIDFSummaryMapReduce;

import java.io.IOException;

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
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

import java.util.List;
import java.util.ArrayList;

public class TFIDFSummaryMapReduce extends Configured implements Tool {
  // Job1: Calculate TF-IDF values
  public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
    private Text docID = new Text();
    private Text termFrequency = new Text();
    private Text unigram = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tfValues = value.toString().split("\t");
      if (values.length >= 3) {
        docID.set(values[0]);
        unigram.set(values[1]);
        termFrequency.set(values[2]);
        context.write(new Text(docID), new Text("A" + "\t" + unigram + "\t" + termFrequency)); // DocID , (Unigram
                                                                                               // termFrequency)
      }
    }
  }

  // Job2: Retrieve Sentences
  public static class SentenceMapper extends Mapper<Object, Text, Text, Text> {
    private Text docID = new Text();
    private Text articleSentence = new Text();
    private static final Pattern docIDPattern = Pattern.compile("<====>(\\d+)<====>");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();

      Matcher docIDMatcher = docIDPattern.matcher(line);
      if (docIDMatcher.find()) {
        // Extract document ID
        String docIDString = docIDMatcher.group(1).trim();
        docID.set(docIDString);

        // Extract and clean text
        String text = line.substring(docIDMatcher.end()).replaceAll("[^A-Za-z0-9 ]", "").toLowerCase();

        // Split the text into unigrams
        String[] sentences = text.split(".");

        for (String sentence : sentences) {
          if (!sentence.isEmpty()) {
            articleSentence.set("B" + "\t" + sentence);
            context.write(docID, articleSentence); // DocID , (Unigram 1)
          }

        }
      }

    }
  }

  // Reducer: Generate Summary based on TF-IDF values
  public static class SummaryReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      int foo = 0;
      // Calculate summary based on TF-IDF values and emit (DocID, Summary) pairs

      // Example pseudocode (modify as per your specific use case):
      // List<Double> tfidfValues = new ArrayList<>();
      // for (DoubleWritable value : values) {
      // tfidfValues.add(value.get());
      // }
      // String summary = generateSummary(tfidfValues); // Implement this method
      // context.write(key, new Text(summary));
    }
  }

  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // Job1 Configuration
    Job job1 = Job.getInstance(conf, "job1");
    MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, TFIDFMapper.class);
    MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, SentenceMapper.class);

    job1.setJarByClass(TFIDFSummaryMapReduce.class);
    job1.setReducerClass(SummaryReducer.class);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job1, new Path(args[2]));

    return (job1.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TFIDFSummaryMapReduce(), args);
    System.exit(res);
  }
}
