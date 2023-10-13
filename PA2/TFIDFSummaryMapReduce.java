package TFIDFSummaryMapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.List;
import java.util.ArrayList;
import java.util.TreeMap;

public class TFIDFSummaryMapReduce extends Configured implements Tool {
  // Job1: Calculate TF-IDF values
  public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
    private Text docID = new Text();
    private Text termFrequency = new Text();
    private Text unigram = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tfValues = value.toString().split("\t");
      if (tfValues.length >= 3) {
        docID.set(tfValues[0]);
        unigram.set(tfValues[1]);
        termFrequency.set(tfValues[2]);
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
        String text = line.substring(docIDMatcher.end()).replaceAll("[^A-Za-z0-9 .]", "").toLowerCase();

        // Split the text into unigrams
        String[] sentences = text.split(". ");

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
  public static class SummaryReducer extends Reducer<Text, Text, NullWritable, Text> {
    public void setup(Context context) throws IOException, InterruptedException {
      unigramTreeMap = new TreeMap<DoubleWritable, Text>();
    }

    public String generateSummary(List<String> tfidfValues) {
      String foo = "foo";
      return foo;
      // // Sort the TF-IDF values
      // Collections.sort(tfidfValues);
      // // Get the top 5 TF-IDF values
      // List<Double> top5TFIDFValues = tfidfValues.subList(tfidfValues.size() - 5,
      // tfidfValues.size());
      // // Calculate the average of the top 5 TF-IDF values
      // double averageTop5TFIDFValues =
      // top5TFIDFValues.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
      // // Get the sentences that have TF-IDF values greater than the average of the
      // top
      // // 5 TF-IDF values
      // List<String> summarySentences = new ArrayList<>();
      // for (String tfidfValue : tfidfValues) {
      // if (tfidfValue > averageTop5TFIDFValues) {
      // summarySentences.add(tfidfValue);
      // }
      // }
      // // Sort the sentences by the TF-IDF values
      // Collections.sort(summarySentences);
      // // Get the top 5 sentences
      // List<String> top5SummarySentences =
      // summarySentences.subList(summarySentences.size() - 5,
      // summarySentences.size());
      // // Generate the summary
      // StringBuilder summary = new StringBuilder();
      // for (String sentence : top5SummarySentences) {
      // summary.append(sentence);
      // summary.append("\n");
      // }
      // return summary.toString();

    }

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      List<String> tfidfValues = new ArrayList<>();
      for (Text value : values) {
        String[] valueSplit = value.toString().split("\t");
        if (valueSplit[0].startsWith("A")) {
          unigramTreeMap.put(key + "\t" + valueSplit[1], new DoubleWritable(Double.parseDouble(valueSplit[2])));
        } else if (valueSplit[0].startsWith("B")) {
          tfidfValues.add(valueSplit[1]);
        }

      }

      String summary = generateSummary(tfidfValues);
      context.write(NullWritable.get(), new Text(summary));
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
