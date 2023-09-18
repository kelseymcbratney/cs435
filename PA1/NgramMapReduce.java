package ngram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class NgramMapReduce extends Configured implements Tool {
  private static int CurrentVolume = 1;

  public static enum Profiles {
    A1('a', 1),
    B1('b', 1),
    A2('a', 2),
    B2('b', 2);

    private final char profileChar;
    private final int ngramNum;

    private Profiles(char c, int n) {
      profileChar = c;
      ngramNum = n;
    }

    public boolean equals(Profiles p) {
      if (p.equals(profileChar)) {
        return true;
      } else {
        return false;
      }
    }
  }

  public static class TokenizerMapper extends Mapper<Object, BytesWritable, Text, VolumeWriteable> {
    IntWritable defaultInt = new IntWritable(1);
    MapWritable defaultMap = new MapWritable();

    private VolumeWriteable volume = new VolumeWriteable(defaultMap, defaultInt);
    private Text inputString = new Text();

    public void map(Object key, BytesWritable bWriteable, Context context) throws IOException, InterruptedException {
      Profiles profile = context.getConfiguration().getEnum("profile", Profiles.A1); // get profile
      volume.insertMapValue(defaultInt, defaultInt);

      // code to get a book
      String rawText = new String(bWriteable.getBytes());
      Book book = new Book(rawText, profile.ngramNum);
      StringTokenizer itr = new StringTokenizer(book.getBookBody());

      // while (itr.hasMoreTokens()) {
      // switch (profiles) {
      // case "A1":
      // inputString.set(itr.nextToken() + '\t' + book.getBookYear() + '\t');
      // context.write(inputString, volume);
      // break;
      // case "B1":
      // break;
      // case "A2":
      // break;
      // case "B2":
      // break;
      // }
      // #TODO#: update NgramMapReduce class variable(s)
    }

  }

  public static class IntSumReducer extends Reducer<Text, VolumeWriteable, Text, VolumeWriteable> {
    private VolumeWriteable volume = new VolumeWriteable();
    private MapWritable map = new MapWritable();
    private IntWritable defaultInt = new IntWritable(1);

    public void reduce(Text key, Iterable<VolumeWriteable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (VolumeWriteable value : values) {
        sum++;
      }
      volume.set(new MapWritable(), new IntWritable(sum));
      volume.insertMapValue(defaultInt, defaultInt);
      context.write(key, volume);
    }

    public static void runJob(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "ngram");

      job.setJarByClass(NgramMapReduce.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setCombinerClass(IntSumReducer.class);
      job.setReducerClass(IntSumReducer.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
      // ToolRunner allows for command line configuration parameters - suitable for
      // shifting between local job and yarn
      // example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value
      // <input_path> <output_path>
      // We use -D mapreduce.framework.name=<value> where <value>=local means the job
      // is run locally and <value>=yarn means using YARN
      int res = ToolRunner.run(new Configuration(), new NgramMapReduce(), args);
      System.exit(res); // res will be 0 if all tasks are executed succesfully and 1 otherwise
    }

    public int run(String[] args) throws Exception {
      Configuration conf = this.getConf();
      Profiles profiles[] = { Profiles.A1, Profiles.A2, Profiles.B1, Profiles.B2 };
      for (Profiles p : profiles) {
        conf.setEnum("profile", p);
        System.out.println("For profile: " + p.toString());
        if (runJob(conf, args[0], args[1] + p.toString()) != 0)
          return 1; // error
      }
      return 0; // success
    }
  }
}
