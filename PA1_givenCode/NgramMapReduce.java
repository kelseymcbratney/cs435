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

public class NgramMapReduce extends Configured implements Tool{
	private static int CurrentVolume = 1;

	public static enum Profiles {
		A1 ('a', 1),
		B1 ('b', 1),
		A2 ('a', 2),
		B2 ('b', 2);

	    private final char profileChar;
	    private final int ngramNum;

	    private Profiles(char c, int n) {
	    	//#TODO#: initialize Profiles class variables
	    }

	    public boolean equals(Profiles p) {
				//#TODO#: implement equals operator for instances of Profiles
	    }
	}

	public static class TokenizerMapper extends Mapper<Object, BytesWritable, Text, VolumeWriteable> {

			//#TODO#: define and initialize some class variables
			private VolumeWriteable volume = new VolumeWriteable(SOMETHING, SOMETHING); //#TODO#: check the class definition ad update arguments

	    public void map(Object key, BytesWritable bWriteable, Context context) throws IOException, InterruptedException {
	    	Profiles profile = context.getConfiguration().getEnum("profile", Profiles.A1); //get profile

				//#TODO#: initial update of appropriate TokenizerMapper class variable(s)

				//code to get a book
	    	String rawText = new String(bWriteable.getBytes());
	    	Book book = new Book(rawText, profile.ngramNum);
	    	StringTokenizer itr = new StringTokenizer(book.getBookBody());

				//#TODO#: define any helper variables you need before looping through tokens
	    	while (itr.hasMoreTokens()) {
	    	//#TODO#: implement mapper code for each token (word)
				//hint: make sure to handle all 4 profiles
				//hint: think about how the output format is specified

	    	}
		    //#TODO#: update NgramMapReduce class variable(s)
	    }

	}

	public static class IntSumReducer extends Reducer<Text, VolumeWriteable, Text, VolumeWriteable> {
		//#TODO#: initialize class variables

		public void reduce(Text key, Iterable<VolumeWriteable> values, Context context) throws IOException, InterruptedException {
			//#TODO#: implement reducer
		}
	}

	public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
		// function to run job

		Job job = Job.getInstance(conf, "ngram");

		//specify classes for Map Reduce tasks
		//#TODO#: update SPECIFYCLASS.class placeholders with appropriate class names

		job.setInputFormatClass(SPECIFYCLASS.class);
		job.setJarByClass(SPECIFYCLASS.class);

		job.setMapperClass(SPECIFYCLASS.class);
		job.setCombinerClass(SPECIFYCLASS.class);
		job.setReducerClass(SPECIFYCLASS.class);

		job.setOutputKeyClass(SPECIFYCLASS.class);
		job.setOutputValueClass(SPECIFYCLASS.class);

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN
		int res = ToolRunner.run(new Configuration(), new NgramMapReduce(), args);
    System.exit(res); //res will be 0 if all tasks are executed succesfully and 1 otherwise
	}

	@Override
   	public int run(String[] args) throws Exception {
		//#TODO#: update few things
		Configuration conf = this.getConf();
		Profiles profiles[] = {Profiles.A1, Profiles.A2, Profiles.B1, Profiles.B2};
		for(Profiles p : profiles) {
			conf.setEnum("profile", SOMETHING); //#TODO#: update this
			System.out.println("For profile: " + p.toString());
			if(runJob(SOMETHING, args[0], args[1]+p.toString()) != 0) //#TODO#: update this
				return 1; //error
		}
		return 0;	 //success
   	}
}
