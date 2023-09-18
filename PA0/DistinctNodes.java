
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException; 
import java.util.HashSet; 
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;


//comment: make sure you go through this code and understand it
public class DistinctNodes extends Configured implements Tool {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, NullWritable> {
		private Text node ;

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString()); 
			while (itr.hasMoreTokens()) {
				String s = itr.nextToken();
				node = new Text( s ); context.write(node, NullWritable.get());
			} 
		}
	}
	public static class CountReducer extends Reducer<Text, NullWritable, IntWritable, NullWritable>{
		private Set<String> distinctNodes ;

		@Override
		protected void setup(Context context) {
			distinctNodes = new HashSet<String>(); 
		 }

		 @Override
		 protected void reduce(Text key, Iterable<NullWritable> values, Context context) {
		 	distinctNodes.add(key.toString()); 
		 }

		 @Override
		 protected void cleanup(Context context) throws IOException, InterruptedException {
		 	IntWritable noOfNodes = new IntWritable(distinctNodes.size());
		 	context.write(noOfNodes, NullWritable.get()); 
		 }
	}

	public static void main(String[] args) throws Exception { 

		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN 
		int res = ToolRunner.run(new Configuration(), new DistinctNodes(), args);
        System.exit(res);
    }

    @Override
   	public int run(String[] args) throws Exception {
   		//Configuration conf = new Configuration();
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "distinct nodes new"); 
		job.setJarByClass(DistinctNodes.class); 
		job.setMapperClass(DistinctNodes.TokenizerMapper.class); 
		job.setReducerClass(DistinctNodes.CountReducer.class); 
		job.setNumReduceTasks(1); 
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(NullWritable.class); 
		job.setOutputKeyClass(IntWritable.class); 
		job.setOutputValueClass(NullWritable.class); 
		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 
		return job.waitForCompletion(true) ? 0 : 1;
   	}
		
	 
}