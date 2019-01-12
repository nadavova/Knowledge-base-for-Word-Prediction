import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

public class StepSort {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] w1w2w3 = value.toString().split("\\s+"); 
			String[] aa = value.toString().split("\t"); 
			String w1w2 = w1w2w3[0] + " " + w1w2w3[1];
			double prob = Double.parseDouble(aa[1]);
			context.write(new Text(w1w2 + " " + (1 - prob)), new Text(value.toString() + "\t" + prob));
		}
	}

	public class MyPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}    

	}
	public static class Reduce extends Reducer<Text,Text,Text,Text> 
	{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException 
		{
			Text temp = new Text();
			for (Text value : values) {
				double ans = Double.parseDouble(value.toString().split("\t")[1]);
				temp.set(String.format("%.20f", ans));
				context.write(new Text(value.toString().split("\t")[0]), temp);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(StepSort.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		FileInputFormat .setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		System.out.println(success);
	}
}
