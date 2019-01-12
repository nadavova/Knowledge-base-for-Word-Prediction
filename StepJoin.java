/**
 * WordCount.java - the very-first MapReduce Program
 *
 * <h1>How To Compile</h1>
 * export HADOOP_HOME=/usr/lib/hadoop-0.20/
 * export DIR=wordcount_classes
 * rm -fR $DIR
 * mkdir $DIR
 * javac -classpath ${HADOOP_HOME}/hadoop-core.jar -d $DIR WordCount.java 
 * jar -cvf wordcount.jar -C $DIR .
 */



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StepJoin {
	static int N = 0;
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text r1 = new Text();
		private Text r2 = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitted = value.toString().split("\t");
			try {
				String ngram = splitted[0];
				String firstCor = splitted[1];
				String secCor = splitted[2];
				int sum = Integer.parseInt(firstCor) + Integer.parseInt(secCor);
				Text output1TextKey =  new Text(String.format("%s", sum));
				Text output1TextValue =  new Text(String.format("%s", ngram));
				context.write(output1TextKey, output1TextValue);
			} catch (Exception e) {
				String rKey = splitted[0];
				String prob = splitted[1];
				Text output2TextKey =  new Text(String.format("%s", rKey));
				Text output2TextValue =  new Text(String.format("%s", prob));
				context.write(output2TextKey, output2TextValue);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text probText = new Text("*");
			List<String> ngramtStringtext = new ArrayList<String>();
			for (Text value : values) {
				if(value.toString().contains("0.")) {
					probText.set(value.toString());
				}
				else {
					ngramtStringtext.add(value.toString());
				}
			}
			for (String number : ngramtStringtext) {
				Text tmp = new Text();
				tmp.set(String.format("%s", number));
				if(!probText.toString().equals("*")) {
					context.write(tmp, probText);	
				}
			}
		}
	}
	
	private static class PartitionerClass extends Partitioner<Text,Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions){
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(StepJoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(PartitionerClass.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class);	
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
	}
}