
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

public class StepCalcProb {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text r1 = new Text();
		private Text r2 = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] keys = value.toString().split("\t");
			String firstCorpus = keys[1];
			String secCorpus = keys[2];
			int keyCorpus = Integer.parseInt(firstCorpus) + Integer.parseInt(secCorpus);
			Text rText = new Text(String.format("%d",keyCorpus));
			Text oneFirst = new Text(String.format("%s\t%s", "1",secCorpus));
			Text oneSec = new Text(String.format("%s\t%s", "1",firstCorpus));
			r1.set(firstCorpus);
			r2.set(secCorpus);
			context.write(rText, oneFirst);
			context.write(rText, oneSec);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text value = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double prob= 0;
			double tSum = 0;
			double sum = 0;
			double N = Double.parseDouble(context.getConfiguration().get("Nsummary"));
			for (Text value : values) {
				String[] vals = value.toString().split("\t");
				sum += Double.parseDouble(vals[0]);
				tSum += Double.parseDouble(vals[1]);      
			}
			prob = tSum/(N * sum);
			value.set(String.format("%.20f", prob));
			context.write(key,value);
		}
	}

	public static void main(String[] args) throws Exception {
		BasicAWSCredentials credentials = new BasicAWSCredentials("name", "key");
		AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion("us-east-1") 
				.build();
		S3Object o = s3.getObject("nadavhadoop","nCount");
		BufferedReader reader = new BufferedReader(new InputStreamReader(o.getObjectContent()));
		String Nsum = reader.readLine().toString();
		Configuration conf = new Configuration();
		conf.set("Nsummary", Nsum);
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(StepCalcProb.class);
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