import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class StepDivide {
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		boolean counter = true;
		@Override
		public void map (LongWritable key, Text line, Context context)  throws IOException, InterruptedException {
			String[] splitted = line.toString().split("\t");
			System.out.println(line.toString());
			String w1 = splitted[0].replaceAll("(?U)[^\\p{L}\\p{N}\\s&.-]", "");
			w1 = w1.replaceAll("\\d","");
		//	System.out.println("splitted[0] : " + splitted[0]);
			//System.out.println("filtered w1 : " + w1);
			//System.out.println("splitted[1](occur) : " + splitted[2]);

			if(!(w1.equals("!!!"))){
				int occur = Integer.parseInt(splitted[2]);
				Text text = new Text();
				Text text1 = new Text();
				Text text2=new Text();
				Text textPair = new Text();
				if (counter) {
					textPair.set(String.format("%s\t0",splitted[2]));
					text.set(String.format("%s ",w1));
					counter = false;
				}
				else {
					textPair.set(String.format("0\t%s",splitted[2]));
					text.set(String.format("%s ",w1));
					counter= true;
				}
				text2.set(String.format("!!!"));// *
				text1.set(String.format("%d",occur));//num of occurance
				context.write(text2,text1);
				context.write(text,textPair);
			}
		}
	}

	private static class Reduce extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String w1 = key.toString();
			int sum_occ = 0;
			int odd = 0;
			int even = 0;
			Text newKey = new Text();
			Text sumEvenOdd = new Text();
			if(!(w1.equals("!!!"))){
				for (Text val : values) {
					String[] evenNodd = val.toString().split("\t");
					even += Integer.parseInt(evenNodd[0]);
					odd += Integer.parseInt(evenNodd[1]);
				}
				sumEvenOdd.set(String.format("%s\t%s",even,odd));
				newKey.set(String.format("%s",w1));
				context.write(newKey, sumEvenOdd);
			}
			else {
				for (Text val : values) {
					sum_occ += Integer.parseInt(val.toString());
				}
				nCount(sum_occ);;
			}
		}

		public void nCount(int sum) {
			BasicAWSCredentials credentials = new BasicAWSCredentials("name", "key");
			AmazonS3 s3 = AmazonS3ClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials))
					.withRegion("us-east-1") 
					.build();
			String str = "nCount";
			try {
				PrintWriter writer = new PrintWriter(str, "UTF-8");
				writer.println(sum);
				writer.close();
				File file = new File(str);
				s3.putObject(new PutObjectRequest("nadavhadoop", str, file));
			} catch (AmazonServiceException ase) {
				System.out.println("Caught an AmazonServiceException, which " +
						"means your request made it " +
						"to Amazon S3, but was rejected with an error response" +
						" for some reason.");
				System.out.println("Error Message:    " + ase.getMessage());
				System.out.println("HTTP Status Code: " + ase.getStatusCode());
				System.out.println("AWS Error Code:   " + ase.getErrorCode());
				System.out.println("Error Type:       " + ase.getErrorType());
				System.out.println("Request ID:       " + ase.getRequestId());
			} catch (AmazonClientException ace) {
				System.out.println("Caught an AmazonClientException, which " +
						"means the client encountered " +
						"an internal error while trying to " +
						"communicate with S3, " +
						"such as not being able to access the network.");
				System.out.println("Error Message: " + ace.getMessage());
			} catch (FileNotFoundException e) {
				System.out.println("my exception : file not found");
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				System.out.println("my exception : unsupportedencoding");
				e.printStackTrace();
			}
		}
	}

	private static class PartitionerClass extends Partitioner<Text,Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions){
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}

	public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException  {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(StepDivide.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(StepDivide.PartitionerClass.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat .setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		System.out.println(success);
	}
}
