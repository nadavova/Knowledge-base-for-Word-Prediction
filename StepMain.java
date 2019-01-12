package mapreduce;

import org.apache.hadoop.mapreduce.TestMapCollection.StepFactory;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class StepMain {
	public static AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
	public static AmazonS3 S3;
	public static AmazonEC2 ec2;
	public static AmazonElasticMapReduce emr;
	public static void main(String[]args){
		System.out.println("Connecting to aws & S3");
		System.out.println("===========================================");
		S3 = AmazonS3ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();

		System.out.println("Connecting to ec2");
		System.out.println("===========================================");
		ec2 = AmazonEC2ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();
		
		System.out.println("Creating EMR instance");
		System.out.println("===========================================");
		emr= AmazonElasticMapReduceClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();
		
		
		StepFactory stepFactory = new StepFactory();
		/*
        Step No 1: Divide the corpus into two parts and get N
		 */
		HadoopJarStepConfig step1 = new HadoopJarStepConfig()
				.withJar("s3://nadavhadoop/step1.jar")
				.withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data","s3://nadavhadoop/step1outPut");
		StepConfig stepOne = new StepConfig()
				.withName("step1")
				.withHadoopJarStep(step1)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
				
		
		/*
        Step No 2: Calculate Tr, Nr and get the probabilities
		 */
		
		HadoopJarStepConfig step2 = new HadoopJarStepConfig()
				.withJar("s3://nadavhadoop/step2.jar")
				.withArgs("s3://nadavhadoop/step1outPut","s3://nadavhadoop/step2outPut");
		StepConfig stepTwo = new StepConfig()
				.withName("step2")
				.withHadoopJarStep(step2)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		
		/*
        Step No 3: Join the probabilities with Ngram
		 */
		
		HadoopJarStepConfig step3 = new HadoopJarStepConfig()
				.withJar("s3://nadavhadoop/step3.jar")
				.withArgs("s3://nadavhadoop/step1outPut","s3://nadavhadoop/step2outPut","s3://nadavhadoop/step3outPut");
		StepConfig stepThree = new StepConfig()
				.withName("step3")
				.withHadoopJarStep(step3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		
		/*
        Step No 4: Sort the corpus
		 *
		 */
		
		HadoopJarStepConfig step4 = new HadoopJarStepConfig()
				.withJar("s3://nadavhadoop/step4.jar")
				.withArgs("s3://nadavhadoop/step3outPut","s3://nadavhadoop/step4outPut");
		StepConfig stepFour = new StepConfig()
				.withName("step4")
				.withHadoopJarStep(step4)
				.withActionOnFailure("TERMINATE_JOB_FLOW");


		/*
        Set instances
		 */
		
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
				.withInstanceCount(3)
				.withMasterInstanceType(InstanceType.M3Xlarge.toString())
				.withSlaveInstanceType(InstanceType.M3Xlarge.toString())
				.withHadoopVersion("2.7.3")                                 
				.withEc2KeyName("test")         
				.withPlacement(new PlacementType("us-east-1a"))
				.withKeepJobFlowAliveWhenNoSteps(false);
		
		/*
        Run all jobs
		 */
		RunJobFlowRequest request = new RunJobFlowRequest()
				.withName("Assignment2Nadav")                                   
				.withInstances(instances)
				.withSteps(stepOne,stepTwo,stepThree,stepFour)
				.withLogUri("s3n://nadavhadoop/logs/")
				.withServiceRole("EMR_DefaultRole")
				.withJobFlowRole("EMR_EC2_DefaultRole")
				.withReleaseLabel("emr-5.11.0");
		RunJobFlowResult result = emr.runJobFlow(request);
		String id=result.getJobFlowId();
		System.out.println("Nadav's cluster id: "+id);
	}
}

