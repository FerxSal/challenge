package com.challenge.floow;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application implements CommandLineRunner {

	
	@Autowired
	private WordRepository repositoryword;
	

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	private static Configuration getConfig() {
	    Configuration conf = new Configuration();
	    return conf;
	}
	
	
	@Override
	public void run(String... args) throws Exception {

		if (args.length != 2) {
			System.err.printf("Usage: %s needs two arguments <input> <output> files\n",
					getClass().getSimpleName());
		}
		
		else{	

	
		final Configuration conf = getConfig();

        final JobConf jobconfig = new JobConf(conf);
        
        Job job = Job.getInstance(jobconfig);
		
		//Job job = new Job();
		job.setJarByClass(WordCount.class);
		job.setJobName("WordCounter");
		
		//Add input and output file paths to job based on the arguments passed
		FileInputFormat.setInputPaths(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		//Set the MapClass and ReduceClass in the job
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		job.waitForCompletion(true);
		//job.submit();
		 
		if(job.isSuccessful()) {
			System.out.println("Job was successful");
			System.out.println("Updating DataBase");
			
			//File to read in HDFS
			String uri = args[1]+"/part-r-00000";
			Configuration confg = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), confg);
			FSDataInputStream in = null;
			in = fs.open(new Path(uri));
			String[] res;
			
			repositoryword.deleteAll();
			
			while (in.read() != -1){
			 res = in.readLine().split("\t");
			 repositoryword.save(new Word(res[0],Integer.valueOf(res[1])));
			}
			System.out.println("Updating DataBase");
			System.out.println();
			in.close();
		
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");			
		}


	 }
	}
}
