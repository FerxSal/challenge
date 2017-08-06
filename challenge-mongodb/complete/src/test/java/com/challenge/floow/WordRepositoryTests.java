package com.challenge.floow;


import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@PropertySource("classpath:application.properties")
@ConfigurationProperties
public class WordRepositoryTests {

	  @Autowired
	  private WordRepository repositoryword;

      Word word_0, word_1, word_2;
 
      @Value("${sourcefile}")
      private String source;
      

      @Value("${outputfile}")
      private String output;
      
      
    @Before
    public void setUp() {

    	repositoryword.deleteAll();

        word_0 =   repositoryword.save(new Word("testword0", 3));
        word_1 = repositoryword.save(new Word("testword1", 3));
        word_2 = repositoryword.save(new Word("testword2", 3));
    }

    @Test
    public void setsIdOnSave() {

        Word dave = repositoryword.save(new Word("Dave", 1));

        assertThat(dave.id).isNotNull();
    }

    @Test
    public void findsByLastName() {

        Word result = repositoryword.findByWord("testword0");
        
        assertThat(result).extracting("word").toString().contains(String.valueOf(3)); 
    }
    

    @Test
    public void testJob() throws IOException,
    InterruptedException, ClassNotFoundException {

    	Job jobtest = new Job();
		jobtest.setJarByClass(WordCount.class);
		jobtest.setJobName("WordCounter");
		
		FileInputFormat.addInputPath(jobtest, new Path(source));
		FileOutputFormat.setOutputPath(jobtest, new Path(output));
	
		jobtest.setOutputKeyClass(Text.class);
		jobtest.setOutputValueClass(IntWritable.class);
		jobtest.setOutputFormatClass(TextOutputFormat.class);
		
		//Set the MapClass and ReduceClass in the job
		jobtest.setMapperClass(MapClass.class);
		jobtest.setReducerClass(ReduceClass.class); 
		
		jobtest.waitForCompletion(true);
		//jobtest.submit();
		
		int returnValue = jobtest.waitForCompletion(true) ? 0:1;
		
		assertTrue(jobtest.isSuccessful());
	    assertEquals(returnValue,0);
    	
    }
    
   @Test
   public void testResults() throws IOException{
	   
	    String uri = output+"/part-r-00000";
		Configuration confg = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), confg);
		FSDataInputStream in = null;
		in = fs.open(new Path(uri));  
		String[] res;
		res = in.readLine().split("\t");
		
		assertEquals("&amp;" ,res[0].toString());
		assertEquals(res[1],String.valueOf(3));
		
		in.close();
	   
   }
}