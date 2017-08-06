package com.challenge.floow;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.springframework.beans.factory.annotation.Autowired;


import com.mongodb.BasicDBObjectBuilder;

/**
 * Reduce class which is executed after the map class and takes
 * key(word) and corresponding values, sums all the values and write the
 * word along with the corresponding total occurances in the output
 * 
 * @author Raman
 */
public class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{

	/**
	 * Method which performs the reduce operation and sums 
	 * all the occurrences of the word before passing it to be stored in output
	 */
	
	public static HashMap<String, Integer> wordcounts = new HashMap<String, Integer>();
	
	
	
	public HashMap<String, Integer> getWordcounts() {
		return wordcounts;
	}


	public void setWordcounts(HashMap<String, Integer> wordcounts) {
		this.wordcounts = wordcounts;
	}


	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
	
		int sum = 0;
		Iterator<IntWritable> valuesIt = values.iterator();
		
		while(valuesIt.hasNext()){
			sum = sum + valuesIt.next().get();
		}
		
		context.write(key, new IntWritable(sum));
		//System.out.println("Key: " +key.toString());
		//System.out.println("Value: "+sum);
		//repositoryword.save(new Word(key.toString(),sum));
		wordcounts.put(key.toString(),sum);
		
	}	
	
	
	  public static class Map extends Mapper<Object, BSONObject, Text, IntWritable> {
	        @Override
	        public void map(final Object key, final BSONObject doc, final Context context)
	          throws IOException, InterruptedException {
	            final String word = (String)doc.get("word");
	            final Number wordCounting = (Number)doc.get("counting");

	            context.write( new Text(word), new IntWritable(wordCounting.intValue()));
	        }
	}
	
}