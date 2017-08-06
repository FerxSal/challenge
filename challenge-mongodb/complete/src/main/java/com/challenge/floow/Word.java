package com.challenge.floow;

import org.springframework.data.annotation.Id;

public class Word {

	    @Id
	    public String id;

	    public String word;
	    public Integer count;
		
	    
	    public Word(){ }
	    
	    public Word(String word, Integer count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return "Word [word=" + word + ", count=" + count + "]";
		}
	

}
