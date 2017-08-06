package com.challenge.floow;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface WordRepository extends MongoRepository<Word, Integer>{

	 public Word findByWord(String word);
	
	
}
