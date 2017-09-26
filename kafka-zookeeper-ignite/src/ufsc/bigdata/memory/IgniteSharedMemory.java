package ufsc.bigdata.memory;

import java.util.ArrayList;
import java.util.List;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import ufsc.bigdata.MyTweet;

public class IgniteSharedMemory {
	
	// PROPRIEDADES
	private final static String CACHE_NAME = "twitter1";
	// PROPRIEDADES
	
	Ignite ignite = Ignition.start();
	IgniteCache<String, MyTweet>  cache = null;
	
	public IgniteSharedMemory(){

		CacheConfiguration cfg = new CacheConfiguration();
		
		cfg.setName(CACHE_NAME);
		cfg.setIndexedTypes(String.class, MyTweet.class);
		
		cache = ignite.getOrCreateCache(cfg);
	}
		
	public List<MyTweet> search(String query){
		
		SqlQuery sql = new SqlQuery(MyTweet.class, "username = ?");

		List<MyTweet> response = new ArrayList<MyTweet>();
		
		try (QueryCursor<Entry<String, MyTweet>> cursor = cache.query(sql.setArgs(query))) {
		  for (Entry<String, MyTweet> entry: cursor)
			  response.add(entry.getValue());
		}
		
		return response;
	}
	
	public void insert(MyTweet tweet){
		
		cache.put(tweet.getId(), tweet);
	}

	// TESTE
	public static void main(String [] args){
		
		MyTweet tweet = new MyTweet();
		tweet.setId("1");
		tweet.setText("Texto de teste");
		tweet.setUsername("lhzsantana");
		
		IgniteSharedMemory igniteSharedMemory = new IgniteSharedMemory();
		
		igniteSharedMemory.insert(tweet);
		
		for(MyTweet myTweet:igniteSharedMemory.search("lhzsantana")){
			System.out.println(myTweet.getId());
			System.out.println(myTweet.getText());
		}
	}
}