package ufsc.bigdata.queue;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import ufsc.bigdata.MyTweet;

public class KafkaProducerQueue {

	// PROPRIEDADES
	private final static String SERVER = "localhost:9092";
	// PROPRIEDADES
	
	private static KafkaProducer<String, MyTweet> producer;
	private final String topic = "myTweetTopic";

	public KafkaProducerQueue() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", SERVER);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		properties.put("request.required.acks", "1");
		
		producer = new KafkaProducer<String, MyTweet>(properties);
	}

	public void publishMesssage(MyTweet tweet) throws Exception {
		producer.send(new ProducerRecord(topic, tweet.getId(), convertToByteArray(tweet)));
	}
	
	public void close(){
		producer.close();		
	}

	public static void main(String[] args) throws Exception {		

		MyTweet tweet = new MyTweet();
		tweet.setId("1");
		tweet.setText("Texto de teste");
		tweet.setUsername("lhzsantana");
				
		KafkaProducerQueue producer = new KafkaProducerQueue();
		
		producer.publishMesssage(tweet);
		
		producer.close();
		
	}

    private byte[] convertToByteArray(MyTweet tweet){
    	
    	ByteArrayOutputStream bos = new ByteArrayOutputStream();
    	
    	ObjectOutput out = null;
    	
    	try {
    	  out = new ObjectOutputStream(bos);   
    	  out.writeObject(tweet);
    	  return bos.toByteArray();
    	} catch (Exception e) {
			e.printStackTrace();
		}
    	
    	return null;
    }

}