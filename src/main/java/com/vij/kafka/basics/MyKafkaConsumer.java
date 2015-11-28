package com.vij.kafka.basics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;



public class MyKafkaConsumer extends Thread{

	
	 
	 ConsumerConnector consumerConnector;
	 final static String clientId = "MyKafkaConsumerClient";
	 final static String TOPIC = "tradesTopic";
	 
	  public MyKafkaConsumer(){
		  
		  Properties properties = new Properties();
	      properties.put("zookeeper.connect","localhost:2181");
	      properties.put("group.id","trades");
	      ConsumerConfig consumerConfig = new ConsumerConfig(properties);
	      consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
	
		  
        }
	    
	public static void main(String[] args) {
		MyKafkaConsumer kafkaConsumer = new MyKafkaConsumer();
		kafkaConsumer.start();
	}
	
	 @Override
	    public void run() {
	       
		 Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	        topicCountMap.put(TOPIC, new Integer(1));
	        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
	        KafkaStream<byte[], byte[]> stream =  consumerMap.get(TOPIC).get(0);
	        ConsumerIterator<byte[], byte[]> it = stream.iterator();
	        System.out.println("Consuming PV01 messages#1");
	        while(it.hasNext())
	            System.out.println("Consuming PV01 messages#1 : "+new String(it.next().message()));
		 
	      

	    }
	
}
