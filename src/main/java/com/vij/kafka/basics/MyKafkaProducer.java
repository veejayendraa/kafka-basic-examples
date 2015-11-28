package com.vij.kafka.basics;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MyKafkaProducer {
	 final static String TOPIC = "tradesTopic";

	public static void main(String[] args) throws InterruptedException {
		Properties properties = new Properties();
		properties.put("metadata.broker.list","localhost:9092");
		properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("groupid","trades-PV01");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
        String riskTypesPV01 = new String("1013");
        
        for(int i=0;i<10000;i++)
        {
        	if(i%10==0)
        	{
        		System.out.println("Going to sleep for");
        		Thread.sleep(1*60*1000);
        	}
	        	KeyedMessage<String, String> messagePV01 =new KeyedMessage<String, String>(TOPIC,getTradeInfo(riskTypesPV01));
	        	producer.send(messagePV01);
        }
        producer.close();
	}
	
	
	static String getTradeInfo(String riskType)
	{
		try{
		Random r = new Random();
		int bookId = r.nextInt(8500-8200) + 8200;
		int dealNum = r.nextInt(15000-10000) + 10000;
		double amt =  1.0 + (1000.0 - 1.0) * r.nextDouble();
		Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String tradeDate = formatter.format(new Date());
		String tradeData = tradeDate+","+bookId+",ICE@"+dealNum+","+riskType+","+amt;
		System.out.println(tradeData);
		return tradeData;
		}catch(Exception ex){return null;}
		
	}

}
