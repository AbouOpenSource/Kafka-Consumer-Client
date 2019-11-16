package com.github.AbouOpenSource.kafka_step_one;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
//import org.apache.kafka.common.protocol.types.Field.Array;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {

	public static void main( String[] args ) throws InterruptedException, ExecutionException
    {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
		String bootstrapServer="127.0.0.1:9092";
		String topic="seventh_topic";
		
		//Create cnsumer configs
		Properties properties= new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
	    
		//create the consumer
		KafkaConsumer<String, String> consumer= new KafkaConsumer<String, String>(properties);
	
		// assign and seek are mostly used to replay data or fetch a specific message 
		
		
		//assign 
		TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
		long offsetToReadFrom =15L;
		consumer.assign(Arrays.asList(partitionToReadFrom));
		
		//seek
		
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		
		
		int numberOfMessagesToRead = 5;
		boolean keepOnReading = true;
		int numberOfMessagesToReadFar =0 ;
		
    while(keepOnReading) {
    ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));// new in kafka 2.0.0
    
    for(ConsumerRecord<String,String> record:records) {
    	
    	numberOfMessagesToReadFar +=1;
    	logger.info("Key: "+record.key()+", value: "+record.value());
    	logger.info("Partition: "+record.partition()+", offset: "+record.offset());
    	
    	if(numberOfMessagesToReadFar >= numberOfMessagesToRead) {
    		keepOnReading= false;
    		//to exit while loop
    		break;
    	}
    
    
    }
    
    }
		
   logger.info("Exiting the application");
    }
	
}
