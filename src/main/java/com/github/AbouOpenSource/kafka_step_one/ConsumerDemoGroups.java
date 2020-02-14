package com.github.AbouOpenSource.kafka_step_one;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoGroups {

	public static void main( String[] args ) throws InterruptedException, ExecutionException
    {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);
		String bootstrapServer="127.0.0.1:9092";
		String groupId="my-fourth-application";
		String topic="first_topic";
		
		//Create cnsumer configs
		Properties properties= new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
	    
		//create the consumer
		KafkaConsumer<String, String> consumer= new KafkaConsumer<String, String>(properties);
		
		// subcribe consumer to our topic(s)
    consumer.subscribe(Arrays.asList(topic));
    
    while(true) {
    ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));// new in kafka 2.0.0
    
    for(ConsumerRecord<String,String> record:records) {
    	logger.info("Key: "+record.key()+", value: "+record.value());
    	logger.info("Partition: "+record.partition()+", offset: "+record.offset());
    }
    
    }
		
    }
	
}
