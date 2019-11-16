package com.github.AbouOpenSource.kafka_step_one;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	public static void main( String[] args )
    {
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
		
		String bootstrapServers="127.0.0.1:9092";
		
		//create Producer properties
		Properties properties = new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
		
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		// create the producer 
		
		//key is string and value is string
		KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties); 
		
		//create a producer record
		for(int i=0;i<10;i++) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","hello-world"+Integer.toString(i));
		
		
		
		//send data - asynchronous
		producer.send(record,new Callback() {

			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO Auto-generated method stub
				//executes every time a record is successfully sent or an exception is thrown
				if(exception == null) {
					// the record was successful sent
					logger.info("Received new metada \n"+
								"Topic: "+metadata.topic()+"\n"+
								"Partition: "+metadata.partition()+ "\n"+
								"Offset: "+ metadata.offset()+"\n"+
								"Timestamp: "+metadata.timestamp());
				
				}else
				{
					logger.error("Error while producing",exception);
				}
			}
			
			
			
		});
		}
		//flush data
		producer.flush();
		
		//flush and close producer
		
		producer.close();
		
    }
}
