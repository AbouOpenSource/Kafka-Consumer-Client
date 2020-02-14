package com.github.AbouOpenSource.kafka_step_one;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
	public static void main( String[] args ) throws InterruptedException, ExecutionException
    {
		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		
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
		
			String topic= "first_topic";
			String value="hello-world"+Integer.toString(i);
			String key= "id_"+Integer.toString(i);
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);
		
		logger.info("Key: "+key);// log the key
		//id_0 is going to partition 1
		//id_1 partition 0
		//id_2 partition 2
		//id_3 partition 0
		//id_4 partition 2
		//id_5 partition 2
		//id_6 partition 0
		//id_7 partition 2
		//id_8 partition 1
		//id_9 partition 2
		/**
		 * The same key will be always in the same partition 
		 *only if the number of partition didn't change
		 */
		
		
		
		
		
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
			
			
			
		}).get();//block the .send() to make it synchronous - don't do this in production 
		}
		//flush data
		producer.flush();
		
		//flush and close producer
		
		producer.close();
		
    }
}
