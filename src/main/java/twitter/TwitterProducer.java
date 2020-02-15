package twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import sun.awt.geom.AreaOp;

public class TwitterProducer {
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	public TwitterProducer() {}
	public static void main(String[] args) {
		new TwitterProducer().run();
	
	}
	
	
	public void run() {
		logger.info("Setup");
		
		
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		
		
		
		// create a twitter client 
		Client client = createTwitterClient(msgQueue);
						// Attempts to establish a connection.
		
		client.connect();
		//create a kafka producer

		KafkaProducer<String, String> producer = createKafkaProducer();
		
		//add a shutdown hook
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run(){	
			logger.info("stopping application");
			logger.info("shutting down client from twitter");
			
			//client.stop();
			
			//producer.stop();
			}
		
		});
		
		
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
		  String msg= null;
		try {
			msg = msgQueue.poll(5,TimeUnit.SECONDS);
			//System.out.println(msg);
		}catch(InterruptedException e) {
			e.printStackTrace();
			client.stop();
		}
		  
		  
		  
		  if (msg!=null) {
			  logger.info(msg);
			  producer.send(new ProducerRecord<String,String>("twitter_tweets",null,msg),new Callback() {
				  public void onCompletion(RecordMetadata recordMetadata,Exception e) {
					  if(e!=null) {
						  logger.error("Something bad happens",e);
					  }
				  }
			  });
		  }
		 
		}		
				//loop to send tweets to kafka
	logger.info("End of application ");		
	}
	
	String consumerKey ="RFqnOgdVUbiVs5Su9ModLdcqS";
	String consumerSecret="7OiMcZ15LcnhCC1t3QfnCOLNrhiM82y7zr1YepCCz4yIb7irFA";
	String token ="904799546324250624-OLZ4dz4pH93YILHh2LNJoWyg99vF32R";
	String secret ="BhciJhczmNKd7esv4mfhnJ0Da6N0ydwDNmkjHTzDb871m";
	List<String> terms = Lists.newArrayList("bitcoin","usa","politics","sport","soccer");
	
	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		
		
		
		
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		//List<Long> followings = Lists.newArrayList(1234L, 566788L);
		//List<String> terms = Lists.newArrayList("twitter", "api");
		
		//hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		
		
		
		
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));
				 
				Client hosebirdClient = builder.build();
				return hosebirdClient;
				
		
	}
	
	public   KafkaProducer<String, String>  createKafkaProducer(){
		String bootstrapServers="127.0.0.1:9092";

		
		Properties properties = new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
		
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		// create safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
		// kafka 2.0 >= 1.0 so we can keep this as 5. Use 1 otherwise
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

		//high throughput producer (at the expense of a it of latency and CPU usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(20 * 1024));

		//key is string and value is string
		KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties); 
		return producer;
	};
}
