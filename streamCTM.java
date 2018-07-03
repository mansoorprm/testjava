package com.123.CTMKstream5;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.stereotype.Component;

import com.123.storm.CTM.model.intraDayStaging;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

import net.sf.jsefa.Deserializer;
import net.sf.jsefa.flr.FlrIOFactory;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;

import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Pattern;




@Component("CTMStream")
public class streamCTM {
	private static MongoDatabase mongoDB;
	private static MongoClient mongoClient;
	static MongoCollection<Document> collection;
	
    private String topic = "Topic3";
    private KafkaStreams streams;
    
    Date logStartDate = null;
    
    @PostConstruct
    public void runStream() {
    	
    	Properties prop = new Properties();
		InputStream input = null;
		
		String mongoDBHost = null;
		String mongoDBName = null;
		String mongoCollection = null;
		int mongoDBPort = 27017;
		
		int parallelismcount = 1;
		String appId = "CTM-Stream";
		String kafkaServer = "localhost:9092";
		
		try {
			
			input = new FileInputStream(System.getProperty("user.dir") + "//conf/config.properties");
			
			prop.load(input);
			
			parallelismcount = Integer.parseInt(prop.getProperty("no.thread"));
			mongoDBHost = prop.getProperty("mongodb.host");
			mongoDBPort = Integer.parseInt(prop.getProperty("mongodb.port"));
			mongoDBName = prop.getProperty("mongodb.database");
			mongoCollection = prop.getProperty("mongodb.collection");
			
			
			appId = prop.getProperty("kstream.appId");
			kafkaServer = prop.getProperty("kafka.host");
			
		}catch(IOException ex) {
			ex.printStackTrace();
		}finally {
			if (input != null) {
				try {
					input.close();
				}catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		mongoClient = new MongoClient(mongoDBHost,mongoDBPort);
		
		mongoDB = mongoClient.getDatabase(mongoDBName);
		
		collection = mongoDB.getCollection(mongoCollection);
		
		Properties config = new Properties();
        final String version = "0.2";
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "CTM-Stream" + version);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
      //  config.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "org.apache.kafka.streams.processor.WallclockTimestampExtractor");
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,parallelismcount);
        config.put(StreamsConfig.EXACTLY_ONCE, "exactly_once");

     //   config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		StreamsConfig streamConfig = new StreamsConfig(config);
		
		StreamsBuilder KStreamBuilder = new StreamsBuilder();
		
		KStream<String,String> source = KStreamBuilder.stream("Topic3");
		
	//	System.out.println("testing");
		
		
		
		source.foreach(new ForeachAction<String, String>() {

			public void apply(String key, String value) {
				executeStream(value);
				//i++;
				//System.out.println(key + ": " + value);
				//System.out.println("Count == " + i);
			}
		});
		
		
		
		Topology topology = KStreamBuilder.build();
		
		KafkaStreams kafkaStreams = new KafkaStreams(topology,streamConfig);
		kafkaStreams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
		
    	System.out.println("test");
    }
    
/*
    @PostConstruct
    public void runStream() {
        Serde<String> stringSerde = Serdes.String();

        Properties config = new Properties();
        final String version = "0.2";
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-example-" + version);
        config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "org.apache.kafka.streams.processor.WallclockTimestampExtractor");
		

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
		
		StreamsBuilder KStreamBuilder = new StreamsBuilder();
		
		KStream<String,String> source = KStreamBuilder.stream("Topic3");
		
	//	System.out.println("testing");
		
		
		
		source.foreach(new ForeachAction<String, String>() {

			public void apply(String key, String value) {
				
				//i++;
				System.out.println(key + ": " + value);
				//System.out.println("Count == " + i);
			}
		});
       

		Topology topology = KStreamBuilder.build();
		
		KafkaStreams kafkaStreams = new KafkaStreams(topology,config);
		kafkaStreams.start();
    }
*/
    @PreDestroy
    public void closeStream() {
        streams.close();
    }
    
public void executeStream(String input) {
		
		String value = input;
		
		SimpleDateFormat Dtformatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
		logStartDate = new Date();
		try {
		
			Deserializer deserializer = FlrIOFactory.createFactory(intraDayStaging.class).createDeserializer();
	        
			Reader red = new StringReader(value);
	    
		    deserializer.open(red);
		   
		    intraDayStaging CTM = deserializer.next();
		    
		    deserializer.close(true);
		
		    CTM.setTime1(Dtformatter.format(logStartDate));
		    
		  //  System.out.println("before update");
		  
    		boolean status = updateCollection(collection, CTM);
    		
    	
		    System.out.println("Status of Upsert: " + status);
		   
		
			
		}catch(Exception e) {
			System.out.println("Exception in MongoDB BOLT " + e);
		
		}
		
	}
	
	public static boolean updateCollection(
    		final MongoCollection<Document> collection, final intraDayStaging CTM) {
    		boolean status = true;
    	
    		ObjectMapper mapper = new ObjectMapper();
    		String jsonString;
    		
    		try {
    			//mapper.disable(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS);
        		// mapper.setDateFormat(new ISO8601DateFormat());
    			jsonString = mapper.writeValueAsString(CTM);
    		
    			BasicDBObject query = new BasicDBObject();
    			query.append("transeqnum", CTM.getTranseqnum());
    			BasicDBObject doc = BasicDBObject.parse(jsonString);
    			
    			Bson newDocument = new Document("$set", doc);
    			
    			UpdateResult result = collection.updateOne(query, newDocument,
    					(new UpdateOptions()).upsert(true));
    			
    			System.out.println("Update Matched Count....: "
    					+ result.getMatchedCount());
    		
    			
    		} catch (IOException e) {
    			status = false;
    		}

    		return status;
    	}
}
