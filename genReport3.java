package com.anz.Integrity;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

public class genReport3 {

	@SuppressWarnings("static-access")
	public static void main(String[] args) {
		// IntegrityCheck.start();
		   Properties prop = new Properties();
			InputStream input = null;
			 
			String mongoDBHost = null;
			String mongoDBName = null;
			
			int NTHREDS = 100;
			int NotifyCTMTime = 20; //Time to Notify CTM
			int SwitchTime = 50; // Time to SWITCH
			
			int maxQueryResult = 100;
			
			MongoOperations mongoOperation ;
				
			try {

				input = new FileInputStream(System.getProperty("user.dir") + "//conf//config.properties");

				// load a properties file
				prop.load(input);

				NTHREDS = Integer.parseInt(prop.getProperty("worker.thread.max"));
				mongoDBHost = prop.getProperty("spring.data.mongodb.host") + ":" + prop.getProperty("spring.data.mongodb.port") ;
				mongoDBName = prop.getProperty("spring.data.mongodb.dbname") ;
				
				 NotifyCTMTime =Integer.parseInt(prop.getProperty("NotifyCTM.maxwait.time"));
				 SwitchTime = Integer.parseInt(prop.getProperty("Switch.maxwait.time"));
				
				 maxQueryResult = Integer.parseInt(prop.getProperty("fetchseq.max.limit"));
				
			} catch (IOException ex) {
				ex.printStackTrace();
			} finally {
				if (input != null) {
					try {
						input.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			MongoClientOptions.Builder options = MongoClientOptions.builder();
			options.connectionsPerHost(4000)  ;
			//options.maxConnectionLifeTime(1500);
			//options.connectTimeout(1000);
			//options.heartbeatSocketTimeout(1500);
			
			
			
			options.socketKeepAlive(true);
			MongoClient mongo = new MongoClient(
					mongoDBHost, options.build());
					mongoOperation = new MongoTemplate(mongo, mongoDBName);

//					
					String[] fields = { "_id", "time1","time2","time3"};
							
					Aggregation agg =  Aggregation.newAggregation(
					//	Aggregation.unwind("bankaccttrn"),
						Aggregation.project(fields),
						Aggregation.sort(Direction.ASC, "_id")

						).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build()); ;
					
					
			 AggregationResults<endtoendlog> data = null;
			 try {
				data = mongoOperation.aggregate(agg,"endtoendlog",endtoendlog.class) ;
			 }catch(Exception e) {
				 e.printStackTrace();
			 }
			
			
			BufferedWriter writer1 = null;

			try {
				writer1 = new BufferedWriter(new FileWriter("endToend.csv"));
		
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		
			
			System.out.println( "total size " + data.getMappedResults().size());
					
			for (int i=0;i<data.getMappedResults().size();i++) {
				
			    try {
			    	
			    	writer1.write( data.getMappedResults().get(i).get_id() + ","+ data.getMappedResults().get(i).getTime1() + "," + data.getMappedResults().get(i).getTime2()+ "," + data.getMappedResults().get(i).getTime3());
			    	writer1.newLine();
			    	
				
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			     
			  
			}
					
			try {
				writer1.close();
		
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}
	
	

}
