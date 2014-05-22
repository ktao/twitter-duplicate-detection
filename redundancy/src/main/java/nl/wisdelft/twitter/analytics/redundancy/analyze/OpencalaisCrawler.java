/*********************************************************
*  Copyright (c) 2010 by Web Information Systems (WIS) Group.
*  Fabian Abel, http://fabianabel.de/
*
*  Some rights reserved.
*
*  Contact: http://www.wis.ewi.tudelft.nl/
*
**********************************************************/
package nl.wisdelft.twitter.analytics.redundancy.analyze;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import mx.bigdata.jcalais.CalaisClient;
import mx.bigdata.jcalais.CalaisObject;
import mx.bigdata.jcalais.CalaisResponse;
import mx.bigdata.jcalais.rest.CalaisRestClient;
import nl.wisdelft.twitter.io.DBUtility;

import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

/**
 * This class queries <a href="http://www.opencalais.com/">OpenCalais</a> to 
 * retrieve further semantic descriptions about news articles (and tweets).
 * 
 * 
 * @author Fabian Abel, <a href="mailto:f.abel@tudelft.nl">f.abel@tudelft.nl</a>
 * @author last edited by: $Author: fabian $
 * 
 * @version created on Nov 26, 2010
 * @version $Revision: 1.1 $ $Date: 2011-01-25 18:46:38 $
 */
public class OpencalaisCrawler {

	private static final String API_KEY_1 = "YOUR OWN API KEY";
	
	private static List<String> API_KEYS = new ArrayList<String>();
	static {
		API_KEYS.add(API_KEY_1);
		// more API KEYs can be added.
//		API_KEYS.add(API_KEY);
//		API_KEYS.add(API_KEY);
//		API_KEYS.add(API_KEY);
//		API_KEYS.add(API_KEY);
	}
	
	public static String API_KEY = API_KEY_1;
	
	private static void switchAPIKey(){
		int index = API_KEYS.indexOf(API_KEY) + 1;
		if(index < API_KEYS.size()){
			API_KEY = API_KEYS.get(index);
		}else{
			API_KEY = API_KEYS.get(0);
		}
	}
	
	public static String switchAPIKey(String oldKey){
		int index = API_KEYS.indexOf(oldKey) + 1;
		if(index < API_KEYS.size() && index > 0){
			return API_KEYS.get(index);
		}else{
			return API_KEYS.get(0);
		}
	}
	
	/**
	 * Approach the OpenCalais Web service to let OpenCalais parse the 
	 * content of tweets that are returned by the given query. Not limited to a single user.
	 * @param query Format changed: Jan. 19th, 2011, SELECT id, content, publish_date, userId FROM tweets WHERE ... 
	 */
	@SuppressWarnings("unchecked")
	public static void getOpenCalaisSemanticsForTweetsAndStoreInDB(String query){
		ResultSet tweets = null;

		System.out.println("Querying for tweets...");
		System.out.println(query);
		tweets = DBUtility.executeQuery(query);
		try{
			String content = null;
			Integer userId = null;
			Long tweetId = null;
			String uri = null;
			Timestamp creationTime = null;
			Map<String, Object> entityAttributes = null;
			
			CalaisClient client = new CalaisRestClient(API_KEY);
		    CalaisResponse response = null;
			
			BatchSqlUpdate su_NewsEntity = new BatchSqlUpdate(
					DBUtility.ds,
					"INSERT IGNORE INTO model_semantics_tweets_oc_entity_tweets2011 (userId, tweetId, type, typeURI, name, uri, relevance, creationTime) "
							+ " values (?,?,?,?,?,?,?,?)");
			su_NewsEntity.declareParameter(new SqlParameter("userId", Types.INTEGER)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("tweetId", Types.BIGINT)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("type", Types.VARCHAR)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("typeURI", Types.VARCHAR)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("name", Types.VARCHAR)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("uri", Types.VARCHAR)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("relevance", Types.DOUBLE)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("creationTime", Types.TIMESTAMP)); //$NON-NLS-1$
			
			BatchSqlUpdate su_NewsTopic = new BatchSqlUpdate(
					DBUtility.ds,
					"INSERT IGNORE INTO model_semantics_tweets_oc_topic_tweets2011 (userId, tweetId, topic, uri, relevance, creationTime) "
							+ " values (?,?,?,?,?,?)");
			su_NewsTopic.declareParameter(new SqlParameter("userId", Types.INTEGER)); //$NON-NLS-1$
			su_NewsTopic.declareParameter(new SqlParameter("tweetId", Types.BIGINT)); //$NON-NLS-1$
			su_NewsTopic.declareParameter(new SqlParameter("topic", Types.VARCHAR)); //$NON-NLS-1$
			su_NewsTopic.declareParameter(new SqlParameter("uri", Types.VARCHAR)); //$NON-NLS-1$
			su_NewsTopic.declareParameter(new SqlParameter("relevance", Types.DOUBLE)); //$NON-NLS-1$
			su_NewsTopic.declareParameter(new SqlParameter("creationTime", Types.TIMESTAMP)); //$NON-NLS-1$
			
			//iterate over all news to make OpenCalais Web Service Call and store the entities, etc.
			int count = 0; int storeAfter = 50;
			while (tweets.next()) {
				try{
					count++;
					userId = tweets.getInt("userId");
					content = tweets.getString("content");
					tweetId = tweets.getLong("id");
					creationTime = tweets.getTimestamp("creationTime");
					
					System.out.println("Processing Tweet " + tweetId);
					
					//make OpenCalais Web service call:
					response = client.analyze(content);
					
					//store entities:
					try{
						if(response.getEntities() != null){
							for (CalaisObject entity : response.getEntities()) {
								//hashed URI:
								uri = entity.getField("_uri");
								if(entity.getList("resolutions") != null){
									entityAttributes = (Map<String, Object>) entity.getList("resolutions").iterator().next();
									if(entityAttributes != null && entityAttributes.containsKey("id")){
										//overwrite URI with resolvable URI:
										uri = entityAttributes.get("id").toString();
									}
								}
								
								//store news entity assignment:
								Object[] newsEntity = {
									userId,
									tweetId,
									entity.getField("_type"),
									entity.getField("_typeReference"),
									entity.getField("name"),
									uri,
									(entity.getField("relevance") != null ? entity.getField("relevance") : 0.0),
									creationTime
								};
								su_NewsEntity.update(newsEntity);
							}
						}
					}catch (Exception e) {
						System.err.println("Problems while storing tweet entity assignments: " + e.getMessage());
					}
					
					//store topics:
					try{
						if(response.getTopics() != null){
							for (CalaisObject topic : response.getTopics()) {
								Object[] newsTopic = {
										userId,
										tweetId,
										topic.getField("categoryName"),
										topic.getField("category"),
										(topic.getField("score") != null ? topic.getField("score") : 0.0),
										creationTime
									};
									su_NewsTopic.update(newsTopic);
							}
						}
					}catch (Exception e) {
						System.err.println("Problems while stroing tweet topic assignments: " + e.getMessage());
					}
					
					if(count % storeAfter == 0){
						su_NewsEntity.flush();
						su_NewsTopic.flush();
						System.out.println("\n*********************\n* Processed " + count + " tweets.\n**********************\n");
					}
				}catch (Exception e) {
					e.printStackTrace();
					if(e.getMessage() != null && e.getMessage().contains("returned a response status of 403") && e.getMessage().toLowerCase().contains("response")){
						switchAPIKey();
						client = new CalaisRestClient(API_KEY);
						System.out.println("####\n#### New API key: " + API_KEY);
					}
				}
			}
			
			//finally flush remaining entries:
			su_NewsEntity.flush();
			su_NewsTopic.flush();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Approach the OpenCalais Web service to let OpenCalais parse the 
	 * content of tweets that are returned by the given query. Not limited to a single user.
	 * @param query Format changed: Nov. 5th, 2012, SELECT id, content, publish_date, userId FROM tweets WHERE ... 
	 */
	@SuppressWarnings("unchecked")
	public static void getOpenCalaisSemanticsForExternalResourcesAndStoreInDB(String query){
		ResultSet externalResources = null;

		System.out.println("Querying for externalResources...");
		System.out.println(query);
		externalResources = DBUtility.executeQuery(query);
		try{
			String content = null;
			Long tweetId = null;
			String uri = null;
			Timestamp publish_date = null;
			Map<String, Object> entityAttributes = null;
			
			CalaisClient client = new CalaisRestClient(API_KEY);
		    CalaisResponse response = null;
			
			BatchSqlUpdate su_NewsEntity = new BatchSqlUpdate(
					DBUtility.ds,
					"INSERT IGNORE INTO model_semantics_enriched_oc_entity_tweets2011 (tweetId, type, typeURI, name, uri, relevance, publish_date) "
							+ " values (?,?,?,?,?,?,?)");
			su_NewsEntity.declareParameter(new SqlParameter("tweetId", Types.BIGINT)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("type", Types.VARCHAR)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("typeURI", Types.VARCHAR)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("name", Types.VARCHAR)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("uri", Types.VARCHAR)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("relevance", Types.DOUBLE)); //$NON-NLS-1$
			su_NewsEntity.declareParameter(new SqlParameter("publish_date", Types.TIMESTAMP)); //$NON-NLS-1$
			
			BatchSqlUpdate su_NewsTopic = new BatchSqlUpdate(
					DBUtility.ds,
					"INSERT IGNORE INTO model_semantics_enriched_oc_topic_tweets2011 (tweetId, topic, uri, relevance, publish_date) "
							+ " values (?,?,?,?,?)");
			su_NewsTopic.declareParameter(new SqlParameter("tweetId", Types.BIGINT)); //$NON-NLS-1$
			su_NewsTopic.declareParameter(new SqlParameter("topic", Types.VARCHAR)); //$NON-NLS-1$
			su_NewsTopic.declareParameter(new SqlParameter("uri", Types.VARCHAR)); //$NON-NLS-1$
			su_NewsTopic.declareParameter(new SqlParameter("relevance", Types.DOUBLE)); //$NON-NLS-1$
			su_NewsTopic.declareParameter(new SqlParameter("publish_date", Types.TIMESTAMP)); //$NON-NLS-1$
			
			//iterate over all news to make OpenCalais Web Service Call and store the entities, etc.
			int count = 0; int storeAfter = 50;
			while (externalResources.next()) {
				try{
					count++;
					content = externalResources.getString("newscontent");
					tweetId = externalResources.getLong("tweetId");
					publish_date = externalResources.getTimestamp("publish_date");
					
					System.out.println("Processing Tweet " + tweetId);
					
					//make OpenCalais Web service call:
					response = client.analyze(content);
					
					//store entities:
					try{
						if(response.getEntities() != null){
							for (CalaisObject entity : response.getEntities()) {
								//hashed URI:
								uri = entity.getField("_uri");
								if(entity.getList("resolutions") != null){
									entityAttributes = (Map<String, Object>) entity.getList("resolutions").iterator().next();
									if(entityAttributes != null && entityAttributes.containsKey("id")){
										//overwrite URI with resolvable URI:
										uri = entityAttributes.get("id").toString();
									}
								}
								
								//store news entity assignment:
								Object[] newsEntity = {
									tweetId,
									entity.getField("_type"),
									entity.getField("_typeReference"),
									entity.getField("name"),
									uri,
									(entity.getField("relevance") != null ? entity.getField("relevance") : 0.0),
									publish_date
								};
								su_NewsEntity.update(newsEntity);
							}
						}
					}catch (Exception e) {
						System.err.println("Problems while storing tweet entity assignments: " + e.getMessage());
					}
					
					//store topics:
					try{
						if(response.getTopics() != null){
							for (CalaisObject topic : response.getTopics()) {
								Object[] newsTopic = {
										tweetId,
										topic.getField("categoryName"),
										topic.getField("category"),
										(topic.getField("score") != null ? topic.getField("score") : 0.0),
										publish_date
									};
									su_NewsTopic.update(newsTopic);
							}
						}
					}catch (Exception e) {
						System.err.println("Problems while stroing tweet topic assignments: " + e.getMessage());
					}
					
					if(count % storeAfter == 0){
						su_NewsEntity.flush();
						su_NewsTopic.flush();
						System.out.println("\n*********************\n* Processed " + count + " tweets.\n**********************\n");
					}
				}catch (Exception e) {
					e.printStackTrace();
					if(e.getMessage() != null && e.getMessage().contains("returned a response status of 403") && e.getMessage().toLowerCase().contains("response")){
						switchAPIKey();
						client = new CalaisRestClient(API_KEY);
						System.out.println("####\n#### New API key: " + API_KEY);
					}
				}
			}
			
			//finally flush remaining entries:
			su_NewsEntity.flush();
			su_NewsTopic.flush();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void testOpenCalais() {
		CalaisClient client = new CalaisRestClient(API_KEY);
	    CalaisResponse response = null;
	    
	    try {
			response = client.analyze("Delft University is actually Delft University of Technology in the Netherlands.");
			for (CalaisObject co : response.getEntities()) {
				// debugging output...
				System.out.println(co);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}
}
