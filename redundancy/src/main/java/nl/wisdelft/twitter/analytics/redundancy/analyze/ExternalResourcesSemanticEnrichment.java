/**
 * This source file is part of the work done in Web Information Systems group
 * at Delft University of Technology (TU Delft). Please contact the author via 
 * Email before reusing and distributing this work.
 * 
 * Copyright � 2012
 * Filename: ExternalResourcesSemanticEnrichment.java
 * Author: Ke Tao <k.tao (at) tudelft.nl>
 * Date created: Oct 22, 2012
 */
package nl.wisdelft.twitter.analytics.redundancy.analyze;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;

import nl.wisdelft.twitter.analytics.redundancy.analyze.SpotlightCrawler.SpotlightEntity;
import nl.wisdelft.twitter.io.DBUtility;

import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;
import org.springframework.jdbc.object.SqlUpdate;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

/**
 * @author ktao
 *
 */
public class ExternalResourcesSemanticEnrichment {
	private static final boolean USING_OFFICIAL_API = true;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}
	
	/**
	 * 
	 * @param args
	 */
	public static void wikipediaMinderSemanticEnrichment(String args[]) {
		
		
		SqlUpdate sut = new SqlUpdate(
				DBUtility.ds,
				"INSERT INTO www2013_enriched_semantics_wikipedia_miner (tweetId, title, weight, confidence, probability, timestamp) " +
				"VALUES(?, ?, ?, ?, ?, ?, ?)");
		sut.declareParameter(new SqlParameter("tweetId", Types.BIGINT));
		sut.declareParameter(new SqlParameter("title", Types.VARCHAR));
		sut.declareParameter(new SqlParameter("weight", Types.DOUBLE));
		sut.declareParameter(new SqlParameter("confidence", Types.DOUBLE));
		sut.declareParameter(new SqlParameter("probability", Types.DOUBLE));
		sut.declareParameter(new SqlParameter("timestamp", Types.VARCHAR));
//		sut.declareParameter(new SqlParameter("score", Types.DOUBLE));
		sut.compile();
		
		long pointer = Long.parseLong(args[0]);
//		long pointer = 0L;
		
		int counter = 0;
		do {
			counter = 0;
			String qExtResrc = "SELECT id, tweetId, userId, newscontent, publish_date FROM externalResources_www2013 WHERE id > " + pointer + " ORDER BY id ASC LIMIT 1000" ;
//			String qExtResrc = "SELECT e.id, e.tweetId, e.userId, e.newscontent, e.publish_date " +
//					"FROM externalResources_www2013 e, qrel_original q " +
//					"WHERE e.id > " + pointer + " AND e.tweetId = q.tweetId AND q.topicId = 22 ORDER BY id ASC LIMIT 1000" ;
	
			ResultSet extResrc = DBUtility.executePrepareQuerySingleConnection(qExtResrc);
			
			try {
				while (extResrc.next()) {
					counter++;
					
					long tweetId = extResrc.getLong("tweetId");
					int userId = extResrc.getInt("userId");
					String content = extResrc.getString("newscontent");
					Timestamp creationTime = extResrc.getTimestamp("publish_date");
					
					HashMap<String, SpotlightEntity> entities = null;
					try {
						entities = SpotlightCrawler.annotate(content, 0.2, 20, USING_OFFICIAL_API);
					} catch (Exception e) {
						pointer = extResrc.getLong("id");
						System.err.println("Processing Error @ tweetId : " + tweetId);			
						System.out.println(counter + " tweets processed.");
						continue;
					}
					
					for (String annotatedText : entities.keySet()) {
						SpotlightEntity entity = entities.get(annotatedText);
						
						URI DBpediaURI = entity.getResource();
						double score = entity.getScore();
						String types = entity.getType();
//						System.out.println("Types:" + types);
						
						sut.update(new Object[] {
								tweetId, 
								userId, 
								creationTime, 
								types, 
								DBpediaURI.toString(), 
								annotatedText, 
								score
						});
					}
					
					System.out.println("Tweet " + tweetId + " processed.");
					
					if (counter % 10 == 0) {
						pointer = extResrc.getLong("id");
						System.out.println(counter + " tweets processed.");
					}
				}
			
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} while (counter == 1000);
	}
	
	public static void DBpediaSemanticEnrichment(String args[]) {
		
		
		SqlUpdate sut = new SqlUpdate(
				DBUtility.ds,
				"INSERT INTO dbpediaEntityExternalResources_www2013 (tweetId, userId, creationTime, types, DBpediaURI, annotatedText, score) " +
				"VALUES(?, ?, ?, ?, ?, ?, ?)");
		sut.declareParameter(new SqlParameter("tweetId", Types.BIGINT));
		sut.declareParameter(new SqlParameter("userId", Types.INTEGER));
		sut.declareParameter(new SqlParameter("creationTime", Types.TIMESTAMP));
		sut.declareParameter(new SqlParameter("types", Types.VARCHAR));
		sut.declareParameter(new SqlParameter("DBpediaURI", Types.VARCHAR));
		sut.declareParameter(new SqlParameter("annotatedText", Types.VARCHAR));
		sut.declareParameter(new SqlParameter("score", Types.DOUBLE));
		sut.compile();
		
		long pointer = Long.parseLong(args[0]);
//		long pointer = 0L;
		
		int counter = 0;
		do {
			counter = 0;
			String qExtResrc = "SELECT id, tweetId, userId, newscontent, publish_date FROM externalResources_www2013 WHERE id > " + pointer + " ORDER BY id ASC LIMIT 1000" ;
//			String qExtResrc = "SELECT e.id, e.tweetId, e.userId, e.newscontent, e.publish_date " +
//					"FROM externalResources_www2013 e, qrel_original q " +
//					"WHERE e.id > " + pointer + " AND e.tweetId = q.tweetId AND q.topicId = 22 ORDER BY id ASC LIMIT 1000" ;
	
			ResultSet extResrc = DBUtility.executePrepareQuerySingleConnection(qExtResrc);
			
			try {
				while (extResrc.next()) {
					counter++;
					
					long tweetId = extResrc.getLong("tweetId");
					int userId = extResrc.getInt("userId");
					String content = extResrc.getString("newscontent");
					Timestamp creationTime = extResrc.getTimestamp("publish_date");
					
					HashMap<String, SpotlightEntity> entities = null;
					try {
						entities = SpotlightCrawler.annotate(content, 0.2, 20, USING_OFFICIAL_API);
					} catch (Exception e) {
						pointer = extResrc.getLong("id");
						System.err.println("Processing Error @ tweetId : " + tweetId);			
						System.out.println(counter + " tweets processed.");
						continue;
					}
					
					for (String annotatedText : entities.keySet()) {
						SpotlightEntity entity = entities.get(annotatedText);
						
						URI DBpediaURI = entity.getResource();
						double score = entity.getScore();
						String types = entity.getType();
//						System.out.println("Types:" + types);
						
						sut.update(new Object[] {
								tweetId, 
								userId, 
								creationTime, 
								types, 
								DBpediaURI.toString(), 
								annotatedText, 
								score
						});
					}
					
					System.out.println("Tweet " + tweetId + " processed.");
					
					if (counter % 10 == 0) {
						pointer = extResrc.getLong("id");
						System.out.println(counter + " tweets processed.");
					}
				}
			
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} while (counter == 1000);
	}
	
	/**
	 * 
	 */
	public String extractMainContent(String urlString) {
		try {
			URL url = new URL(urlString);
			
			String text = ArticleExtractor.INSTANCE.getText(url);
						
			return text;
		} catch (MalformedURLException e) {
			System.err.println("Malformed URL given.");
		} catch (BoilerpipeProcessingException e) {
			System.err.println("");
		}
		
		return null;
	}
}
