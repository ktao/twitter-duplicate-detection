/**
 * This source file is part of the work done in Web Information Systems group
 * at Delft University of Technology (TU Delft). Please contact the author via 
 * Email before reusing and distributing this work.
 * 
 * Copyright � 2012
 * Filename: SemanticFeatureConstruction.java
 * Author: Ke Tao <k.tao (at) tudelft.nl>
 * Date created: Nov 6, 2012
 */
package nl.wisdelft.twitter.analytics.redundancy.feature;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;

import nl.wisdelft.twitter.io.DBUtility;

import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

/**
 * @author ktao
 *
 */
public class SemanticFeatureConstruction {

	public static void constructSemanticFeatures() {
		BatchSqlUpdate su = new BatchSqlUpdate(DBUtility.ds, "UPDATE www2013_duplicate_detection_arff " +
				"SET overlapDBpediaEntities = ?, overlapDBpediaEntityTypes = ?, overlapOpenCalaisEntities = ?, overlapOpenCalaisTypes = ?, " +
				"overlapOpenCalaisTopic = ?, overlapWordNetConcepts = ?, overlapWordNetSynsetConcepts = ?, wordNetSimilarity = ? " + // 
				"WHERE tweetIdA = ? AND tweetIdB = ?");
		su.declareParameter(new SqlParameter("overlapDBpediaEntities", Types.DOUBLE));
		su.declareParameter(new SqlParameter("overlapDBpediaEntityTypes", Types.DOUBLE));
		su.declareParameter(new SqlParameter("overlapOpenCalaisEntities", Types.DOUBLE));
		su.declareParameter(new SqlParameter("overlapOpenCalaisTypes", Types.DOUBLE));
		su.declareParameter(new SqlParameter("overlapOpenCalaisTopic", Types.DOUBLE));
		su.declareParameter(new SqlParameter("overlapWordNetConcepts", Types.DOUBLE));
		su.declareParameter(new SqlParameter("overlapWordNetSynsetConcepts", Types.DOUBLE));
		su.declareParameter(new SqlParameter("wordNetSimilarity", Types.DOUBLE));
		su.declareParameter(new SqlParameter("tweetIdA", Types.BIGINT));
		su.declareParameter(new SqlParameter("tweetIdB", Types.BIGINT));
		su.compile();
		
		// syntactical features
		String query = "SELECT i.tweetIdA, ta.content as contentA, i.tweetIdB, tb.content as contentB " +
				"FROM www2013_duplicate_detection_arff i, tweets ta, tweets tb " +
				"WHERE i.tweetIdA = ta.id AND i.tweetIdB = tb.id " +
				"ORDER BY i.tweetIdA ASC, i.tweetIdB ASC ";
		
		System.out.println(query);
		ResultSet tweets = DBUtility.executeQuerySingleConnection(query);
		
		int count = 0;
		
		try {
			while (tweets.next()) {
				count++;
				String tContentA = tweets.getString("contentA");
				String tContentB = tweets.getString("contentB");
				long tweetIdA = tweets.getLong("tweetIdA");
				long tweetIdB = tweets.getLong("tweetIdB");
				
				// Feature Semantics 1 = overlapDBpediaEntities
				double overlapDBpediaEntities = overlapDBpediaEntities(tweetIdA, tweetIdB);
				
				// Feature Semantics 2 = overlapDBpediaEntityTypes
				double overlapDBpediaEntityTypes = overlapDBpediaEntityTypes(tweetIdA, tweetIdB);
				
				// Feature semantic 3 = overlapOpenCalaisEntities
				double overlapOpenCalaisEntities = overlapOpenCalaisEntities(tweetIdA, tweetIdB);
				
				// Feature semantic 4 = overlapOpenCalaisTypes
				double overlapOpenCalaisTypes = overlapOpenCalaisTypes(tweetIdA, tweetIdB);
				
				// Feature semantic 5 = sameOpenCalaisTopic
				double overlapOpenCalaisTopic = overlapOpenCalaisTopic(tweetIdA, tweetIdB);
				
				// Feature semantic 6 = overlapWordNetConcepts
				double overlapWordNetConcepts = overlapWordNetConcepts(tContentA, tContentB);
				
				// Feature semantic 7 = overlapWordNetSynsetConcepts
				double overlapWordNetSynsetConcepts = overlapWordNetSynsetConcepts(tContentA, tContentB);
				
				// Feature semantic 8 = wordNetSimilarity
				double wordNetSimilarity = wordNetSimilarity(tContentA, tContentB);
				
//				if (overlapDBpediaEntities == Double.NaN || overlapDBpediaEntityTypes == Double.NaN || 
//						overlapOpenCalaisEntities == Double.NaN || overlapOpenCalaisTypes == Double.NaN ||
//						overlapOpenCalaisTopic == Double.NaN || overlapWordNetConcepts == Double.NaN ||
//						overlapWordNetSynsetConcepts == Double.NaN || wordNetSimilarity == Double.NaN) {
//					System.out.println("TweetIdA:" + tweetIdA);
//					System.out.println("TweetIdB:" + tweetIdB);
//					System.out.println("tContentA:" + tContentA);
//					System.out.println("tContentA:" + tContentB);
//				}
				
				su.update(new Object[]{
					overlapDBpediaEntities,
					overlapDBpediaEntityTypes,
					overlapOpenCalaisEntities,
					overlapOpenCalaisTypes,
					overlapOpenCalaisTopic,
					overlapWordNetConcepts,
					overlapWordNetSynsetConcepts,
					wordNetSimilarity,
					tweetIdA,
					tweetIdB
				});
				
				if (count % 100 == 0) {
					su.flush();
					System.out.println(count + " pairs updated!");
				}
			}
			su.flush();
			System.out.println(count + " pairs updated! Finished!");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static double wordNetSimilarity(String tContentA, String tContentB) {
		return WordNetRelatedSimilarity.getInstance().getWordNetSimilarityBasedOnWS4J(tContentA, tContentB, WordNetRelatedSimilarity.RCS_LIN);
	}

	public static double overlapWordNetSynsetConcepts(String tContentA,
			String tContentB) {
		return WordNetRelatedSimilarity.getInstance().getWordNetSimilarityBasedOnSynsets(tContentA, tContentB);
	}

	public static double overlapWordNetConcepts(String tContentA,
			String tContentB) {
		return WordNetRelatedSimilarity.getInstance().getWordNetSimilarityBasedOnWords(tContentA, tContentB);
	}

	public static double overlapOpenCalaisTopic(long tweetIdA, long tweetIdB) {
		ResultSet rsTopicsA = DBUtility.executeQuerySingleConnection("SELECT uri FROM semanticsTweetsTopic_www2013 WHERE tweetId = " + tweetIdA);
		HashSet<String> topicsA = new HashSet<String>();
		try {
			while (rsTopicsA.next()) {
				topicsA.add(rsTopicsA.getString("uri"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ResultSet rsTopicsB = DBUtility.executeQuerySingleConnection("SELECT uri FROM semanticsTweetsTopic_www2013 WHERE tweetId = " + tweetIdB);
		HashSet<String> topicsB = new HashSet<String>();
		try {
			while (rsTopicsB.next()) {
				topicsB.add(rsTopicsB.getString("uri"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (topicsA.size() * topicsB.size() == 0)
			return 0.0;
		
		int counter = 0;
		for (String topicA : topicsA) {
			if (topicsB.contains(topicA))
				counter++;
		}
		
		return (double) counter / (double) (topicsA.size() + topicsB.size() - counter);
	}

	public static double overlapOpenCalaisTypes(long tweetIdA, long tweetIdB) {
		ResultSet rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT typeURI FROM semanticsTweetsEntity_www2013 WHERE type != 'URL' AND tweetId = " + tweetIdA);
		HashSet<String> entitiesA = new HashSet<String>();
		try {
			while (rsEntitiesA.next()) {
				entitiesA.add(rsEntitiesA.getString("typeURI"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ResultSet rsEntitiesB = DBUtility.executeQuerySingleConnection("SELECT typeURI FROM semanticsTweetsEntity_www2013 WHERE type != 'URL' AND tweetId = " + tweetIdB);
		HashSet<String> entitiesB = new HashSet<String>();
		try {
			while (rsEntitiesB.next()) {
				entitiesB.add(rsEntitiesB.getString("typeURI"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (entitiesA.size() * entitiesB.size() == 0)
			return 0.0;
		
		int counter = 0;
		for (String entityA : entitiesA) {
			if (entitiesB.contains(entityA))
				counter++;
		}
		
		return (double) counter / (double) (entitiesA.size() + entitiesB.size() - counter);
	}

	public static double overlapOpenCalaisEntities(long tweetIdA, long tweetIdB) { // ignore URL
		ResultSet rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT uri FROM semanticsTweetsEntity_www2013 WHERE type != 'URL' AND tweetId = " + tweetIdA);
		HashSet<String> entitiesA = new HashSet<String>();
		try {
			while (rsEntitiesA.next()) {
				entitiesA.add(rsEntitiesA.getString("uri"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ResultSet rsEntitiesB = DBUtility.executeQuerySingleConnection("SELECT uri FROM semanticsTweetsEntity_www2013 WHERE type != 'URL' AND tweetId = " + tweetIdB);
		HashSet<String> entitiesB = new HashSet<String>();
		try {
			while (rsEntitiesB.next()) {
				entitiesB.add(rsEntitiesB.getString("uri"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (entitiesA.size() * entitiesB.size() == 0)
			return 0.0;
		
		int counter = 0;
		for (String entityA : entitiesA) {
			if (entitiesB.contains(entityA))
				counter++;
		}
		
		return (double) counter / (double) (entitiesA.size() + entitiesB.size() - counter);
	}

	public static double overlapDBpediaEntityTypes(long tweetIdA,
			long tweetIdB) {
		ResultSet rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT types FROM dbpediaEntity_www2013 WHERE tweetId = " + tweetIdA);
		HashSet<String> entitiesA = new HashSet<String>();
		try {
			while (rsEntitiesA.next()) {
				for (String type : rsEntitiesA.getString("types").split(",")) {
					entitiesA.add(type);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ResultSet rsEntitiesB = DBUtility.executeQuerySingleConnection("SELECT types FROM dbpediaEntity_www2013 WHERE tweetId = " + tweetIdB);
		HashSet<String> entitiesB = new HashSet<String>();
		try {
			while (rsEntitiesB.next()) {
				for (String type : rsEntitiesB.getString("types").split(",")) {
					entitiesB.add(type);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (entitiesA.size() * entitiesB.size() == 0)
			return 0.0;
		
		int counter = 0;
		for (String entityA : entitiesA) {
			if (entitiesB.contains(entityA))
				counter++;
		}
		
		return (double) counter / (double) (entitiesA.size() + entitiesB.size() - counter);
	}

	public static double overlapWikipediaMinerEntities(long tweetIdA, long tweetIdB) {
		ResultSet rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT title FROM www2013_semantics_wikipedia_miner WHERE tweetId = " + tweetIdA);
		HashSet<String> entitiesA = new HashSet<String>();
		try {
			while (rsEntitiesA.next()) {
				entitiesA.add(rsEntitiesA.getString("title"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ResultSet rsEntitiesB = DBUtility.executeQuerySingleConnection("SELECT title FROM www2013_semantics_wikipedia_miner WHERE tweetId = " + tweetIdB);
		HashSet<String> entitiesB = new HashSet<String>();
		try {
			while (rsEntitiesB.next()) {
				entitiesB.add(rsEntitiesB.getString("title"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (entitiesA.size() * entitiesB.size() == 0)
			return 0.0;
		
		int counter = 0;
		for (String entityA : entitiesA) {
			if (entitiesB.contains(entityA))
				counter++;
		}
		
		return (double) counter / (double) (entitiesA.size() + entitiesB.size() - counter);
	}
	
	public static double overlapDBpediaEntities(long tweetIdA, long tweetIdB) {
		ResultSet rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT DBpediaURI FROM dbpediaEntity_www2013 WHERE tweetId = " + tweetIdA);
		HashSet<String> entitiesA = new HashSet<String>();
		try {
			while (rsEntitiesA.next()) {
				entitiesA.add(rsEntitiesA.getString("DBpediaURI"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ResultSet rsEntitiesB = DBUtility.executeQuerySingleConnection("SELECT DBpediaURI FROM dbpediaEntity_www2013 WHERE tweetId = " + tweetIdB);
		HashSet<String> entitiesB = new HashSet<String>();
		try {
			while (rsEntitiesB.next()) {
				entitiesB.add(rsEntitiesB.getString("DBpediaURI"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (entitiesA.size() * entitiesB.size() == 0)
			return 0.0;
		
		int counter = 0;
		for (String entityA : entitiesA) {
			if (entitiesB.contains(entityA))
				counter++;
		}
		
		return (double) counter / (double) (entitiesA.size() + entitiesB.size() - counter);
	}
	
	/**
	 * For CIKM 2013 paper, construct the features based on wikipedia miner entities
	 * 
	 * The table to be updated: cikm2013_duplicate_detection_model_arff
	 * Tweets from: www2013_duplicate_detection_arff
	 */
	public static void constructWPMSemanticFeatures() {
		BatchSqlUpdate su = new BatchSqlUpdate(DBUtility.ds, "UPDATE cikm2013_duplicate_detection_model_arff " +
				"SET overlapWikipediaMinerEntities = ? " + // 
				"WHERE tweetIdA = ? AND tweetIdB = ?");
		su.declareParameter(new SqlParameter("overlapWikipediaMinerEntities", Types.DOUBLE));
		su.declareParameter(new SqlParameter("tweetIdA", Types.BIGINT));
		su.declareParameter(new SqlParameter("tweetIdB", Types.BIGINT));
		su.compile();
		
		// syntactical features
		String query = "SELECT i.tweetIdA, ta.content as contentA, i.tweetIdB, tb.content as contentB " +
				"FROM cikm2013_duplicate_detection_model_arff i, tweets_www2013 ta, tweets_www2013 tb " +
				"WHERE i.tweetIdA = ta.id AND i.tweetIdB = tb.id " +
				"ORDER BY i.tweetIdA ASC, i.tweetIdB ASC ";
		
		System.out.println(query);
		ResultSet tweets = DBUtility.executeQuerySingleConnection(query);
		
		int count = 0;
		
		try {
			while (tweets.next()) {
				count++;
//				String tContentA = tweets.getString("contentA");
//				String tContentB = tweets.getString("contentB");
				long tweetIdA = tweets.getLong("tweetIdA");
				long tweetIdB = tweets.getLong("tweetIdB");
				
				// Feature Semantics 1 = overlapDBpediaEntities
				double overlapWikipediaMinerEntities = overlapWikipediaMinerEntities(tweetIdA, tweetIdB);

				
//				if (overlapDBpediaEntities == Double.NaN || overlapDBpediaEntityTypes == Double.NaN || 
//						overlapOpenCalaisEntities == Double.NaN || overlapOpenCalaisTypes == Double.NaN ||
//						overlapOpenCalaisTopic == Double.NaN || overlapWordNetConcepts == Double.NaN ||
//						overlapWordNetSynsetConcepts == Double.NaN || wordNetSimilarity == Double.NaN) {
//					System.out.println("TweetIdA:" + tweetIdA);
//					System.out.println("TweetIdB:" + tweetIdB);
//					System.out.println("tContentA:" + tContentA);
//					System.out.println("tContentA:" + tContentB);
//				}
				
				su.update(new Object[]{
					overlapWikipediaMinerEntities,
					tweetIdA,
					tweetIdB
				});
				
				if (count % 100 == 0) {
					su.flush();
					System.out.println(count + " pairs updated!");
				}
			}
			su.flush();
			System.out.println(count + " pairs updated! Finished!");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		constructWPMSemanticFeatures();
	}

}
