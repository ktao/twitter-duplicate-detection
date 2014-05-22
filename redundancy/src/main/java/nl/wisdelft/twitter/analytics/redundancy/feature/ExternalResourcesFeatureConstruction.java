/*********************************************************
*  Copyright (c) 2011 by Web Information Systems (WIS) Group.
*  Ke Tao, http://taubau.info/
*
*  Some rights reserved.
*
*  Contact: http://www.wis.ewi.tudelft.nl/
*
**********************************************************/
package nl.wisdelft.twitter.analytics.redundancy.feature;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;

import nl.wisdelft.twitter.io.DBUtility;

import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

/**
 * 
 * @author Ke Tao, <a href="mailto:k.tao@tudelft.nl">k.tao@tudelft.nl</a>
 * @author last edited by: ktao
 * 
 * @version created on Nov 6, 2012
 */
public class ExternalResourcesFeatureConstruction {

	public static void constructExternalResourcesFeatures(int items2skip) {
		constructExternalResourcesFeatures(items2skip, new boolean[] {
			true,
			true,
			true,
			true,
			true,
			true,
			true,
			true
			
		});
	}
	
	public static void constructExternalResourcesFeatures(int items2skip, boolean[] features) {
		if (features.length != 8) {
			System.out.println("Parameter error, 8 boolean values needed.");
			return;
		}
		
		String query = "UPDATE www2013_duplicate_detection_arff " +
				"SET " +
				(features[0] ? "overlapEnrichedDBpediaEntities = ?, " : " ") +
				(features[1] ? "overlapEnrichedDBpediaEntityTypes = ?, " : " ") +
				(features[2] ? "overlapEnrichedOpenCalaisEntities = ?, " : " ") +
				(features[3] ? "overlapEnrichedOpenCalaisTypes = ?, " : " ") +
				(features[4] ? "overlapEnrichedOpenCalaisTopic = ?, " : " ") +
				(features[5] ? "overlapEnrichedWordNetConcepts = ?, " : " ") +
				(features[6] ? "overlapEnrichedWordNetSynsetConcepts = ?, " : " ") +
				(features[7] ? "enrichedWordNetSimilarity = ?, " : " ") +
				"topicId = topicId " +
				" WHERE tweetIdA = ? AND tweetIdB = ?";
		BatchSqlUpdate su = new BatchSqlUpdate(DBUtility.ds, query);
		
		System.out.println(query);
		
		int i = 0;
		if (features[0]) {
			su.declareParameter(new SqlParameter("overlapEnrichedDBpediaEntities", Types.DOUBLE));
			i++;
		}
		if (features[1]) {
			su.declareParameter(new SqlParameter("overlapEnrichedDBpediaEntityTypes", Types.DOUBLE));
			i++;
		}
		if (features[2]) {
			su.declareParameter(new SqlParameter("overlapEnrichedOpenCalaisEntities", Types.DOUBLE));
			i++;
		}
		if (features[3]) {
			su.declareParameter(new SqlParameter("overlapEnrichedOpenCalaisTypes", Types.DOUBLE));
			i++;
		}
		if (features[4]) {
			su.declareParameter(new SqlParameter("overlapEnrichedOpenCalaisTopic", Types.DOUBLE));
			i++;
		}
		if (features[5]) {
			su.declareParameter(new SqlParameter("overlapEnrichedWordNetConcepts", Types.DOUBLE));
			i++;
		}
		if (features[6]) {
			su.declareParameter(new SqlParameter("overlapEnrichedWordNetSynsetConcepts", Types.DOUBLE));
			i++;
		}
		if (features[7]) {
			su.declareParameter(new SqlParameter("enrichedWordNetSimilarity", Types.DOUBLE));
			i++;
		}
		su.declareParameter(new SqlParameter("tweetIdA", Types.BIGINT));
		su.declareParameter(new SqlParameter("tweetIdB", Types.BIGINT));
		su.compile();
		
		// syntactical features
		ResultSet tweets = DBUtility.executeQuerySingleConnection("SELECT i.tweetIdA, ta.content as contentA, i.tweetIdB, tb.content as contentB " +
				"FROM www2013_duplicate_detection_arff i, tweets_www2013 ta, tweets_www2013 tb " +
				"WHERE i.tweetIdA = ta.id AND i.tweetIdB = tb.id " + 
				"ORDER BY i.tweetIdA ASC, i.tweetIdB ASC");
		
		int count = 0;
		
		try {
			while (tweets.next()) {
				count++;
				if (count <= items2skip) {
					if (count % 1000 == 0)
						System.out.println(count + " items skipped!");
					continue;
				}
//				String tContentA = tweets.getString("contentA");
//				String tContentB = tweets.getString("contentB");
				long tweetIdA = tweets.getLong("tweetIdA");
				long tweetIdB = tweets.getLong("tweetIdB");
				
				System.out.println("Processing tweet pair (" + tweetIdA + ", " + tweetIdB + ")");
//				long timeA = System.currentTimeMillis();
				
				// Feature External Resources 1 = overlapEnrichedDBpediaEntities
				double overlapEnrichedDBpediaEntities = -1;
				if (features[0])
					overlapEnrichedDBpediaEntities = overlapEnrichedDBpediaEntities(tweetIdA, tweetIdB);
				
//				long timeB = System.currentTimeMillis();
//				System.out.println("Time:" + (timeB - timeA) + "ms.");
//				timeA = timeB;
				
				// Feature External Resources 2 = overlapEnrichedDBpediaEntityTypes
				double overlapEnrichedDBpediaEntityTypes = -1;
				if (features[1])
					overlapEnrichedDBpediaEntityTypes = overlapEnrichedDBpediaEntityTypes(tweetIdA, tweetIdB);
				
//				timeB = System.currentTimeMillis();
//				System.out.println("Time:" + (timeB - timeA) + "ms.");
//				timeA = timeB;
				
				// Feature External Resources 3 = overlapEnrichedOpenCalaisEntities
				double overlapEnrichedOpenCalaisEntities = -1;
				if (features[2])
					overlapEnrichedOpenCalaisEntities = overlapEnrichedOpenCalaisEntities(tweetIdA, tweetIdB);
				
//				timeB = System.currentTimeMillis();
//				System.out.println("Time:" + (timeB - timeA) + "ms.");
//				timeA = timeB;
				
				// Feature External Resources 4 = overlapEnrichedDBpediaEntityTypes
				double overlapEnrichedOpenCalaisTypes = -1;
				if (features[3])
					overlapEnrichedOpenCalaisTypes = overlapEnrichedOpenCalaisTypes(tweetIdA, tweetIdB);
				
//				timeB = System.currentTimeMillis();
//				System.out.println("Time:" + (timeB - timeA) + "ms.");
//				timeA = timeB;
				
				// Feature External Resources 5 = sameEnrichedOpenCalaisTopic
				double overlapEnrichedOpenCalaisTopic = -1;
				if (features[4])
					overlapEnrichedOpenCalaisTopic = overlapEnrichedOpenCalaisTopic(tweetIdA, tweetIdB);
				
//				timeB = System.currentTimeMillis();
//				System.out.println("Time:" + (timeB - timeA) + "ms.");
//				timeA = timeB;
				
				double overlapEnrichedWordNetConcepts = -1;
				if (features[5])
					overlapEnrichedWordNetConcepts = overlapEnrichedWordNetConcepts(tweetIdA, tweetIdB);
				
				double overlapEnrichedWordNetSynsetConcepts = -1;
				if (features[6])
					overlapEnrichedWordNetSynsetConcepts = overlapEnrichedWordNetSynsetConcepts(tweetIdA, tweetIdB);
				
				// Feature External Resources 6 = userSimilarity
				double enrichedWordNetSimilarity = -1;
				if (features[7])
					enrichedWordNetSimilarity = enrichedWordNetSimilarity(tweetIdA, tweetIdB);
				
//				timeB = System.currentTimeMillis();
//				System.out.println("Time:" + (timeB - timeA) + "ms.");
				
				
				
				Object[] entry = new Object[i + 2];
				
				int k = 0;
				if (features[0]) {
					entry[k] = overlapEnrichedDBpediaEntities;
					k++;
				}
				if (features[1]) {
					entry[k] = overlapEnrichedDBpediaEntityTypes;
					k++;
				}
				if (features[2]) {
					entry[k] = overlapEnrichedOpenCalaisEntities;
					k++;
				}
				if (features[3]) {
					entry[k] = overlapEnrichedOpenCalaisTypes;
					k++;
				}
				if (features[4]) {
					entry[k] = overlapEnrichedOpenCalaisTopic;
					k++;
				}
				if (features[5]) {
					entry[k] = overlapEnrichedWordNetConcepts;
					k++;
				}
				if (features[6]) {
					entry[k] = overlapEnrichedWordNetSynsetConcepts;
					k++;
				}
				if (features[7]) {
					entry[k] = enrichedWordNetSimilarity;
					k++;
				}
				
				entry[k] = tweetIdA;
				entry[k + 1] = tweetIdB;
				
				su.update(entry);
				
				if (count % 100 == 0) {
					System.out.println(count + " pairs updated!");
					System.out.println(su.flush().length + " rows affected.");
				}
			}
			System.out.println(su.flush().length + " rows affected.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static double overlapEnrichedWordNetSynsetConcepts(long tweetIdA,
			long tweetIdB) {
		String contentA = "";
		ResultSet erA = DBUtility.executeQuerySingleConnection("SELECT content FROM tweets_www2013 WHERE id = " + tweetIdA);
		
		try {
			if(erA.next())
				contentA += erA.getString("content") + "\n";
		} catch (SQLException e) {
			e.printStackTrace();
		}
			
		erA = DBUtility.executeQuerySingleConnection("SELECT newscontent FROM externalResources_www2013 WHERE tweetId = " + tweetIdA);
		try {
			while (erA.next()) {
				contentA += erA.getString("newscontent") + "\n";
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		String contentB = "";
		ResultSet erB = DBUtility.executeQuerySingleConnection("SELECT content FROM tweets_www2013 WHERE id = " + tweetIdB);
		
		try {
			if(erB.next())
				contentB += erB.getString("content") + "\n";
		} catch (SQLException e) {
			e.printStackTrace();
		}
			
		erB = DBUtility.executeQuerySingleConnection("SELECT newscontent FROM externalResources_www2013 WHERE tweetId = " + tweetIdB);
		try {
			while (erB.next()) {
				contentB += erB.getString("newscontent") + "\n";
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return WordNetRelatedSimilarity.getInstance().getWordNetSimilarityBasedOnSynsets(contentA, contentB);
	}

	public static double overlapEnrichedWordNetConcepts(long tweetIdA,
			long tweetIdB) {
		String contentA = "";
		ResultSet erA = DBUtility.executeQuerySingleConnection("SELECT content FROM tweets_www2013 WHERE id = " + tweetIdA);
		
		try {
			if(erA.next())
				contentA += erA.getString("content") + "\n";
		} catch (SQLException e) {
			e.printStackTrace();
		}
			
		erA = DBUtility.executeQuerySingleConnection("SELECT newscontent FROM externalResources_www2013 WHERE tweetId = " + tweetIdA);
		try {
			while (erA.next()) {
				contentA += erA.getString("newscontent") + "\n";
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		String contentB = "";
		ResultSet erB = DBUtility.executeQuerySingleConnection("SELECT content FROM tweets_www2013 WHERE id = " + tweetIdB);
		
		try {
			if(erB.next())
				contentB += erB.getString("content") + "\n";
		} catch (SQLException e) {
			e.printStackTrace();
		}
			
		erB = DBUtility.executeQuerySingleConnection("SELECT newscontent FROM externalResources_www2013 WHERE tweetId = " + tweetIdB);
		try {
			while (erB.next()) {
				contentB += erB.getString("newscontent") + "\n";
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return WordNetRelatedSimilarity.getInstance().getWordNetSimilarityBasedOnWords(contentA, contentB);
	}

	public static double enrichedWordNetSimilarity(long tweetIdA, long tweetIdB) {
		String contentA = "";
		ResultSet erA = DBUtility.executeQuerySingleConnection("SELECT content FROM tweets_www2013 WHERE id = " + tweetIdA);
		
		try {
			if(erA.next())
				contentA += erA.getString("content") + "\n";
		} catch (SQLException e) {
			e.printStackTrace();
		}
			
		erA = DBUtility.executeQuerySingleConnection("SELECT newscontent FROM externalResources_www2013 WHERE tweetId = " + tweetIdA);
		try {
			while (erA.next()) {
				contentA += erA.getString("newscontent");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		String contentB = "";
		ResultSet erB = DBUtility.executeQuerySingleConnection("SELECT content FROM tweets_www2013 WHERE id = " + tweetIdB);
		
		try {
			if(erB.next())
				contentB += erB.getString("content") + "\n";
		} catch (SQLException e) {
			e.printStackTrace();
		}
			
		erB = DBUtility.executeQuerySingleConnection("SELECT newscontent FROM externalResources_www2013 WHERE tweetId = " + tweetIdB);
		try {
			while (erB.next()) {
				contentB += erB.getString("newscontent");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return WordNetRelatedSimilarity.getInstance().getWordNetSimilarityBasedOnWS4J(contentA, contentB, WordNetRelatedSimilarity.RCS_LIN);
	}

	public static double overlapEnrichedOpenCalaisTopic(long tweetIdA,
			long tweetIdB) {
		ResultSet rsTopicsA = DBUtility.executeQuerySingleConnection("SELECT uri FROM semanticsTweetsTopic_www2013 WHERE tweetId = " + tweetIdA);
		HashSet<String> topicsA = new HashSet<String>();
		try {
			while (rsTopicsA.next()) {
				topicsA.add(rsTopicsA.getString("uri"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		rsTopicsA = DBUtility.executeQuerySingleConnection("SELECT uri FROM semanticsExternalResourcesTopic_www2013 WHERE tweetId = " + tweetIdA);
		
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
		
		rsTopicsB = DBUtility.executeQuerySingleConnection("SELECT uri FROM semanticsExternalResourcesTopic_www2013 WHERE tweetId = " + tweetIdB);
		
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

	public static double overlapEnrichedOpenCalaisTypes(long tweetIdA,
			long tweetIdB) {
		ResultSet rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT typeURI FROM semanticsTweetsEntity_www2013 WHERE type != 'URL' AND tweetId = " + tweetIdA);
		HashSet<String> entitiesA = new HashSet<String>();
		try {
			while (rsEntitiesA.next()) {
				entitiesA.add(rsEntitiesA.getString("typeURI"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT typeURI FROM semanticsExternalResourcesEntity_www2013 WHERE type != 'URL' AND tweetId = " + tweetIdA);
		
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
		
		DBUtility.executeQuerySingleConnection("SELECT typeURI FROM semanticsExternalResourcesEntity_www2013 WHERE type != 'URL' AND tweetId = " + tweetIdB);
		
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

	public static double overlapEnrichedOpenCalaisEntities(long tweetIdA,
			long tweetIdB) {
		ResultSet rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT uri FROM semanticsTweetsEntity_www2013 WHERE type != 'URL' AND tweetId = " + tweetIdA);
		HashSet<String> entitiesA = new HashSet<String>();
		try {
			while (rsEntitiesA.next()) {
				entitiesA.add(rsEntitiesA.getString("uri"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT uri FROM semanticsExternalResourcesEntity_www2013 WHERE type != 'URL' AND tweetId = " + tweetIdA);
		
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
		
		rsEntitiesB = DBUtility.executeQuerySingleConnection("SELECT uri FROM semanticsExternalResourcesEntity_www2013 WHERE type != 'URL' AND tweetId = " + tweetIdB);
		
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

	public static double overlapEnrichedDBpediaEntityTypes(long tweetIdA,
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
		
		rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT types FROM dbpediaEntityExternalResources_www2013 WHERE tweetId = " + tweetIdA);
		
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
		
		rsEntitiesB = DBUtility.executeQuerySingleConnection("SELECT types FROM dbpediaEntityExternalResources_www2013 WHERE tweetId = " + tweetIdB);
		
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

	public static double overlapEnrichedDBpediaEntities(long tweetIdA,
			long tweetIdB) {
		ResultSet rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT DBpediaURI FROM dbpediaEntity_www2013 WHERE tweetId = " + tweetIdA);
		HashSet<String> entitiesA = new HashSet<String>();
		try {
			while (rsEntitiesA.next()) {
				entitiesA.add(rsEntitiesA.getString("DBpediaURI"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT DBpediaURI FROM dbpediaEntityExternalResources_www2013 WHERE tweetId = " + tweetIdA);
		
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
		
		rsEntitiesB = DBUtility.executeQuerySingleConnection("SELECT DBpediaURI FROM dbpediaEntityExternalResources_www2013 WHERE tweetId = " + tweetIdB);
		
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
	 * @param args
	 */
	public static void main(String[] args) {
//		int i2s = Integer.parseInt(args[0]);
//		constructExternalResourcesFeatures(i2s);
//		constructExternalResourcesFeatures(i2s, new boolean[] {false, false, false, false, false, true, true, false});
//		constructExternalResourcesFeatures();
		//System.out.println(enrichedWordNetSimilarity(28967095878287360L, 29012766593384448L));
		
		ResultSet rs = DBUtility.executeQuerySingleConnection("SELECT tweetIdA, tweetIdB, enrichedWordNetSimilarity " +
				"FROM www2013_duplicate_detection_arff " + 
				"ORDER BY tweetIdA ASC, tweetIdB ASC");
		
		try {
			while (rs.next()) {
				System.out.println("UPDATE www2013_duplicate_detection_arff SET enrichedWordNetSimilarity = " + rs.getDouble("enrichedWordNetSimilarity") + " " +
						"WHERE tweetIdA = " + rs.getLong("tweetIdA") + " AND tweetIdB = " + rs.getLong("tweetIdB") + ";");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static double overlapEnrichedWPMEntities(long tweetIdA,
			long tweetIdB) {
		ResultSet rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT title FROM model_semantics_tweets_wpm_tweets2011 WHERE tweetId = " + tweetIdA);
		HashSet<String> entitiesA = new HashSet<String>();
		try {
			while (rsEntitiesA.next()) {
				entitiesA.add(rsEntitiesA.getString("title"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		rsEntitiesA = DBUtility.executeQuerySingleConnection("SELECT title FROM model_semantics_enriched_wpm_tweets2011 WHERE tweetId = " + tweetIdA);
		
		try {
			while (rsEntitiesA.next()) {
				entitiesA.add(rsEntitiesA.getString("title"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ResultSet rsEntitiesB = DBUtility.executeQuerySingleConnection("SELECT title FROM model_semantics_tweets_wpm_tweets2011 WHERE tweetId = " + tweetIdB);
		HashSet<String> entitiesB = new HashSet<String>();
		try {
			while (rsEntitiesB.next()) {
				entitiesB.add(rsEntitiesB.getString("title"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		rsEntitiesB = DBUtility.executeQuerySingleConnection("SELECT title FROM model_semantics_enriched_wpm_tweets2011 WHERE tweetId = " + tweetIdB);
		
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
}
