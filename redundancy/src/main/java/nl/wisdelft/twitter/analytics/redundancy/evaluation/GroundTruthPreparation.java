/**
 * This source file is part of the work done in Web Information Systems group
 * at Delft University of Technology (TU Delft). Please contact the author via 
 * Email before reusing and distributing this work.
 * 
 * Copyright � 2012
 * Filename: GroundTruthPreparation.java
 * Author: Ke Tao <k.tao (at) tudelft.nl>
 * Date created: Oct 23, 2012
 */
package nl.wisdelft.twitter.analytics.redundancy.evaluation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.TreeMap;

import nl.wisdelft.twitter.io.DBUtility;

import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;
import org.springframework.jdbc.object.SqlUpdate;

/**
 * @author ktao
 *
 */
public class GroundTruthPreparation {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		positiveInstances();
		negativeInstantces();
	}
	
	public static void positiveInstances() {
		// positive iterate over all duplicate sets
		SqlUpdate su = new SqlUpdate(DBUtility.ds, "INSERT INTO www2013_duplicate_detection_arff (tweetIdA, tweetIdB, topicId, judgement) " +
				"VALUES(?, ?, ?, ?)");
		su.declareParameter(new SqlParameter("tweetIdA", Types.BIGINT));
		su.declareParameter(new SqlParameter("tweetIdB", Types.BIGINT));
		su.declareParameter(new SqlParameter("topicId", Types.INTEGER));
		su.declareParameter(new SqlParameter("judgement", Types.INTEGER));
		su.compile();
		
		ResultSet duplicateSets = DBUtility.executeQuerySingleConnection("SELECT distinct duplicate_set " +
				"FROM duplicate_detection_judgement " +
				"WHERE duplicate_set != 0 " +
				"ORDER BY duplicate_set ASC");
		
		try {
			while (duplicateSets.next()) {
				ResultSet tweetsInSet = DBUtility.executeQuerySingleConnection("SELECT tweetId, duplicate_judge, topicId " +
						"FROM duplicate_detection_judgement " +
						"WHERE duplicate_set = " + duplicateSets.getInt("duplicate_set"));
				
				TreeMap<Long, Integer> judgesInSet = new TreeMap<Long, Integer>();
				int topicId = 0;
				while (tweetsInSet.next()) {
					judgesInSet.put(tweetsInSet.getLong("tweetId"), tweetsInSet.getInt("duplicate_judge"));
					topicId = tweetsInSet.getInt("topicId");
				}
				
				for (Long tweetIdA : judgesInSet.keySet()) {
					for (Long tweetIdB : judgesInSet.keySet()) {
						if (tweetIdA >= tweetIdB) {
							continue;
						} else {
							ResultSet judgeRS = DBUtility.executeQuerySingleConnection("SELECT * FROM www2013_duplicate_detection_arff " +
									"WHERE tweetIdA = " + tweetIdA + " AND tweetIdB = " + tweetIdB);
							
							if (judgeRS.next())
								continue;
							
							
							int judge = 0;
							if (judgesInSet.get(tweetIdA) <= judgesInSet.get(tweetIdB)) {
								judge = judgesInSet.get(tweetIdA);
							} else {
								judge = judgesInSet.get(tweetIdB);
							}
							su.update(new Object[] {
									tweetIdA,
									tweetIdB,
									topicId,
									judge
							});
						}
					}
				}
				System.out.println("Duplicate set: " + duplicateSets.getInt("duplicate_set") + " completed!");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private static void negativeInstantces() {
		// iterate over all topics --> for every non-duplicate v.s. duplicate -> judge = 0;
		
		BatchSqlUpdate su = new BatchSqlUpdate(DBUtility.ds, "INSERT INTO www2013_duplicate_detection_arff (tweetIdA, tweetIdB, topicId, judgement) " +
				"VALUES(?, ?, ?, ?)");
		su.declareParameter(new SqlParameter("tweetIdA", Types.BIGINT));
		su.declareParameter(new SqlParameter("tweetIdB", Types.BIGINT));
		su.declareParameter(new SqlParameter("topicId", Types.INTEGER));
		su.declareParameter(new SqlParameter("judgement", Types.INTEGER));
		su.compile();
		
//		ResultSet topics = DBUtility.executeQuerySingleConnection("SELECT distinct topicId " +
//				"FROM duplicate_detection_judgement " +
//				"ORDER BY topicId ASC");
//		
		try {
//			while (topics.next()) {
				int topicId = 22;//topics.getInt("topicId");
				System.out.println("Topic " + topicId + " started!");
				ArrayList<Long> positiveTweets = new ArrayList<Long>();
				ArrayList<Long> negativeTweets = new ArrayList<Long>();
				
				ResultSet negativeInstances = DBUtility.executeQuerySingleConnection("SELECT tweetId " +
						"FROM qrel_original " +
						"WHERE judge > 0 AND topicId = " + topicId);
				while (negativeInstances.next()) {
					negativeTweets.add(negativeInstances.getLong("tweetId"));
				}
				
				ResultSet positiveInstances = DBUtility.executeQuerySingleConnection("SELECT tweetId " +
						"FROM duplicate_detection_judgement " +
						"WHERE duplicate_judge > 0 AND topicId = " + topicId);
				while (positiveInstances.next()) {
					positiveTweets.add(positiveInstances.getLong("tweetId"));
					negativeTweets.remove(positiveInstances.getLong("tweetId"));
				}
				
				System.out.println("Topic " + topicId + " positive:" + positiveTweets.size());
				System.out.println("Topic " + topicId + " negative:" + negativeTweets.size());
				
				int counter = 0;
				for (int i = 0; i < positiveTweets.size(); i++) {
					for (int j = 0; j < negativeTweets.size(); j++) {
						counter++;
						long tweetIdA = positiveTweets.get(i);
						long tweetIdB = negativeTweets.get(j);
						
						if (tweetIdA > tweetIdB) {
							long temp = tweetIdB;
							tweetIdB = tweetIdA;
							tweetIdA = temp;
						}
						
						ResultSet judgeRS = DBUtility.executeQuerySingleConnection("SELECT * FROM www2013_duplicate_detection_arff " +
								"WHERE tweetIdA = " + tweetIdA + " AND tweetIdB = " + tweetIdB);
						
						if (judgeRS.next())
							continue;
						
						su.update(new Object[] {
								tweetIdA,
								tweetIdB,
								topicId,
								0
						});
					}
				}
				su.flush();
				
				System.out.println("Topic " + topicId + " finished!");
				System.out.println(counter + " judgements inserted!");
//			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
