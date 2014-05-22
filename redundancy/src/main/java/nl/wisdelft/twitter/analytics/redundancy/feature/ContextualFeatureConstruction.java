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
import java.sql.Timestamp;
import java.sql.Types;

import nl.wisdelft.twitter.io.DBUtility;

import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

/**
 * @author ktao
 *
 */
public class ContextualFeatureConstruction {

	public static void constructContextualFeatures() {
		constructContextualFeatures(0);
	}
	
	public static void constructContextualFeatures(int i2s) {
		BatchSqlUpdate su = new BatchSqlUpdate(DBUtility.ds, "UPDATE model_arff_tweets2011 " +
				"SET timeDifference = ?, friendsDifference = ?, followersDifference = ?, sameClient = ? " + 
				"WHERE tweetIdA = ? AND tweetIdB = ?");
		su.declareParameter(new SqlParameter("timeDifference", Types.DOUBLE));
		su.declareParameter(new SqlParameter("friendsDifference", Types.DOUBLE));
		su.declareParameter(new SqlParameter("followersDifference", Types.DOUBLE));
		su.declareParameter(new SqlParameter("sameClient", Types.DOUBLE));
		su.declareParameter(new SqlParameter("tweetIdA", Types.BIGINT));
		su.declareParameter(new SqlParameter("tweetIdB", Types.BIGINT));
		su.compile();
		
		// syntactical features
		ResultSet tweets = DBUtility.executeQuerySingleConnection("SELECT i.tweetIdA, ta.content as contentA, i.tweetIdB, tb.content as contentB, ta.userId as userIdA, tb.userId as userIdB " +
				"FROM model_arff_tweets2011 i, tweets ta, tweets tb " +
				"WHERE i.tweetIdA = ta.id AND i.tweetIdB = tb.id");
		
		int count = 0;
		
		try {
			while (tweets.next()) {
				count++;
				if (count < i2s)
					continue;
//				String tContentA = tweets.getString("contentA");
//				String tContentB = tweets.getString("contentB");
				long tweetIdA = tweets.getLong("tweetIdA");
				long tweetIdB = tweets.getLong("tweetIdB");
				int userIdA = tweets.getInt("userIdA");
				int userIdB = tweets.getInt("userIdB");
				
				// Feature Contextual 1 = timeDifference
				double timeDifference = timeDifference(tweetIdA, tweetIdB);
				
				// Feature Contextual 2 = friendsDifference
				double friendsDifference = friendsDifference(userIdA, userIdB);
				
				// Feature Contextual 3 =
				double followersDifference = followersDifference(userIdA, userIdB);
				
				// Feature Contextual 4 = using same client application or not
				double sameClient = sameClient(tweetIdA, tweetIdB);
				
				su.update(new Object[]{
					timeDifference,
					friendsDifference,
					followersDifference,
					sameClient,
					tweetIdA,
					tweetIdB
				});
				
				if (count % 100 == 0) {
					System.out.println(count + " pairs updated!");
					su.flush();
				}
			}
			su.flush();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static double friendsDifference(long tweetIdA, long tweetIdB) {
		if (tweetIdA == 0 || tweetIdB == 0)
			return -1;
		int numberOfFriendsA = -1;
		ResultSet rs = DBUtility.executeQuerySingleConnection("SELECT numberOfFriends " + 
				"FROM user_www2013 WHERE id = " + tweetIdA);
		try {
			if (rs.next()) {
				numberOfFriendsA = rs.getInt("numberOfFriends");
			}
		} catch (SQLException e) {
			System.err.println("Error while fetching user data @ userId: " + tweetIdA);
		}
		
		int numberOfFriendsB = -1;
		rs = DBUtility.executeQuerySingleConnection("SELECT numberOfFriends " + 
				"FROM user_www2013 WHERE id = " + tweetIdB);
		try {
			if (rs.next()) {
				numberOfFriendsB = rs.getInt("numberOfFriends");
			}
		} catch (SQLException e) {
			System.err.println("Error while fetching user data @ userId: " + tweetIdB);
		}
		
		if (numberOfFriendsA == -1 || numberOfFriendsB == -1) {
			return -1;
		}
		
		if (numberOfFriendsA == numberOfFriendsB) {
			return 0;
		} else {
			return Math.log10(Math.abs(numberOfFriendsA - numberOfFriendsB)) / 7;
		}
	}

	public static double followersDifference(long tweetIdA, long tweetIdB) {
		if (tweetIdA == 0 || tweetIdB == 0)
			return -1;
		int numberOfFollowersA = -1;
		ResultSet rs = DBUtility.executeQuerySingleConnection("SELECT numberOfFollowers " + 
				"FROM user_www2013 WHERE id = " + tweetIdA);
		try {
			if (rs.next()) {
				numberOfFollowersA = rs.getInt("numberOfFollowers");
			}
		} catch (SQLException e) {
			System.err.println("Error while fetching user data @ userId: " + tweetIdA);
		}
		
		int numberOfFollowersB = -1;
		rs = DBUtility.executeQuerySingleConnection("SELECT numberOfFollowers " + 
				"FROM user_www2013 WHERE id = " + tweetIdB);
		try {
			if (rs.next()) {
				numberOfFollowersB = rs.getInt("numberOfFollowers");
			}
		} catch (SQLException e) {
			System.err.println("Error while fetching user data @ userId: " + tweetIdB);
		}
		
		if (numberOfFollowersA == numberOfFollowersB) {
			return 0;
		} else {
			return Math.log10(Math.abs(numberOfFollowersA - numberOfFollowersB)) / 7;
		}
	}

	public static double sameClient(long tweetIdA, long tweetIdB) {
		ResultSet rsA = DBUtility.executeQuerySingleConnection("SELECT source FROM tweets_www2013 WHERE id = " + tweetIdA);
		String clientA = null;
		try {
			if (rsA.next()) {
				clientA = rsA.getString("source");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if (clientA == null) {
			System.err.println("Tweet doesn't exist? ->" + tweetIdA);
			return 0.0;
		}
		
		ResultSet rsB = DBUtility.executeQuerySingleConnection("SELECT source FROM tweets_www2013 WHERE id = " + tweetIdB);
		String clientB = null;
		try {
			if (rsB.next()) {
				clientB = rsB.getString("source");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if (clientB == null) {
			System.err.println("Tweet doesn't exist? ->" + tweetIdB);
			return 0.0;
		}
		
		if (clientA.equals(clientB))
			return 1.0;
		else
			return 0.0;
	}

	public static double timeDifference(long tweetIdA, long tweetIdB) {
		ResultSet rsA = DBUtility.executeQuerySingleConnection("SELECT creationTime FROM tweets_www2013 WHERE id = " + tweetIdA);
		Timestamp tsA = null;
		try {
			if (rsA.next()) {
				tsA = rsA.getTimestamp("creationTime");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ResultSet rsB = DBUtility.executeQuerySingleConnection("SELECT creationTime FROM tweets_www2013 WHERE id = " + tweetIdB);
		Timestamp tsB = null;
		try {
			if (rsB.next()) {
				tsB = rsB.getTimestamp("creationTime");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if (tsA == null || tsB == null) {
			System.err.println("Timestamp error!");
			return 0.0;
		} else {
			return (double) (tsB.getTime() - tsA.getTime()) / (2 * 7 * 24 * 60 * 60 * 1000);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		constructContextualFeatures(35100);
	}

}
