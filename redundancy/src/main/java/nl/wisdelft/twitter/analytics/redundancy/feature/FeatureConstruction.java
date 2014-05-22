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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import nl.wisdelft.twitter.io.DBUtility;
import nl.wisdelft.twitter.util.parser.TwitterParser;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

/**
 * 
 * @author Ke Tao, <a href="mailto:k.tao@tudelft.nl">k.tao@tudelft.nl</a>
 * @author last edited by: ktao
 * 
 * @version created on Oct 26, 2012
 */
public class FeatureConstruction {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		syntacticFeatures();
		int levensteinDistance = StringUtils.getLevenshteinDistance("Delft", "Rio de Janeiro");
		System.out.println(levensteinDistance);
	}
	
	public static void syntacticFeatures() {
		BatchSqlUpdate su = new BatchSqlUpdate(DBUtility.ds, "UPDATE cikm2013_duplicate_detection_features " +
				"SET levensteinDistance = ?, overlapTerms = ?, overlapHashtags = ?, overlapURLs = ?, overlapExtendedURLs = ?, lengthDifference = ? " + // 
				"WHERE tweetIdA = ? AND tweetIdB = ?");
		su.declareParameter(new SqlParameter("levensteinDistance", Types.DOUBLE));
		su.declareParameter(new SqlParameter("overlapTerms", Types.DOUBLE));
		su.declareParameter(new SqlParameter("overlapHashtags", Types.DOUBLE));
		su.declareParameter(new SqlParameter("overlapURLs", Types.DOUBLE));
		su.declareParameter(new SqlParameter("overlapExtendedURLs", Types.DOUBLE));
		su.declareParameter(new SqlParameter("lengthDifference", Types.DOUBLE));
		su.declareParameter(new SqlParameter("tweetIdA", Types.BIGINT));
		su.declareParameter(new SqlParameter("tweetIdB", Types.BIGINT));
		su.compile();
		
		// syntactical features
		ResultSet tweets = DBUtility.executeQuerySingleConnection("SELECT i.tweetIdA, ta.content as contentA, i.tweetIdB, tb.content as contentB " +
				"FROM cikm2013_duplicate_detection_features i, trec_2013_microblog_tweets_full ta, trec_2013_microblog_tweets_full tb " +
				"WHERE i.tweetIdA = ta.id AND i.tweetIdB = tb.id");
		// SELECT i.tweetIdA, ta.content as contentA, i.tweetIdB, tb.content as contentB FROM www2013_duplicate_detection_arff i, tweets ta, tweets tb WHERE i.tweetIdA = ta.id AND i.tweetIdB = tb.id AND judgement = 1;  
		
		int count = 0;
		
		try {
			while (tweets.next()) {
				count++;
				String tContentA = tweets.getString("contentA");
				String tContentB = tweets.getString("contentB");
				long tweetIdA = tweets.getLong("tweetIdA");
				long tweetIdB = tweets.getLong("tweetIdB");
				
				// Feature Syn1 = levenstein distance
				int levensteinDistance = StringUtils.getLevenshteinDistance(tContentA, tContentB);
				
				// Feature Syn2 = overlap of terms? stop words? now use the max as the denominator
				double overlapOfTerms = overlapOfTermsDivByMean(tContentA, tContentB);
				
				// Feature Syn3 = overlap of terms? stop words? now use the max as the denominator
				double overlapOfHashtags = overlapOfHashtagsDivByMean(tContentA, tContentB);
				
				// Feature Syn4
				double overlapOfURLs = overlapOfURLs(tweetIdA, tweetIdB);
				
				// Feature Syn5
				double overlapOfExtendedURLs = overlapOfExtendedURLs(tweetIdA, tweetIdB);
				
				// Feature Syn6
				int lengthDiff = tContentA.length() - tContentB.length();
				lengthDiff = lengthDiff >= 0 ? lengthDiff : -lengthDiff;
				
				su.update(new Object[]{
					(double) levensteinDistance / 140.00,
					overlapOfTerms,
					overlapOfHashtags,
					overlapOfURLs,
					overlapOfExtendedURLs,
					(double) lengthDiff / 140.00,
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

	// use max as denominator
	public static double overlapOfTermsDivByMax(String tContentA, String tContentB) {
		String[] termsA = tContentA.split("\\s+");
		String[] termsB = tContentB.split("\\s+");
		List<String> termListB = Arrays.asList(termsB);
		
		int counter = 0;
		for (String termA : termsA) {
			if (termListB.contains(termA)) {
				counter++;
			}
		}
		
		if (termsA.length + termsB.length == 0) {
			return 0.0;
		} else if (termsA.length >= termsB.length) {
			return (double) counter / (double) termsA.length;
		} else {
			return (double) counter / (double) termsB.length;
		}
	}
	
	// use mean (0.5 * ) as denominator
	public static double overlapOfTermsDivByMean(String tContentA, String tContentB) {
		String[] termsA = tContentA.split("\\s+");
		String[] termsB = tContentB.split("\\s+");
		List<String> termListB = Arrays.asList(termsB);
		
		int counter = 0;
		for (String termA : termsA) {
			if (termListB.contains(termA)) {
				counter++;
			}
		}
		
		if (termsA.length + termsB.length == 0) 
			return 0.0;
		else
			return (double) counter / (double) (termsA.length + termsB.length - counter);
	}
	
	public static double overlapOfHashtagsDivByMax(String tContentA, String tContentB) {
		ArrayList<String> hashtagsA = TwitterParser.getHashTags(tContentA);
		ArrayList<String> hashtagsB = TwitterParser.getHashTags(tContentB);
		
		int counter = 0;
		for (String hashtagA : hashtagsA) {
			if (hashtagsB.contains(hashtagA))
				counter++;
		}
		
		if (hashtagsA.size() + hashtagsB.size() == 0) {
			return 0.0;
			
		} else if (hashtagsA.size() >= hashtagsB.size()) {
			return (double) counter / (double) hashtagsA.size();
		} else {
			return (double) counter / (double) hashtagsB.size();
		}
	}
	
	public static double overlapOfHashtagsDivByMean(String tContentA, String tContentB) {
		ArrayList<String> hashtagsA = TwitterParser.getHashTags(tContentA);
		ArrayList<String> hashtagsB = TwitterParser.getHashTags(tContentB);
		
		int counter = 0;
		for (String hashtagA : hashtagsA) {
			if (hashtagsB.contains(hashtagA))
				counter++;
		}
		
		if (hashtagsA.size() * hashtagsB.size() == 0)
			return 0.0;
		else
			return (double) counter / (double) (hashtagsA.size() + hashtagsB.size() - counter);
	}
	
	// divided by mean
	public static double overlapOfURLs(long tIdA, long tIdB) {
		ResultSet rsurlsA = DBUtility.executeQuerySingleConnection("SELECT originalUri FROM tweets_uri_www2013 WHERE id = " + tIdA);
		ArrayList<String> urlsA = new ArrayList<String>();
		try {
			while (rsurlsA.next()) {
				urlsA.add(rsurlsA.getString("originalUri"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ResultSet rsurlsB = DBUtility.executeQuerySingleConnection("SELECT originalUri FROM tweets_uri_www2013 WHERE id = " + tIdB);
		ArrayList<String> urlsB = new ArrayList<String>();
		try {
			while (rsurlsB.next()) {
				urlsB.add(rsurlsB.getString("originalUri"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (urlsA.size() * urlsB.size() == 0)
			return 0.0;
		
		int counter = 0;
		for (String urlA : urlsA) {
			if (urlsB.contains(urlA))
				counter++;
		}
		
		return (double) counter / (double) (urlsA.size() + urlsB.size() - counter);
	}
	
	// intersection divided by union
	public static double overlapOfExtendedURLs(long tIdA, long tIdB) {
		ResultSet rsurlsA = DBUtility.executeQuerySingleConnection("SELECT expandedUri FROM tweets_uri_www2013 WHERE id = " + tIdA);
		ArrayList<String> urlsA = new ArrayList<String>();
		try {
			while (rsurlsA.next()) {
				urlsA.add(rsurlsA.getString("expandedUri"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ResultSet rsurlsB = DBUtility.executeQuerySingleConnection("SELECT expandedUri FROM tweets_uri_www2013 WHERE id = " + tIdB);
		ArrayList<String> urlsB = new ArrayList<String>();
		try {
			while (rsurlsB.next()) {
				urlsB.add(rsurlsB.getString("expandedUri"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (urlsA.size() * urlsB.size() == 0)
			return 0.0;
		
		int counter = 0;
		for (String urlA : urlsA) {
			if (urlsB.contains(urlA))
				counter++;
		}
		
		return (double) counter / (double) (urlsA.size() + urlsB.size() - counter);
	}
}
