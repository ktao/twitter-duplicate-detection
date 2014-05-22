/*********************************************************
*  Copyright (c) 2011 by Web Information Systems (WIS) Group.
*  Ke Tao, http://taubau.info/
*
*  Some rights reserved.
*
*  Contact: http://www.wis.ewi.tudelft.nl/
*
**********************************************************/
package nl.wisdelft.twitter.analytics.redundancy.analyze;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import nl.wisdelft.twitter.io.DBUtility;
import nl.wisdelft.twitter.util.parser.TwitterParser;

import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

/**
 * 
 * @author Ke Tao, <a href="mailto:k.tao@tudelft.nl">k.tao@tudelft.nl</a>
 * @author last edited by: ktao
 * 
 * @version created on Nov 5, 2012
 */
public class URLExpander {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String query = "SELECT id, content FROM trec_2013_microblog_tweets_full " +
				"WHERE id > " + args[0] + " ORDER BY id ASC";
		
		expandURLInTweetsAndStoreInDB(query);
	}
	
	/**
	 * 
	 * @param query SELECT 
	 */
	public static void expandURLInTweetsAndStoreInDB(String query) {
		ResultSet tweets = DBUtility.executeQuerySingleConnection(query);
		
		BatchSqlUpdate su = new BatchSqlUpdate(DBUtility.ds, "INSERT IGNORE INTO tweets_uri_cikm2013 " +
				"(id, originalUri, isShort, expandedUri) " +
				"VALUE (?, ?, ?, ?)");
		su.declareParameter(new SqlParameter("id", Types.BIGINT));
		su.declareParameter(new SqlParameter("originalUri", Types.VARCHAR));
		su.declareParameter(new SqlParameter("isShort", Types.TINYINT));
		su.declareParameter(new SqlParameter("expandedUri", Types.VARCHAR));
		su.compile();
		
		int updateAfter = 100;
		
		int count = 0;
		try {
			while (tweets.next()) {
				String tweet = tweets.getString("content");
				count ++;
				
				System.out.println("Processing Tweet " + tweets.getLong("id"));
				
				for (String url : TwitterParser.getURLs(tweet)) {
					String expandedURL = TwitterParser.getExtendedURL(url);
					if (expandedURL.equals(url)) {
						su.update(new Object[] {
							tweets.getLong("id"),
							url,
							0,
							url // FIXED: set the expanded URL to original URL
						});
					} else {
						su.update(new Object[] {
							tweets.getLong("id"),
							url,
							1,
							expandedURL
						});
					}
				}
				
				if (count % updateAfter == 0) {
					su.flush();
					System.out.println("**************************");
					System.out.println(count + " tweets processed.");
					System.out.println("**************************");
				}
			}
			su.flush();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
