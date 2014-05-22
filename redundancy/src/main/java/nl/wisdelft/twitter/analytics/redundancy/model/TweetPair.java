/*********************************************************
*  Copyright (c) 2011 by Web Information Systems (WIS) Group.
*  Ke Tao, http://taubau.info/
*
*  Some rights reserved.
*
*  Contact: http://www.wis.ewi.tudelft.nl/
*
**********************************************************/
package nl.wisdelft.twitter.analytics.redundancy.model;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;

import nl.wisdelft.twitter.analytics.redundancy.feature.ContextualFeatureConstruction;
import nl.wisdelft.twitter.analytics.redundancy.feature.ExternalResourcesFeatureConstruction;
import nl.wisdelft.twitter.analytics.redundancy.feature.FeatureConstruction;
import nl.wisdelft.twitter.analytics.redundancy.feature.SemanticFeatureConstruction;
import nl.wisdelft.twitter.io.DBUtility;


/**
 * 
 * @author Ke Tao, <a href="mailto:k.tao@tudelft.nl">k.tao@tudelft.nl</a>
 * @author last edited by: ktao
 * 
 * @version created on Jun 20, 2013
 */
public class TweetPair {

	public long tweetIdA;
	public long tweetIdB;
	
	private String tContentA = null;
	private String tContentB = null;
	
	// Syntactical features - 6
	private Double levenshteinDistance = null;
	private Double overlapTerms = null;
	private Double overlapHashtags = null;
	private Double overlapURLs = null;
	private Double overlapExtendedURLs = null;
	private Double lengthDifference = null;
				
	// Semantic features - 9
	private Double overlapDBpediaEntities = null;
	private Double overlapDBpediaEntityTypes = null;
	private Double overlapOpenCalaisEntities = null;
	private Double overlapOpenCalaisTypes = null;
	private Double overlapOpenCalaisTopic = null;	//
	private Double overlapWPMEntities = null;
	private Double overlapWordNetConcepts = null;
	private Double overlapWordNetSynsetConcepts = null;
	private Double wordNetSimilarity = null;
				
	// External Resources features - 9
	private Double overlapEnrichedDBpediaEntities = null;
	private Double overlapEnrichedDBpediaEntityTypes = null;
	private Double overlapEnrichedOpenCalaisEntities = null;
	private Double overlapEnrichedOpenCalaisTypes = null;
	private Double overlapEnrichedOpenCalaisTopic = null;
	private Double overlapEnrichedWPMEntities = null;
	private Double overlapEnrichedWordNetConcepts = null;
	private Double overlapEnrichedWordNetSynsetConcepts = null;
	private Double enrichedWordNetSimilarity = null;
	
	// Contextual features - 4
	private Double timeDifference = null;
	private Double friendsDifference = null;
	private Double followersDifference = null;
	private Double sameClient = null;
		
	public TweetPair (long _tweetIdA, long _tweetIdB) {
		// try read database for the existing features
		if (_tweetIdA > _tweetIdB) {
			this.tweetIdA = _tweetIdB;
			this.tweetIdB = _tweetIdA;
		} else {
			this.tweetIdA = _tweetIdA;
			this.tweetIdB = _tweetIdB;
		}
		
		ResultSet rs = DBUtility.executeQuerySingleConnection("SELECT id, content FROM tweets_airs2013 " +
				"WHERE id = " + this.tweetIdA + " OR id = " + this.tweetIdB + " ORDER BY id ASC");
		
		try {
			if (rs.next() && this.tweetIdA == rs.getLong("id")) {
				this.tContentA = rs.getString("content");
			} else {
				System.err.println("Tweet not in the database: " + tweetIdA);
			}
			
			if (rs.next() && this.tweetIdB == rs.getLong("id")) {
				this.tContentB = rs.getString("content");
			} else {
				System.err.println("Tweet not in the database: " + tweetIdB);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		rs = DBUtility.executePrepareQuerySingleConnection("SELECT * FROM features_tweets2013 " +
				"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
		try {
			if (rs.next()) {
				// fill syntactical features
				if (rs.getDouble("levensteinDistance") != -1)
					this.levenshteinDistance = rs.getDouble("levensteinDistance");
				if (rs.getDouble("overlapTerms") != -1)
					this.overlapTerms = rs.getDouble("overlapTerms");
				if (rs.getDouble("overlapHashtags") != -1)
					this.overlapHashtags = rs.getDouble("overlapHashtags");
				if (rs.getDouble("overlapURLs") != -1)
					this.overlapURLs = rs.getDouble("overlapURLs");
				if (rs.getDouble("overlapExtendedURLs") != -1)
					this.overlapExtendedURLs = rs.getDouble("overlapExtendedURLs");
				if (rs.getDouble("lengthDifference") != -1)
					this.lengthDifference = rs.getDouble("lengthDifference");
				
				// fill semantic features
				if (rs.getDouble("overlapDBpediaEntities") != -1)
					this.overlapDBpediaEntities = rs.getDouble("overlapDBpediaEntities");
				if (rs.getDouble("overlapDBpediaEntityTypes") != -1)
					this.overlapDBpediaEntityTypes = rs.getDouble("overlapDBpediaEntityTypes");
				if (rs.getDouble("overlapOpenCalaisEntities") != -1)
					this.overlapOpenCalaisEntities = rs.getDouble("overlapOpenCalaisEntities");
				if (rs.getDouble("overlapOpenCalaisTypes") != -1)
					this.overlapOpenCalaisTypes = rs.getDouble("overlapOpenCalaisTypes");
				if (rs.getDouble("overlapOpenCalaisTopic") != -1)
					this.overlapOpenCalaisTopic = rs.getDouble("overlapOpenCalaisTopic");
				if (rs.getDouble("overlapWPMEntities") != -1)
					this.overlapWPMEntities = rs.getDouble("overlapWPMEntities");
				if (rs.getDouble("overlapWordNetConcepts") != -1)
					this.overlapWordNetConcepts = rs.getDouble("overlapWordNetConcepts");
				if (rs.getDouble("overlapWordNetSynsetConcepts") != -1)
					this.overlapWordNetSynsetConcepts = rs.getDouble("overlapWordNetSynsetConcepts");
				if (rs.getDouble("wordNetSimilarity") != -1)
					this.wordNetSimilarity = rs.getDouble("wordNetSimilarity");
				
				// fill enriched semantic features
				if (rs.getDouble("overlapEnrichedDBpediaEntities") != -1)
					this.overlapEnrichedDBpediaEntities = rs.getDouble("overlapEnrichedDBpediaEntities");
				if (rs.getDouble("overlapEnrichedDBpediaEntityTypes") != -1)
					this.overlapEnrichedDBpediaEntityTypes = rs.getDouble("overlapEnrichedDBpediaEntityTypes");
				if (rs.getDouble("overlapEnrichedOpenCalaisEntities") != -1)
					this.overlapEnrichedOpenCalaisEntities = rs.getDouble("overlapEnrichedOpenCalaisEntities");
				if (rs.getDouble("overlapEnrichedOpenCalaisTypes") != -1)
					this.overlapEnrichedOpenCalaisTypes = rs.getDouble("overlapEnrichedOpenCalaisTypes");
				if (rs.getDouble("overlapEnrichedOpenCalaisTopic") != -1)
					this.overlapEnrichedOpenCalaisTopic = rs.getDouble("overlapEnrichedOpenCalaisTopic");
				if (rs.getDouble("overlapEnrichedWPMEntities") != -1)
					this.overlapEnrichedWPMEntities = rs.getDouble("overlapEnrichedWPMEntities");
				if (rs.getDouble("overlapEnrichedWordNetConcepts") != -1)
					this.overlapEnrichedWordNetConcepts = rs.getDouble("overlapEnrichedWordNetConcepts");
				if (rs.getDouble("overlapEnrichedWordNetSynsetConcepts") != -1)
					this.overlapEnrichedWordNetSynsetConcepts = rs.getDouble("overlapEnrichedWordNetSynsetConcepts");
				if (rs.getDouble("enrichedWordNetSimilarity") != -1)
					this.enrichedWordNetSimilarity = rs.getDouble("enrichedWordNetSimilarity");
				
				// contextual features
				if (rs.getDouble("timeDifference") != -1)
					this.timeDifference = rs.getDouble("timeDifference");
				if (rs.getDouble("timeDifference") != -1)
					this.followersDifference = rs.getDouble("timeDifference");
				if (rs.getDouble("friendsDifference") != -1)
					this.friendsDifference = rs.getDouble("friendsDifference");
				if (rs.getDouble("sameClient") != -1)
					this.sameClient = rs.getDouble("sameClient");
			} else {
				int res = DBUtility.executeUpdateQuerySingleConnection("INSERT INTO features_tweets2013 (tweetIdA, tweetIdB) " +
						"VALUES (" + this.tweetIdA + ", " + this.tweetIdB + ")");
				System.out.println(res + " row(s) affected.");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		rs = null;
	}

	public Double getLevenshteinDistance() {
		if (levenshteinDistance == null) {
			this.levenshteinDistance = (double)StringUtils.getLevenshteinDistance(tContentA, tContentB) / 140.00;
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET levensteinDistance = " + this.levenshteinDistance + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("Levenshtein updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		return levenshteinDistance;
	}

	public Double getOverlapTerms() {
		if (overlapTerms == null) {
			this.overlapTerms = FeatureConstruction.overlapOfTermsDivByMean(tContentA, tContentB);
			
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapTerms = " + this.overlapTerms + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapTerms updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		return overlapTerms;
	}

	public Double getOverlapHashtags() {
		if (overlapHashtags == null) {
			this.overlapHashtags = FeatureConstruction.overlapOfHashtagsDivByMean(tContentA, tContentB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapHashtags = " + this.overlapHashtags + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapHashtags updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapHashtags;
	}

	public Double getOverlapURLs() {
		if (overlapURLs == null) {
			this.overlapURLs = FeatureConstruction.overlapOfURLs(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapURLs = " + this.overlapURLs + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapURLs updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapURLs;
	}

	public Double getOverlapExtendedURLs() {
		if (overlapExtendedURLs == null) {
			this.overlapExtendedURLs = FeatureConstruction.overlapOfExtendedURLs(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapExtendedURLs = " + this.overlapExtendedURLs + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapExtendedURLs updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapExtendedURLs;
	}

	public Double getLengthDifference() {
		if (lengthDifference == null) {
			this.lengthDifference = (double)Math.abs(tContentA.length() - tContentB.length()) / 140.00;
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET lengthDifference = " + this.lengthDifference + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("lengthDifference updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return lengthDifference;
	}

	public Double getOverlapDBpediaEntities() {
		if (overlapDBpediaEntities == null) {
			this.overlapDBpediaEntities = SemanticFeatureConstruction.overlapDBpediaEntities(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapDBpediaEntities = " + this.overlapDBpediaEntities + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapDBpediaEntities updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		return overlapDBpediaEntities;
	}

	public Double getOverlapDBpediaEntityTypes() {
		if (overlapDBpediaEntityTypes == null) {
			this.overlapDBpediaEntityTypes = SemanticFeatureConstruction.overlapDBpediaEntityTypes(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapDBpediaEntityTypes = " + this.overlapDBpediaEntityTypes + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapDBpediaEntityTypes updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapDBpediaEntityTypes;
	}

	public Double getOverlapOpenCalaisEntities() {
		if (overlapOpenCalaisEntities == null) {
			this.overlapOpenCalaisEntities = SemanticFeatureConstruction.overlapOpenCalaisEntities(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapOpenCalaisEntities = " + this.overlapOpenCalaisEntities + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapOpenCalaisEntities updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapOpenCalaisEntities;
	}

	public Double getOverlapOpenCalaisTypes() {
		if (overlapOpenCalaisTypes == null) {
			this.overlapOpenCalaisTypes = SemanticFeatureConstruction.overlapOpenCalaisTypes(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapOpenCalaisTypes = " + this.overlapOpenCalaisTypes + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapOpenCalaisTypes updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapOpenCalaisTypes;
	}

	public Double getOverlapOpenCalaisTopic() {
		if (overlapOpenCalaisTopic == null) {
			this.overlapOpenCalaisTopic = SemanticFeatureConstruction.overlapOpenCalaisTopic(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapOpenCalaisTopic = " + this.overlapOpenCalaisTopic + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapOpenCalaisTopic updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapOpenCalaisTopic;
	}

	public Double getOverlapWPMEntities() {
		if (overlapWPMEntities == null) {
			this.overlapWPMEntities = SemanticFeatureConstruction.overlapWikipediaMinerEntities(tweetIdA, tweetIdB);
			
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapWPMEntities = " + this.overlapWPMEntities + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapWPMEntities updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapWPMEntities;
	}

	public Double getOverlapWordNetConcepts() {
		if (overlapWordNetConcepts == null) {
			this.overlapWordNetConcepts = SemanticFeatureConstruction.overlapWordNetConcepts(tContentA, tContentB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapWordNetConcepts = " + this.overlapWordNetConcepts + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapWordNetConcepts updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapWordNetConcepts;
	}

	public Double getOverlapWordNetSynsetConcepts() {
		if (overlapWordNetSynsetConcepts == null) {
			this.overlapWordNetSynsetConcepts = SemanticFeatureConstruction.overlapWordNetSynsetConcepts(tContentA, tContentB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapWordNetSynsetConcepts = " + this.overlapWordNetSynsetConcepts + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapWordNetSynsetConcepts updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapWordNetSynsetConcepts;
	}

	public Double getWordNetSimilarity() {
		if (wordNetSimilarity == null) {
			this.wordNetSimilarity = SemanticFeatureConstruction.wordNetSimilarity(tContentA, tContentB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET wordNetSimilarity = " + this.wordNetSimilarity + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("wordNetSimilarity updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return wordNetSimilarity;
	}

	public Double getOverlapEnrichedDBpediaEntities() {
		if (overlapEnrichedDBpediaEntities == null) {
			this.overlapEnrichedDBpediaEntities = ExternalResourcesFeatureConstruction.overlapEnrichedDBpediaEntities(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapEnrichedDBpediaEntities = " + this.overlapEnrichedDBpediaEntities + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapEnrichedDBpediaEntities updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapEnrichedDBpediaEntities;
	}

	public Double getOverlapEnrichedDBpediaEntityTypes() {
		if (overlapEnrichedDBpediaEntityTypes == null) {
			this.overlapEnrichedDBpediaEntityTypes = ExternalResourcesFeatureConstruction.overlapEnrichedDBpediaEntityTypes(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapEnrichedDBpediaEntityTypes = " + this.overlapEnrichedDBpediaEntityTypes + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapEnrichedDBpediaEntityTypes updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapEnrichedDBpediaEntityTypes;
	}

	public Double getOverlapEnrichedOpenCalaisEntities() {
		if (overlapEnrichedOpenCalaisEntities == null) {
			this.overlapEnrichedOpenCalaisEntities = ExternalResourcesFeatureConstruction.overlapEnrichedOpenCalaisEntities(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapEnrichedOpenCalaisEntities = " + this.overlapEnrichedOpenCalaisEntities + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapEnrichedOpenCalaisEntities updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapEnrichedOpenCalaisEntities;
	}

	public Double getOverlapEnrichedOpenCalaisTypes() {
		if (overlapEnrichedOpenCalaisTypes == null) {
			this.overlapEnrichedOpenCalaisTypes = ExternalResourcesFeatureConstruction.overlapEnrichedOpenCalaisTypes(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapEnrichedOpenCalaisTypes = " + this.overlapEnrichedOpenCalaisTypes + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapEnrichedOpenCalaisTypes updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapEnrichedOpenCalaisTypes;
	}

	public Double getOverlapEnrichedOpenCalaisTopic() {
		if (overlapEnrichedOpenCalaisTopic == null) {
			this.overlapEnrichedOpenCalaisTopic = ExternalResourcesFeatureConstruction.overlapEnrichedOpenCalaisTopic(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapEnrichedOpenCalaisTopic = " + this.overlapEnrichedOpenCalaisTopic + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapEnrichedOpenCalaisTopic updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapEnrichedOpenCalaisTopic;
	}

	public Double getOverlapEnrichedWPMEntities() {
		if (overlapEnrichedWPMEntities == null) {
			this.overlapEnrichedWPMEntities = ExternalResourcesFeatureConstruction.overlapEnrichedWPMEntities(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapEnrichedWPMEntities = " + this.overlapEnrichedWPMEntities + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapEnrichedWPMEntities updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapEnrichedWPMEntities;
	}

	public Double getOverlapEnrichedWordNetConcepts() {
		if (overlapEnrichedWordNetConcepts == null) {
			this.overlapEnrichedWordNetConcepts = ExternalResourcesFeatureConstruction.overlapEnrichedWordNetConcepts(tweetIdA, tweetIdB);

			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapEnrichedWordNetConcepts = " + this.overlapEnrichedWordNetConcepts + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapEnrichedWordNetConcepts updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapEnrichedWordNetConcepts;
	}

	public Double getOverlapEnrichedWordNetSynsetConcepts() {
		if (overlapEnrichedWordNetSynsetConcepts == null) {
			this.overlapEnrichedWordNetSynsetConcepts = ExternalResourcesFeatureConstruction.overlapEnrichedWordNetSynsetConcepts(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET overlapEnrichedWordNetSynsetConcepts = " + this.overlapEnrichedWordNetSynsetConcepts + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("overlapEnrichedWordNetSynsetConcepts updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return overlapEnrichedWordNetSynsetConcepts;
	}

	public Double getEnrichedWordNetSimilarity() {
		if (enrichedWordNetSimilarity == null) {
			this.enrichedWordNetSimilarity = ExternalResourcesFeatureConstruction.enrichedWordNetSimilarity(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET enrichedWordNetSimilarity = " + this.enrichedWordNetSimilarity + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("enrichedWordNetSimilarity updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return enrichedWordNetSimilarity;
	}

	public Double getTimeDifference() {
		if (timeDifference == null) {
			this.timeDifference = ContextualFeatureConstruction.timeDifference(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET timeDifference = " + this.timeDifference + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("timeDifference updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return timeDifference;
	}

	public Double getFriendsDifference() {
		if (friendsDifference == null) {
			this.friendsDifference = ContextualFeatureConstruction.friendsDifference(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET friendsDifference = " + this.friendsDifference + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("friendsDifference updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return friendsDifference;
	}

	public Double getFollowersDifference() {
		if (followersDifference == null) {
			this.followersDifference = ContextualFeatureConstruction.followersDifference(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET followersDifference = " + this.followersDifference + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("followersDifference updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return followersDifference;
	}

	public Double getSameClient() {
		if (sameClient == null) {
			this.sameClient = ContextualFeatureConstruction.sameClient(tweetIdA, tweetIdB);
		
			int res = DBUtility.executeUpdateQuerySingleConnection("UPDATE features_tweets2013 SET sameClient = " + this.sameClient + " " +
					"WHERE tweetIdA = " + this.tweetIdA + " AND tweetIdB = " + this.tweetIdB);
			if (res == 1)
				System.out.println("sameClient updated for pair (" + this.tweetIdA + "," + this.tweetIdB + ").");
		}
		
		return sameClient;
	}

}
