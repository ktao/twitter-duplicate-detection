/**
 * This source file is part of the work done in Web Information Systems group
 * at Delft University of Technology (TU Delft). Please contact the author via 
 * Email before reusing and distributing this work.
 * 
 * Copyright � 2012
 * Filename: ARFFFileGeneration.java
 * Author: Ke Tao <k.tao (at) tudelft.nl>
 * Date created: Nov 5, 2012
 */
package nl.wisdelft.twitter.analytics.redundancy.evaluation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import nl.wisdelft.twitter.io.DBUtility;


/**
 * @author ktao
 *
 */
public class ARFFFileGeneration {	
	
	private static DecimalFormat df = new DecimalFormat("0.000000000");
	private static boolean BINARY = true;
	private static boolean OPTIMIZED = false;
	
	public static Map<String, String> topicSQLRestrictions = new LinkedHashMap<String, String>();
	static {
		topicSQLRestrictions.put("popular", "topicId IN (2, 3, 8, 10, 12, " +
														"13, 15, 17, 18, 19, " +
														"20, 22, 24, 26, 27, " +
														"28, 29, 32, 36, 37, " +
														"38, 41, 45, 48)");
		topicSQLRestrictions.put("unpopular", "topicId IN (1, 4, 5, 6, 7, " +
														"9, 11, 14, 16, 21, " +
														"23, 25, 30, 31, 33, " +
														"34, 35, 39, 40, 42, " +
														"43, 44, 46, 47, 49, 50)");
		topicSQLRestrictions.put("global", "topicId IN (1, 2, 9, 10, 11, 12, " +
														"14, 18, 27, 29, 31, " +
														"36, 38, 41, 42, 45, " +
														"47, 48)");
		topicSQLRestrictions.put("local", "topicId IN (3, 4, 5, 6, 7, 8, 13, " +
														"15, 16, 17, 19, 20, " +
														"21, 22, 23, 24, 25, " +
														"26, 28, 30, 32, 33, " +
														"34, 35, 37, 39, 40, " +
														"43, 44, 46, 49, 50)");
		topicSQLRestrictions.put("persistent", "topicId IN (2, 4, 5, 6, 8, 12, " +
														"13, 19, 20, 21, 22, " +
														"23, 25, 26, 27, 29, " +
														"32, 34, 35, 37, 38, " +
														"39, 41, 45, 46, 47, " +
														"48, 49)");
		topicSQLRestrictions.put("occasional", "topicId IN (1, 3, 7, 9, 10, 11, " +
														"14, 15, 16, 17, 18, " +
														"24, 28, 30, 31, 33, " +
														"36, 40, 42, 43, 44, 50)");
		
		// 5 categories of topics
		topicSQLRestrictions.put("business", "topicId IN (1, 6, 9, 20, 28, 46)");
		topicSQLRestrictions.put("entertainment", "topicId IN (13, 14, 16, 17, 18, " +
														"30, 33, 34, 35, 37, 40, 43)");
		topicSQLRestrictions.put("sports", "topicId IN (2, 11, 15, 24, 31)");
		topicSQLRestrictions.put("politics", "topicId IN (3, 4, 7, 8, 10, 12, " +
														"19, 21, 22, 25, 26, 32, " +
														"36, 38, 39, 41, 42, 44, " +
														"45, 48, 49, 50)");
		topicSQLRestrictions.put("technology", "topicId IN (5, 23)");
	}
	
	public static void generatePerTopicARFFFileInBatch(String[] args) throws IOException {
		ResultSet topics = DBUtility.executeQuerySingleConnection("SELECT DISTINCT topicid FROM www2013_duplicate_detection_arff ORDER BY topicid ASC;");
		try {
			while (topics.next()) {
				int topicid = topics.getInt("topicId");
				generateARFFFile(new String[]{"www2013_dd_Sy_topic_" + topicid + ".arff", "topicId=" + Integer.toString(topicid)});
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		generatePerTopicARFFFileInBatch(args);
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void generateARFFFile(String[] args) throws IOException {
//		String output = "SELECT count(*) from www2013_duplicate_detection_arff where ";
//		
//		for (String category : topicSQLRestrictions.keySet()) {
//			System.out.println(category);
//			System.out.println(output + topicSQLRestrictions.get(category));
//		}
		
		String filename = args[0];
		File arffFile = new File(filename);
		
		if (!arffFile.exists()) {
			arffFile.createNewFile();
		} else {
			System.out.println("The specified file exists! Exit.");
			return;
		}
		
		String topicRestriction = "";
		
		if (args.length == 2 && topicSQLRestrictions.containsKey(args[1])) {
			topicRestriction = topicSQLRestrictions.get(args[1]);
		} else {
			topicRestriction = args[1];
		}
		
		FileWriter fw = new FileWriter(arffFile.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		bw.write("@RELATION duplicate-detection\n\n");
		
		ArrayList<String> selectedFeatures = new ArrayList<String>();
		
		// Syntactical features - 6
		selectedFeatures.add("levensteinDistance");
		selectedFeatures.add("overlapTerms");
		selectedFeatures.add("overlapHashtags");
		selectedFeatures.add("overlapURLs");
		selectedFeatures.add("overlapExtendedURLs");
		selectedFeatures.add("lengthDifference");
		
		// Semantic features - 8
//		selectedFeatures.add("overlapDBpediaEntities");
//		selectedFeatures.add("overlapDBpediaEntityTypes");
//		selectedFeatures.add("overlapOpenCalaisEntities");
//		selectedFeatures.add("overlapOpenCalaisTypes");
//		selectedFeatures.add("overlapOpenCalaisTopic");	//
//		selectedFeatures.add("overlapWordNetConcepts");
//		selectedFeatures.add("overlapWordNetSynsetConcepts");
//		selectedFeatures.add("wordNetSimilarity");
		
		// Contextual features - 3 + 1
//		selectedFeatures.add("timeDifference");
//		selectedFeatures.add("friendsDifference");
//		selectedFeatures.add("followersDifference");
//		selectedFeatures.add("sameClient");		// boolean
		
		// External Resources features - 8 - 1
//		selectedFeatures.add("overlapEnrichedDBpediaEntities");
//		selectedFeatures.add("overlapEnrichedDBpediaEntityTypes");
//		selectedFeatures.add("overlapEnrichedOpenCalaisEntities");
//		selectedFeatures.add("overlapEnrichedOpenCalaisTypes");
//		selectedFeatures.add("overlapEnrichedOpenCalaisTopic");
//		selectedFeatures.add("overlapEnrichedWordNetConcepts");
//		selectedFeatures.add("overlapEnrichedWordNetSynsetConcepts");
//		selectedFeatures.add("enrichedWordNetSimilarity");
		
		prepareDescription(selectedFeatures, bw);
		
		bw.write("\n\n@DATA\n");
		
		if (args.length == 2) {
			prepareDataInstances(selectedFeatures, bw, topicRestriction);
		} else {
			prepareDataInstances(selectedFeatures, bw);
		}
		
		bw.close();
	}
	
	private static void prepareDescription(ArrayList<String> features, BufferedWriter bw) throws IOException {
		// Syntactical features - 6
		if (features.contains("levensteinDistance"))	// 1
			bw.write("@ATTRIBUTE levensteinDistance NUMERIC\n");
		
		if (features.contains("overlapTerms"))
			bw.write("@ATTRIBUTE overlapTerms NUMERIC\n");	//2
		
		if (features.contains("overlapHashtags"))	//3
			bw.write("@ATTRIBUTE overlapHashtags NUMERIC\n");
		
		if (features.contains("overlapURLs"))	//4
			bw.write("@ATTRIBUTE overlapURLs NUMERIC\n");
		
		if (features.contains("overlapExtendedURLs"))	//5
			bw.write("@ATTRIBUTE overlapExtendedURLs NUMERIC\n");
		
		if (features.contains("lengthDifference"))	//6
			bw.write("@ATTRIBUTE lengthDifference NUMERIC\n");
				
		// SEMANTIC FEATURES - 8
		if (features.contains("overlapDBpediaEntities"))	// 1
			bw.write("@ATTRIBUTE overlapDBpediaEntities NUMERIC\n");
		
		if (features.contains("overlapDBpediaEntityTypes"))	// 2
			bw.write("@ATTRIBUTE overlapDBpediaEntityTypes NUMERIC\n");
		
		if (features.contains("overlapOpenCalaisEntities"))	// 3
			bw.write("@ATTRIBUTE overlapOpenCalaisEntities NUMERIC\n");
		
		if (features.contains("overlapOpenCalaisTypes"))	// 4
			bw.write("@ATTRIBUTE overlapOpenCalaisTypes NUMERIC\n");
		
		if (features.contains("overlapOpenCalaisTopic"))	// 5
			bw.write("@ATTRIBUTE overlapOpenCalaisTopic NUMERIC\n");
		
		if (features.contains("overlapWordNetConcepts"))	// 6
			bw.write("@ATTRIBUTE overlapWordNetConcepts NUMERIC\n");
		
		if (features.contains("overlapWordNetSynsetConcepts"))	// 7
			bw.write("@ATTRIBUTE overlapWordNetSynsetConcepts NUMERIC\n");
		
		if (features.contains("wordNetSimilarity"))	// 8
			bw.write("@ATTRIBUTE wordNetSimilarity NUMERIC\n");
		
		// Contextual features - 3
		if (features.contains("timeDifference"))	// 1
			bw.write("@ATTRIBUTE timeDifference NUMERIC\n");
		
		if (features.contains("friendsDifference"))	// 2
			bw.write("@ATTRIBUTE friendsDifference NUMERIC\n");
		
		if (features.contains("followersDifference"))	// 3
			bw.write("@ATTRIBUTE followersDifference NUMERIC\n");
		
		if (features.contains("sameClient"))	// 4
			bw.write("@ATTRIBUTE sameClient NUMERIC\n");
		
		// External Resources features - 6
		if (features.contains("overlapEnrichedDBpediaEntities"))	// 1
			bw.write("@ATTRIBUTE overlapEnrichedDBpediaEntities NUMERIC\n");
		
		if (features.contains("overlapEnrichedDBpediaEntityTypes"))	// 2
			bw.write("@ATTRIBUTE overlapEnrichedDBpediaEntityTypes NUMERIC\n");
		
		if (features.contains("overlapEnrichedOpenCalaisEntities"))	// 3
			bw.write("@ATTRIBUTE overlapEnrichedOpenCalaisEntities NUMERIC\n");
		
		if (features.contains("overlapEnrichedOpenCalaisTypes"))	// 4
			bw.write("@ATTRIBUTE overlapEnrichedOpenCalaisTypes NUMERIC\n");
		
		if (features.contains("overlapEnrichedOpenCalaisTopic"))	// 5
			bw.write("@ATTRIBUTE overlapEnrichedOpenCalaisTopic NUMERIC\n");
		
		if (features.contains("overlapEnrichedWordNetConcepts"))	// 6
			bw.write("@ATTRIBUTE overlapEnrichedWordNetConcepts NUMERIC\n");
		
		if (features.contains("overlapEnrichedWordNetSynsetConcepts"))	// 7
			bw.write("@ATTRIBUTE overlapEnrichedWordNetSynsetConcepts NUMERIC\n");
		
		if (features.contains("enrichedWordNetSimilarity"))	// 8
			bw.write("@ATTRIBUTE enrichedWordNetSimilarity NUMERIC\n");
		
		if (BINARY) {
			bw.write("@ATTRIBUTE judgement {1,0}\n");
		} else {
			bw.write("@ATTRIBUTE judgement {5,4,3,2,1}\n");
		}
	}
	
	private static void prepareDataInstances(ArrayList<String> features, BufferedWriter bw) {
		prepareDataInstances(features, bw, null);
	}
	
	/**
	 * Just for diversification evaluation
	 * @param filename
	 * @param tweetIdA
	 * @param tweetIdB
	 * @throws IOException
	 */
	public static void prepareSingleInstanceARFFFile(String filename, long tweetIdA, long tweetIdB) throws IOException {
		File arffFile = new File(filename);
		
		FileWriter fw = new FileWriter(arffFile.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		bw.write("@RELATION www2013-duplicate-detection\n\n");
		
		ArrayList<String> selectedFeatures = new ArrayList<String>();
		
		// Syntactical features - 6
		selectedFeatures.add("levensteinDistance");
		selectedFeatures.add("overlapTerms");
		selectedFeatures.add("overlapHashtags");
		selectedFeatures.add("overlapURLs");
		selectedFeatures.add("overlapExtendedURLs");
		selectedFeatures.add("lengthDifference");
		
		// Semantic features - 8
		selectedFeatures.add("overlapDBpediaEntities");
		selectedFeatures.add("overlapDBpediaEntityTypes");
//		selectedFeatures.add("overlapOpenCalaisEntities");
//		selectedFeatures.add("overlapOpenCalaisTypes");
		selectedFeatures.add("overlapOpenCalaisTopic");	//
		selectedFeatures.add("overlapWordNetConcepts");
		selectedFeatures.add("overlapWordNetSynsetConcepts");
		selectedFeatures.add("wordNetSimilarity");
		
		// Contextual features - 3 + 1
		selectedFeatures.add("timeDifference");
		selectedFeatures.add("friendsDifference");
		selectedFeatures.add("followersDifference");
		selectedFeatures.add("sameClient");		// boolean
		
		// External Resources features - 8 - 1
		selectedFeatures.add("overlapEnrichedDBpediaEntities");
		selectedFeatures.add("overlapEnrichedDBpediaEntityTypes");
//		selectedFeatures.add("overlapEnrichedOpenCalaisEntities");
//		selectedFeatures.add("overlapEnrichedOpenCalaisTypes");
		selectedFeatures.add("overlapEnrichedOpenCalaisTopic");
		selectedFeatures.add("overlapEnrichedWordNetConcepts");
		selectedFeatures.add("overlapEnrichedWordNetSynsetConcepts");
		selectedFeatures.add("enrichedWordNetSimilarity");
		
		prepareDescription(selectedFeatures, bw);
		
		bw.write("\n\n@DATA\n");
		
		String query = "SELECT * FROM www2013_duplicate_detection_arff " +
				"WHERE tweetIdA = " + tweetIdA + " AND tweetIdB = " + tweetIdB;
		ResultSet rs = DBUtility.executeQuerySingleConnection(query);
		
		try {
			if (rs.next()) {
				String line = "";
				
				for (String attr : selectedFeatures) {
					line += df.format(rs.getDouble(attr)) + ",";
				}
				
				if (BINARY) {
					line += (rs.getInt("judgement") > 0 ? 1 : 0) + "\n";
				} else {
					line += rs.getInt("judgement") + "\n";
				}
				bw.write(line);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println("SQL error! Exit.");
			return;
		} catch (IOException e) {
			System.out.println("Error while writing the entry!");
		}
		
		bw.close();
	}
	
	private static void prepareDataInstances(ArrayList<String> features, BufferedWriter bw, String topicRestriction) {
		String query = "";
		
		if (topicRestriction == null) {
			if (BINARY) {
				query = "SELECT * FROM www2013_duplicate_detection_arff " +
//					"WHERE userIdA != 0 AND userIdB != 0 AND friendsDifference != -1 AND followersDifference != -1 " +
					"ORDER BY tweetIdA ASC, tweetIdB ASC";
			} else {
				query = "SELECT * FROM www2013_duplicate_detection_arff " +
//					"WHERE judgement > 0 AND userIdA != 0 AND userIdB != 0 AND friendsDifference != -1 AND followersDifference != -1 " +
					"WHERE judgement > 0 " +
					"ORDER BY tweetIdA ASC, tweetIdB ASC";
			}
		} else {
			if (BINARY) {
				query = "SELECT * FROM www2013_duplicate_detection_arff " +
					"WHERE " + topicRestriction + " " +
					"ORDER BY tweetIdA ASC, tweetIdB ASC";
			} else {
				query = "SELECT * FROM www2013_duplicate_detection_arff " +
					"WHERE judgement > 0 AND " + topicRestriction + " " +
					"ORDER BY tweetIdA ASC, tweetIdB ASC";
			}
		}
		
		System.out.println("Query: " + query);
		
		ResultSet rs = DBUtility.executeQuerySingleConnection(query);
		
		int counter = 0;
		try {
			while (rs.next()) {
				String line = "";
				
				for (String attr : features) {
					line += df.format(rs.getDouble(attr)) + ",";
				}
				
				if (BINARY) {
					if (OPTIMIZED && rs.getInt("judgement") >= 4) {
						continue;
					}
					line += (rs.getInt("judgement") > 0 ? 1 : 0) + "\n";
				} else {
					line += rs.getInt("judgement") + "\n";
				}
				bw.write(line);
				counter++;
				if (counter % 1000 == 0)
					System.out.println(counter + " entries written.");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.err.println("SQL error! Exit.");
			return;
		} catch (IOException e) {
			System.out.println("Error while writing the entry!");
		}
		System.out.println(counter + " entries written.");
	}
}
