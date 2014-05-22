package nl.wisdelft.twitter.analytics.redundancy;

import java.util.ArrayList;

import nl.wisdelft.twitter.analytics.redundancy.model.TweetPair;
import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

/**
 * @author Ke Tao, <a href="mailto:k.tao@tudelft.nl">k.tao@tudelft.nl</a>
 * @author last edited by: ktao
 * This is a program that is used to detect duplicate with given classification model.
 * 
 * The OpenCalais, DBpedia Spotlight, or Wikipedia-Miner NER extraction should be 
 * ready before running this program.
 *  
 * @version created on May 23, 2013
 */
public class DuplicateDetection {
	
	private static DuplicateDetection instance = null;
	
	/** The classifier */
	private Classifier cls = null;
	
	/** List the useful features */
	private ArrayList<String> selectedFeatures = null;
	
	private FastVector fvWekaAttributes = null;
	
	private DuplicateDetection(String modelFilename, boolean[] usingFeatureSet) {
		try {
			// Load the classifier
			cls = (Classifier) weka.core.SerializationHelper.read(modelFilename);
			
			selectedFeatures = new ArrayList<String>();
			
			// Syntactical features - 6
			selectedFeatures.add("levensteinDistance");
			if (usingFeatureSet[0]) {
				selectedFeatures.add("overlapTerms");
				selectedFeatures.add("overlapHashtags");
				selectedFeatures.add("overlapURLs");
				selectedFeatures.add("overlapExtendedURLs");
				selectedFeatures.add("lengthDifference");
			}
			
			// Semantic features - 9
			if (usingFeatureSet[1]) {
				selectedFeatures.add("overlapDBpediaEntities");
				selectedFeatures.add("overlapDBpediaEntityTypes");
				selectedFeatures.add("overlapOpenCalaisEntities");
				selectedFeatures.add("overlapOpenCalaisTypes");
				selectedFeatures.add("overlapOpenCalaisTopic");	//
				selectedFeatures.add("overlapWPMEntities");
				selectedFeatures.add("overlapWordNetConcepts");
				selectedFeatures.add("overlapWordNetSynsetConcepts");
				selectedFeatures.add("wordNetSimilarity");
			}
			
			// Contextual features - 4
			if (usingFeatureSet[2]) {
				selectedFeatures.add("timeDifference");
				selectedFeatures.add("friendsDifference");
				selectedFeatures.add("followersDifference");
				selectedFeatures.add("sameClient");
			}
			
			// External Resources features - 9
			if (usingFeatureSet[3]) {
				selectedFeatures.add("overlapEnrichedDBpediaEntities");
				selectedFeatures.add("overlapEnrichedDBpediaEntityTypes");
				selectedFeatures.add("overlapEnrichedOpenCalaisEntities");
				selectedFeatures.add("overlapEnrichedOpenCalaisTypes");
				selectedFeatures.add("overlapEnrichedOpenCalaisTopic");
				selectedFeatures.add("overlapEnrichedWPMEntities");
				selectedFeatures.add("overlapEnrichedWordNetConcepts");
				selectedFeatures.add("overlapEnrichedWordNetSynsetConcepts");
				selectedFeatures.add("enrichedWordNetSimilarity");
			}
			
			// Express the problem with features
			Attribute[] attributes = new Attribute[selectedFeatures.size()];
			
			for (int i = 0; i < selectedFeatures.size(); i++) {
				attributes[i] = new Attribute(selectedFeatures.get(i));
			}
			
			// Declare the class attribute along with its values
			FastVector fvClassVal = new FastVector(2);
			fvClassVal.addElement("1");
			fvClassVal.addElement("0");
			Attribute classAttribute = new Attribute("judgement", fvClassVal);
			
			fvWekaAttributes = new FastVector(selectedFeatures.size() + 1); // number of features + 1 (class)
			for (Attribute attribute : attributes) {
				fvWekaAttributes.addElement(attribute);
			}
			fvWekaAttributes.addElement(classAttribute);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Loading model failed, reset instance to null.");
			instance = null;
		}
	}
	
	public static DuplicateDetection getInstance(String modelFilename, boolean[] featureSet) {
		if (instance == null)
			instance = new DuplicateDetection(modelFilename, featureSet);
		
		return instance;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// Which feature set do you want to use?
		boolean[] fs = new boolean[]{true, true, true, true};
		
		// Load a classification model
		DuplicateDetection detector = DuplicateDetection.getInstance("", fs);
		
		long tweetIdA = 297392846946832385L;
		long tweetIdB = 297309636133011457L;
		if(detector.detect(tweetIdA, tweetIdB))
			System.out.println("Duplicate found.");
		else
			System.out.println("Not duplicate.");
	}
	
	public boolean detect (long tweetIdA, long tweetIdB) {
		 try {
			// start to build an instance
			//System.out.println("The number of features: " + selectedFeatures.size());
			Instance iExample = new Instance(selectedFeatures.size() + 1);
			
			// Retrieve the content of two tweets
			TweetPair tPair = new TweetPair(tweetIdA, tweetIdB);
			
			int i = 0; // feature order in the instance
			if (selectedFeatures.contains("levensteinDistance")) {
				iExample.setValue(i, tPair.getLevenshteinDistance());
				i++;
			}
			
			if (selectedFeatures.contains("overlapTerms")) {
				iExample.setValue(i, tPair.getOverlapTerms());
				i++;
			}
			
			if (selectedFeatures.contains("overlapHashtags")) {
				iExample.setValue(i, tPair.getOverlapHashtags());
				i++;
			}
			
			if (selectedFeatures.contains("overlapURLs")) {
				iExample.setValue(i, tPair.getOverlapURLs());
				i++;
			}
			
			if (selectedFeatures.contains("overlapExtendedURLs")) {
				iExample.setValue(i, tPair.getOverlapExtendedURLs());
				i++;
			}
			
			if (selectedFeatures.contains("lengthDifference")) {
				iExample.setValue(i, tPair.getLengthDifference());
				i++;
			}
			
			// SEMANTIC FEATURES - 9
			if (selectedFeatures.contains("overlapDBpediaEntities")) {
				iExample.setValue(i, tPair.getOverlapDBpediaEntities());
				i++;
			}
			
			if (selectedFeatures.contains("overlapDBpediaEntityTypes")) {
				iExample.setValue(i, tPair.getOverlapDBpediaEntityTypes());
				i++;
			}
			
			if (selectedFeatures.contains("overlapOpenCalaisEntities")) {
				iExample.setValue(i, tPair.getOverlapOpenCalaisEntities());
				i++;
			}
			
			if (selectedFeatures.contains("overlapOpenCalaisTypes")) {
				iExample.setValue(i, tPair.getOverlapOpenCalaisTypes());
				i++;
			}
			
			if (selectedFeatures.contains("overlapOpenCalaisTopic")) {
				iExample.setValue(i, tPair.getOverlapOpenCalaisTopic());
				i++;
			}
			
			if (selectedFeatures.contains("overlapWPMEntities")) {
				iExample.setValue(i, tPair.getOverlapWPMEntities());
				i++;
			}
			
			if (selectedFeatures.contains("overlapWordNetConcepts")) {
				iExample.setValue(i, tPair.getOverlapWordNetConcepts());
				i++;
			}
			
			if (selectedFeatures.contains("overlapWordNetSynsetConcepts")) {
				iExample.setValue(i, tPair.getOverlapWordNetSynsetConcepts());
				i++;
			}
			
			if (selectedFeatures.contains("wordNetSimilarity")) {
				iExample.setValue(i, tPair.getWordNetSimilarity());
				i++;
			}
			
			// Contextual features - 4
			if (selectedFeatures.contains("timeDifference")) {
				iExample.setValue(i, tPair.getTimeDifference());
				i++;
			}
			
			if (selectedFeatures.contains("friendsDifference")) {
				iExample.setValue(i, tPair.getFriendsDifference());
				i++;
			}
			
			if (selectedFeatures.contains("followersDifference")) {
				iExample.setValue(i, tPair.getFollowersDifference());
				i++;
			}
			
			if (selectedFeatures.contains("sameClient")) {
				iExample.setValue(i, tPair.getSameClient());
				i++;
			}
			
			if (selectedFeatures.contains("overlapEnrichedDBpediaEntities")) {
				iExample.setValue(i, tPair.getOverlapEnrichedDBpediaEntities());
				i++;
			}
			
			if (selectedFeatures.contains("overlapEnrichedDBpediaEntityTypes")) {
				iExample.setValue(i, tPair.getOverlapEnrichedDBpediaEntityTypes());
				i++;
			}
			
			if (selectedFeatures.contains("overlapEnrichedOpenCalaisEntities")) {
				iExample.setValue(i, tPair.getOverlapEnrichedOpenCalaisEntities());
				i++;
			}
			
			if (selectedFeatures.contains("overlapEnrichedOpenCalaisTypes")) {
				iExample.setValue(i, tPair.getOverlapEnrichedOpenCalaisTypes());
				i++;
			}
			
			if (selectedFeatures.contains("overlapEnrichedOpenCalaisTopic")) {
				iExample.setValue(i, tPair.getOverlapEnrichedOpenCalaisTopic());
				i++;
			}
			
			if (selectedFeatures.contains("overlapEnrichedWPMEntities")) {
				iExample.setValue(i, tPair.getOverlapEnrichedWPMEntities());
				i++;
			}
			
			if (selectedFeatures.contains("overlapEnrichedWordNetConcepts")) {
				iExample.setValue(i, tPair.getOverlapEnrichedWordNetConcepts());
				i++;
			}
			
			if (selectedFeatures.contains("overlapEnrichedWordNetSynsetConcepts")) {
				iExample.setValue(i, tPair.getOverlapEnrichedWordNetSynsetConcepts());
				i++;
			}
			
			if (selectedFeatures.contains("enrichedWordNetSimilarity")) {
				iExample.setValue(i, tPair.getEnrichedWordNetSimilarity());
				i++;
			}
			
			Instances dataUnlabeled = new Instances("TestInstances", fvWekaAttributes, 0);
			dataUnlabeled.add(iExample);
			dataUnlabeled.setClassIndex(dataUnlabeled.numAttributes() - 1);
			
			double result = cls.classifyInstance(dataUnlabeled.firstInstance());
			
			tPair = null;
			iExample = null;
			dataUnlabeled = null;
			
			if (result == 0.0)
				return true; // 0.0 means positive, depending on the ARFF file with which you trained the model.
			else
				return false; // 1.0 means negative
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.err.println("Wrong execution.");
		return false;
	}
}
