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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;

import nl.wisdelft.twitter.util.parser.TwitterParser;
import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.HirstStOnge;
import edu.cmu.lti.ws4j.impl.JiangConrath;
import edu.cmu.lti.ws4j.impl.LeacockChodorow;
import edu.cmu.lti.ws4j.impl.Lesk;
import edu.cmu.lti.ws4j.impl.Lin;
import edu.cmu.lti.ws4j.impl.Path;
import edu.cmu.lti.ws4j.impl.Resnik;
import edu.cmu.lti.ws4j.impl.WuPalmer;
import edu.mit.jwi.Dictionary;
import edu.mit.jwi.IDictionary;
import edu.mit.jwi.item.IIndexWord;
import edu.mit.jwi.item.IWord;
import edu.mit.jwi.item.IWordID;
import edu.mit.jwi.item.POS;
import edu.mit.jwi.morph.WordnetStemmer;

/**
 * This class works for WordNet-related features
 * 
 * @author Ke Tao, <a href="mailto:k.tao@tudelft.nl">k.tao@tudelft.nl</a>
 * @author last edited by: ktao
 * 
 * @version created on Nov 5, 2012
 */
public class WordNetRelatedSimilarity {
	
	private IDictionary dict;
	private WordnetStemmer wnstemmer;
	private static WordNetRelatedSimilarity instance;
	
	// ws4j
	private static ILexicalDatabase db;
	private static RelatednessCalculator[] rcs;
	
	public static WordNetRelatedSimilarity getInstance() {
		if (instance == null) {
			instance = new WordNetRelatedSimilarity();
		}
		return instance;
	}
	
	// RelatednessCalculator
	public static final int RCS_HIRSTSTONGE = 0;
	public static final int RCS_LEACOCKCHODOROW = 1;
	public static final int RCS_LESK = 2;
	public static final int RCS_WUPALMER = 3;
	public static final int RCS_RESNIK = 4;
	public static final int RCS_JIANGCONRATH = 5;
	public static final int RCS_LIN = 6;
	public static final int RCS_PATH = 7;
	
	
	private WordNetRelatedSimilarity() {
		// construct the URL to the Wordnet dictionary directory
		String wnhome = System.getenv("WNHOME");
		String path = wnhome + File.separator + "dict";
		URL url = null;
		try {
			url = new URL("file", null, path);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		
		if (url == null)
			return;
		
		dict = new Dictionary(url);
		
		// construct the dictionary object and open it
		try {
			dict.open();
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Error occured while trying to open the dictionary!");
		}
		
		wnstemmer = new WordnetStemmer(dict);
		
		db = new NictWordNet();
		
		rcs = new RelatednessCalculator[] {
				new HirstStOnge(db), new LeacockChodorow(db), new Lesk(db),  new WuPalmer(db), 
				new Resnik(db), new JiangConrath(db), new Lin(db), new Path(db)
        };
	}
	
	/**
	 * calculate the relatedness between wordA and wordB by using algorithm RCS (integer, defined at the beginning)
	 * @param wordA
	 * @param wordB
	 * @param RCS
	 * @return
	 */
	public double getRelatednessOfWords(String wordA, String wordB, int RCS) {
		if (wordA.equals(wordB))
			return 1.0;
		else
			return rcs[RCS].calcRelatednessOfWords(wordA, wordB);
	}
	
	/**
	 * 
	 * @param tweetA
	 * @param tweetB
	 * @param RCS
	 * @return
	 */
	public double getWordNetSimilarityBasedOnWS4J(String tweetA, String tweetB, int RCS) {
		tweetA = TwitterParser.removeNoneLetters(tweetA);
		tweetB = TwitterParser.removeNoneLetters(tweetB);
		
		String[] wordsA = tweetA.split("\\s+");
		HashSet<String> wordsetA = new HashSet<String>();
		for (String wordA : wordsA) {
			IWord wnWord = getStemmedIWord(wordA, POS.NOUN); 
			if (wnWord != null)
				wordsetA.add(wnWord.getLemma());
		}
		
		String[] wordsB = tweetB.split("\\s+");
		HashSet<String> wordsetB = new HashSet<String>();
		for (String wordB : wordsB) {
			IWord wnWord = getStemmedIWord(wordB, POS.NOUN); 
			if (wnWord != null)
				wordsetB.add(wnWord.getLemma());
		}
		
		int counter = 0;
		double total = 0;
		if (wordsetA.size() > wordsetB.size()) {
			for (String wordB : wordsetB) {
				double maxRelatedness = -1.0;
				String maxWordA = null;
				for (String wordA : wordsetA) {
					double relatedness = getRelatednessOfWords(wordA, wordB, RCS);
//					System.out.println("Relatedness : " + relatedness);
//					System.out.println("wordA : " + wordA);
//					System.out.println("wordB : " + wordB);
					if (relatedness > maxRelatedness) {
						maxRelatedness = relatedness;
						maxWordA = wordA;
					}
				}
				
				if (maxWordA != null) { // remove
					wordsetA.remove(maxWordA);
					counter ++;
					total +=  maxRelatedness;
				}
			}
		} else {
			for (String wordA : wordsetA) {
				double maxRelatedness = -1.0;
				String maxWordB = null;
				for (String wordB : wordsetB) {
					double relatedness = getRelatednessOfWords(wordA, wordB, RCS);
//					System.out.println("Relatedness : " + relatedness);
//					System.out.println("wordA : " + wordA);
//					System.out.println("wordB : " + wordB);
					if (relatedness > maxRelatedness) {
						maxRelatedness = relatedness;
						maxWordB = wordB;
					}
				}
				
				if (maxWordB != null) { // remove
					wordsetB.remove(maxWordB);
					counter ++;
					total +=  maxRelatedness;
				}
			}
		}
		if (counter == 0)
			return 0.0;
//		System.out.println("counter : " + counter);
//		System.out.println("total : " + total);
		return total / counter;
	}
	
	public double getWordNetSimilarityBasedOnWords(String tweetA, String tweetB) {
		tweetA = TwitterParser.removeNoneLetters(tweetA);
		tweetB = TwitterParser.removeNoneLetters(tweetB);
		
		String[] wordsA = tweetA.split("\\s+");
		HashSet<String> wordsetA = new HashSet<String>();
		for (String wordA : wordsA) {
			IWord wnWord = getStemmedIWord(wordA, POS.NOUN); 
			if (wnWord != null)
				wordsetA.add(wnWord.getLemma());
		}
		
		String[] wordsB = tweetB.split("\\s+");
		HashSet<String> wordsetB = new HashSet<String>();
		for (String wordB : wordsB) {
			IWord wnWord = getStemmedIWord(wordB, POS.NOUN); 
			if (wnWord != null)
				wordsetB.add(wnWord.getLemma());
		}
		
		int counter = 0;
		for (String wordB : wordsetB) {
			if (wordsetA.contains(wordB))
				counter++;
		}
		
		if (wordsetA.size() + wordsetB.size() == 0) {
			return 0.0;
		} else {
			return (double) counter / (double) (wordsetA.size() + wordsetB.size() - counter);
		}
	}
	
	/**
	 * Based on the overlapping in terms of concepts that can be found by referring the nouns
	 * in both tweets.
	 * @param tweetA
	 * @param tweetB
	 * @return the ratio of overlapping concepts in synsets between two tweets.
	 */
	public double getWordNetSimilarityBasedOnSynsets(String tweetA, String tweetB) {
		tweetA = TwitterParser.removeNoneLetters(tweetA);
		tweetB = TwitterParser.removeNoneLetters(tweetB);
		
		String[] wordsA = tweetA.split("\\s+");
		HashSet<String> synwordsA = new HashSet<String>();
		for (String wordA : wordsA) {
			IWord wnWord = getStemmedIWord(wordA, POS.NOUN); 
			if (wnWord != null) {
				for (IWord word : wnWord.getSynset().getWords()) {
					synwordsA.add(word.getLemma());
				}
			}
		}
		
		String[] wordsB = tweetB.split("\\s+");
		HashSet<String> synwordsB = new HashSet<String>();
		for (String wordB : wordsB) {
			IWord wnWord = getStemmedIWord(wordB, POS.NOUN); 
			if (wnWord != null) {
				for (IWord word : wnWord.getSynset().getWords()) {
					synwordsB.add(word.getLemma());
				}
			}
		}
		
		int counter = 0;
		for (String wordB : synwordsB) {
			if (synwordsA.contains(wordB))
				counter++;
		}
		
		if (synwordsA.size() + synwordsB.size() == 0) {
			return 0.0;
		}
		
		return (double) counter / (double) (synwordsA.size() + synwordsB.size() - counter);
	}
	
	public List<String> getStemmedWords(String word, POS pos) {
		return wnstemmer.findStems(word, pos);
	}
	
	public String getStemmedWord(String word, POS pos) {
		return wnstemmer.findStems(word, pos).get(0);
	}
	
	public String getStemmedWord(String word, POS pos, int index) {
		return wnstemmer.findStems(word, pos).get(index);
	}
	
	public IIndexWord getIndexWord(String word, POS pos) {
		return dict.getIndexWord(word, pos);
	}
	
	public IWordID getWordID(IIndexWord idxWord) {
		return idxWord.getWordIDs().get(0);
	}
	
	public IWord getWord(IWordID wordID) {
		return dict.getWord(wordID);
	}
	
	public IWord getStemmedIWord(String word, POS pos) {
		String stemmedWord = null;
		try {
			stemmedWord = wnstemmer.findStems(word, pos).get(0);
		} catch(Exception e) {
			//System.err.println("Error: the word is:" + word);
			return null;
		}
		IIndexWord idxWord = dict.getIndexWord(stemmedWord, pos);
		IWordID wordID = null;
		if (idxWord != null) {
			wordID = idxWord.getWordIDs().get(0);
		} else
			return null;
		return dict.getWord(wordID);
	}
	
	/**
	 * Used for test purpose
	 * @param args
	 */
	public static void main(String[] args) {		
		// look up first sense of the word "dog"
		/*
		String original = "dog's";
		
		IWord word = WordNetRelatedSimilarity.getInstance().getStemmedIWord(original, POS.NOUN);
		System.out.println("Lemma = " + word.getLemma());
		System.out.println("Gloss = " + word.getSynset().getGloss());
		
		ISynset synset = word.getSynset();
		
		for (IWord w : synset.getWords()) {
			System.out.println(w.getLemma());
		}
		*/
		
		String tweetA = "BBC World Service axes five languages, 650 jobs (AFP): AFP - The BBC World Service said Wednesday it would ... Http://bit.ly/f1uNCb";
		String tweetB = "BBC World Service axes five language services (AFP) - AFP - The BBC World Service has said it will close five o... http://ow.ly/1b23Gf";
		System.out.println("Word-based: Similarity score: " + WordNetRelatedSimilarity.getInstance().getWordNetSimilarityBasedOnWords(tweetA, tweetB));
		System.out.println("Synset-based: Similarity score: " + WordNetRelatedSimilarity.getInstance().getWordNetSimilarityBasedOnSynsets(tweetA, tweetB));
	}
}
