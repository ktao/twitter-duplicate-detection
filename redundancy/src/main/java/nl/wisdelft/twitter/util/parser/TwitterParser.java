package nl.wisdelft.twitter.util.parser;


/**
 * @author Qi Gao <a href="mailto:q.gao@tudelft.nl">q.gao@tudelft.nl</a>
 * @version created on Nov 17, 2010 4:02:03 PM
 */
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TwitterParser {
	private static final String HASHREGEX = "\\s#[^\\s]+"; //regular expression for hashtags
	private static final String URLREGEX = "https?://([-A-Za-z0-9+&@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%=~_()|])"; //regular expression for URLs
	private static final String REPLYREGEX = "^@([a-z0-9_]{1,20})";

	public static ArrayList<String> getHashTags(String tweet) {
		Pattern p = Pattern.compile(HASHREGEX);
		Matcher m = p.matcher(tweet);
		ArrayList<String> hashtags = new ArrayList<String>();
		while (m.find()) {
			hashtags.add(m.group().replaceFirst("#", "").trim());
		}
		return hashtags;
	}
	
	public static ArrayList<String> getURLs(String tweet) {
		Pattern p = Pattern.compile(URLREGEX);
		Matcher m = p.matcher(tweet);
		ArrayList<String> uris = new ArrayList<String>();
		while (m.find()) {
			uris.add(m.group());
		}
		return uris;
	}
	
	public static ArrayList<String> getExtendedURLs(String tweet) {
		Pattern p = Pattern.compile(URLREGEX);
		Matcher m = p.matcher(tweet);
		ArrayList<String> urls = new ArrayList<String>();
		while (m.find()) {
			String url;
			String expandedURL = m.group();
			int counter = 0;
			do {
				counter++;
				url = expandedURL;
				expandedURL = expandURL(url);
			} while (expandedURL != null && counter <= 50);
			urls.add(url);
		}
		return urls;
	}
	
	public static String getExtendedURL(String url) {
		String expandedURL = url;
		
		int counter = 0;
		do {
			url = expandedURL;
			expandedURL = expandURL(url);
			counter ++;
		} while (expandedURL != null && counter <= 50);
		return url;
	}
	
	public static boolean isReply(String tweet) {
		Pattern p = Pattern.compile(REPLYREGEX, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(tweet);
		if(m.find()) 
			return true;
		else
			return false;
	}
	
	public static String expandURL(String locator) {
		try {
	        URL url = new URL(locator);
	 
	        HttpURLConnection connection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY); //using proxy may increase latency
	        connection.setReadTimeout(10000);
	        connection.setInstanceFollowRedirects(false);
	        connection.connect();
	        String expandedURL = connection.getHeaderField("Location");
	        connection.getInputStream().close();
	        return expandedURL;
		} catch (IOException e) {
			return null;
		} catch (Exception e) {
			return null;
		}
	}
	
	public static void main(String args[]) {
		for (String url : getExtendedURLs("http://goo.gl/kWYOI")) {
			System.out.println(url);
		}
		
		// two shortened links
		for (String url : getExtendedURLs("http://bit.ly/S9N8lb http://goo.gl/kWYOI")) {
			System.out.println(url);
		}
	}
	
	public static String removeNoneLetters(String tweet) {
		Pattern matchurl = Pattern.compile(URLREGEX);
		Matcher matcherurl = matchurl.matcher(tweet);
		tweet = matcherurl.replaceAll("");
		
		Pattern matchht = Pattern.compile(HASHREGEX);
		Matcher matcherht = matchht.matcher(tweet);
		tweet = matcherht.replaceAll("");
		
		Pattern matchre = Pattern.compile(REPLYREGEX);
		Matcher matcherre = matchre.matcher(tweet);
		tweet = matcherre.replaceAll("");
		
		String regex = "[^a-zA-Z\\s]";
        Pattern matchsip = Pattern.compile(regex);
        Matcher mp = matchsip.matcher(tweet);
        
        return mp.replaceAll("");
    }
}
