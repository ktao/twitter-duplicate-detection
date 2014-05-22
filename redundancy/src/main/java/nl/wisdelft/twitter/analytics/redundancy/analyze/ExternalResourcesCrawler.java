package nl.wisdelft.twitter.analytics.redundancy.analyze;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import nl.wisdelft.twitter.io.DBUtility;
import nl.wisdelft.twitter.util.parser.TwitterParser;

import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;
import org.springframework.jdbc.object.SqlUpdate;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.ArticleExtractor;

/**
 * @author ktao
 *
 */
public class ExternalResourcesCrawler {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SqlUpdate suer = new SqlUpdate(DBUtility.ds,
				"INSERT INTO externalResources_www2013 (url, tweetId, userId, title, newscontent, publish_date, crawl_date) VALUE(?, ?, ?, ?, ?, ?, ?)");
		suer.declareParameter(new SqlParameter("url", Types.VARCHAR));
		suer.declareParameter(new SqlParameter("tweetId", Types.BIGINT));
		suer.declareParameter(new SqlParameter("userId", Types.INTEGER));
		suer.declareParameter(new SqlParameter("title", Types.VARCHAR));
		suer.declareParameter(new SqlParameter("newscontent", Types.VARCHAR));
		suer.declareParameter(new SqlParameter("publish_date", Types.TIMESTAMP));
		suer.declareParameter(new SqlParameter("crawl_date", Types.TIMESTAMP));
		suer.compile();
		
		long pointer = 0l;
		
		if (args.length == 1) {
			pointer = Long.parseLong(args[0]);
		}
		
		int counter = 0;
		int loop = 0;
		
		do {
			loop = 0;
//			String qTweets = "SELECT t.id, t.userId, t.content, t.creationTime FROM tweets_www2013 t, qrel_original q WHERE t.language = 'English' AND t.id > " + pointer + " AND t.id = q.tweetId AND q.topicId = 22 ORDER BY t.id ASC LIMIT 1000";
			String qTweets = "SELECT id, t.userId, content, creationTime FROM tweets_www2013 WHERE id > " + pointer + " ORDER BY t.id ASC LIMIT 1000";
	
			ResultSet tweets = DBUtility.executePrepareQuerySingleConnection(qTweets);
			
			try {
				while(tweets.next()) {
					loop++;
					counter++;
						
					long id = tweets.getLong("id");
					int userId = tweets.getInt("userId");
					String content = tweets.getString("content");
					Timestamp creationTime = tweets.getTimestamp("creationTime");
					
					System.out.println("Processing tweet (" + id + "):" + content);
					
					for (String uri : TwitterParser.getURLs(content)) {
						String text = null;
						try {
							text = ArticleExtractor.getInstance().getText(new URL(uri));
						} catch (BoilerpipeProcessingException e) {
							System.err.println("Processing Error: " + uri + " @ tweetId : " + id);
							continue;
						} catch (MalformedURLException e) {
							System.err.println("Malformed URL: " + uri + " @ tweetId : " + id);
							continue;
						} catch (Exception e) {
							System.err.println("Other Error URL: " + uri + " @ tweetId : " + id);
							continue;
						}
						
						if (text != null) { // proceed semantic enrichment
							suer.update(new Object[] {
								uri,
								id,
								userId,
								"",
								text,
								creationTime,
								new Timestamp(System.currentTimeMillis())
							});
						}
					}
					
					if (counter % 50 == 0) {
						System.out.println(counter + " tweets processed.");
						pointer = id;
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} while (loop % 1000 == 0);
	}

}
