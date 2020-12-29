/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm;

/**
 *
 * @author bernardo
 */

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import java.util.ArrayList;
import java.sql.*;

public class NLP {
	static StanfordCoreNLP pipeline;

	public static void init() {
		pipeline = new StanfordCoreNLP("MyPropFile.properties");
	}

	public static int findSentiment(String tweet) {

		int mainSentiment = 0;
		if (tweet != null && tweet.length() > 0) {
			int longest = 0;
			Annotation annotation = pipeline.process(tweet);
			for (CoreMap sentence : annotation
					.get(CoreAnnotations.SentencesAnnotation.class)) {
				Tree tree = sentence
						.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
				int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
				String partText = sentence.toString();
				if (partText.length() > longest) {
					mainSentiment = sentiment;
					longest = partText.length();
				}

			}
		}
		return mainSentiment;
	}
        
        static final String DB_URL = "jdbc:derby://localhost:1527/MyDatabase";

        //  Database credentials
        static final String USER = "Bernardo";
        static final String PASS = "bernardo";
    
        public static void main(String[] args) {
            Connection conn = null;
            Statement stmt = null;
            
            try{
            
                conn = DriverManager.getConnection(DB_URL, USER, PASS);
                stmt = conn.createStatement();
                stmt = conn.createStatement();

                stmt.executeQuery(" select TEXT from TWEETS where ID>=100");
                ResultSet rs = stmt.getResultSet();
                System.out.println(rs);
                ArrayList<String> tweets=new ArrayList<>();
                while(rs.next()) {
                    //System.out.println(rs.getString("TEXT"));
                    tweets.add(rs.getString("TEXT"));
                }
                NLP.init();
                tweets.forEach((tweet) -> {
                    System.out.println(tweet + " : " + NLP.findSentiment(tweet));
                }); /*
                String topic = "ICCT20WC";
                ArrayList<String> tweets = TweetManager.getTweets(topic);
                NLP.init();
                for(String tweet : tweets) {
                System.out.println(tweet + " : " + NLP.findSentiment(tweet));
                }*/
            
            }catch(SQLException se){
           //Handle errors for JDBC
                se.printStackTrace();
            }catch(Exception e){
           //Handle errors for Class.forName
                e.printStackTrace();
        }finally{
           //finally block used to close resources
           try{
              if(stmt!=null)
                 conn.close();
           }catch(SQLException se){
           }// do nothing
           try{
              if(conn!=null)
                 conn.close();
           }catch(SQLException se){
              se.printStackTrace();
           }
                
        
        }
    }
}