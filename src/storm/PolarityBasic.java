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
import com.aliasi.util.Files;

import com.aliasi.classify.Classification;
import com.aliasi.classify.Classified;
import com.aliasi.classify.DynamicLMClassifier;

import com.aliasi.lm.NGramProcessLM;
import edu.stanford.nlp.pipeline.CoreNLPProtos;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import java.sql.Timestamp;
import java.util.ArrayList;
import org.apache.storm.utils.Utils;

public class PolarityBasic {

    File mPolarityDir;
    String[] mCategories;
    File mPolarityDir2;
    File mPolarityDir3;
    String [] mCategories2;
    DynamicLMClassifier<NGramProcessLM> mClassifier2;
    
    
    DynamicLMClassifier<NGramProcessLM> mClassifier;
    static final String DB_URL = "jdbc:derby://localhost:1527/MyDatabase";

    //  Database credentials
    static final String USER = "Bernardo";
    static final String PASS = "bernardo";

    PolarityBasic(String[] args) {
        System.out.println("\nBASIC POLARITY DEMO");
        mPolarityDir = new File("/home/bernardo/Scaricati/review_polarity","txt_sentoken");
        System.out.println("\nData Directory=" + mPolarityDir);
        mCategories = mPolarityDir.list();
        int nGram = 8;
        mClassifier 
            = DynamicLMClassifier
            .createNGramProcess(mCategories,nGram);
        mPolarityDir2 = new File("/home/bernardo/NetBeansProjects/Storm","train");
        mCategories2 = mPolarityDir2.list();
        mClassifier2 = DynamicLMClassifier.createNGramProcess(mCategories2, nGram);
        mPolarityDir3 = new File("/home/bernardo/NetBeansProjects/Storm","test");
    }

    public DynamicLMClassifier<NGramProcessLM> getmClassifier() {
        return mClassifier;
    }
    
    public DynamicLMClassifier<NGramProcessLM> getmClassifier2() {
        return mClassifier2;
    }
    
    

    void run() throws ClassNotFoundException, IOException {
        //train();
        //evaluate();
        train2();
        evaluate2();
    }

    boolean isTrainingFile(File file) {
        return file.getName().charAt(2) != '9';  // test on fold 9
    }

    void train() throws IOException {
        int numTrainingCases = 0;
        int numTrainingChars = 0;
        System.out.println("\nTraining.");
        for (int i = 0; i < mCategories.length; ++i) {
            String category = mCategories[i];
            Classification classification
                = new Classification(category);
            File file = new File(mPolarityDir,mCategories[i]);
            File[] trainFiles = file.listFiles();
            for (int j = 0; j < trainFiles.length; ++j) {
                File trainFile = trainFiles[j];
                if (isTrainingFile(trainFile)) {
                    ++numTrainingCases;
                    String review = Files.readFromFile(trainFile,"ISO-8859-1");
                    numTrainingChars += review.length();
                    Classified<CharSequence> classified
                        = new Classified<CharSequence>(review,classification);
                    mClassifier.handle(classified);
                }
            }
        }
        System.out.println("  # Training Cases=" + numTrainingCases);
        System.out.println("  # Training Chars=" + numTrainingChars);
    }
    
    void train2() throws IOException {
        int numTrainingCases = 0;
        int numTrainingChars = 0;
        System.out.println("\nTraining.");
        for (int i = 0; i < mCategories2.length; ++i) {
            String category = mCategories2[i];
            Classification classification
                = new Classification(category);
            File file = new File(mPolarityDir2,mCategories2[i]);
            File[] trainFiles = file.listFiles();
            File trainFile = trainFiles[0];
            ++numTrainingCases;
            String review = Files.readFromFile(trainFile,"ISO-8859-1");
            if(i==0){
                System.out.println(review);
            }
            numTrainingChars += review.length();
            Classified<CharSequence> classified
                = new Classified<CharSequence>(review,classification);
            mClassifier2.handle(classified);
        }
        
        
    }

    void evaluate() throws IOException {
        System.out.println("\nEvaluating.");
        int numTests = 0;
        int numCorrect = 0;
        for (int i = 0; i < mCategories.length; ++i) {
            String category = mCategories[i];
            File file = new File(mPolarityDir,mCategories[i]);
            File[] trainFiles = file.listFiles();
            for (int j = 0; j < trainFiles.length; ++j) {
                File trainFile = trainFiles[j];
                if (!isTrainingFile(trainFile)) {
                    String review = Files.readFromFile(trainFile,"ISO-8859-1");
                    ++numTests;
                    Classification classification
                        = mClassifier.classify(review);
                    //System.out.println(classification.getClass().getTypeName());
                    if (classification.bestCategory().equals(category))
                        ++numCorrect;
                }
            }
        }
        System.out.println("  # Test Cases=" + numTests);
        System.out.println("  # Correct=" + numCorrect);
        System.out.println("  % Correct=" 
                           + ((double)numCorrect)/(double)numTests);
    }

    void evaluate2() throws IOException {
        System.out.println("\nEvaluating.");
        int numTests = 0;
        int numCorrect = 0;
        for (int i = 0; i < mCategories2.length; ++i) {
            
            String category = mCategories2[i];
/*se uso il test demo devo usare questa trasformazione            
            if (category.equals("neg")){
                category="0";
            }else{
                category="4";
            }
        */
            File file = new File(mPolarityDir3,mCategories2[i]);
            File[] trainFiles = file.listFiles();
            for (int j = 0; j < trainFiles.length; ++j) {
                File trainFile = trainFiles[j];
                if (/*isTrainingFile(trainFile)||(isTrainingFile(file))*/true){
                    String review = Files.readFromFile(trainFile,"ISO-8859-1");
                    ++numTests;
                    Classification classification
                        = mClassifier2.classify(review);
                    //System.out.println(classification.getClass().getTypeName());
                    if (classification.bestCategory().equals(category))
                        ++numCorrect;
                }
            }
        }
        System.out.println("  # Test Cases=" + numTests);
        System.out.println("  # Correct=" + numCorrect);
        System.out.println("  % Correct=" 
                           + ((double)numCorrect)/(double)numTests);
    }
    
    void createDataset(){
        Connection conn = null;
        Statement stmt = null;
        try {
            //STEP 2: Register JDBC driver
            //Class.forName("com.mysql.jdbc.Driver");

            //STEP 3: Open a connection
            System.out.println("Connecting to a selected database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected database successfully...");

            //STEP 4: Execute a query
            System.out.println("Creating table in given database...");
            stmt = conn.createStatement();
            //String query="select TWEET_ID, AIRLINE_SENTIMENT, NAME, TEXT from AIRLINE";
            //String query="select distinct * from LABELEDTWEETS";
            String q="select distinct TEXT from LABELEDTWEETS2";
            ArrayList<String> listtweets=new ArrayList<>();
            ResultSet rs = stmt.executeQuery(q);
            while(rs.next()){
                listtweets.add(rs.getString("TEXT").toString());
            }
            String query="select distinct * from LABELEDTWEETS2";
            rs = stmt.executeQuery(query);
            int c0=0;
            int c2=0;
            int c4=0;
            int f0=0;
            int f2=0;
            int f4=0;
            
            File pos = new File("/home/bernardo/NetBeansProjects/Storm/train/4/trainPositive"+String.valueOf(f4)+".txt");
            File neu = new File("/home/bernardo/NetBeansProjects/Storm/train/2/trainNeutral"+String.valueOf(f2)+".txt");
            File neg = new File("/home/bernardo/NetBeansProjects/Storm/train/0/trainNegative"+String.valueOf(f0)+".txt");
            FileWriter w4 = new FileWriter("/home/bernardo/NetBeansProjects/Storm/train/4/trainPositive"+String.valueOf(f4)+".txt");
            FileWriter w2 = new FileWriter("/home/bernardo/NetBeansProjects/Storm/train/2/trainNeutral"+String.valueOf(f2)+".txt");
            FileWriter w0 = new FileWriter("/home/bernardo/NetBeansProjects/Storm/train/0/trainNegative"+String.valueOf(f0)+".txt");
            
            /*
            File pos = new File("/home/bernardo/NetBeansProjects/Storm/test/4/testPositive"+String.valueOf(f4)+".txt");
            File neu = new File("/home/bernardo/NetBeansProjects/Storm/test/2/testNeutral"+String.valueOf(f2)+".txt");
            File neg = new File("/home/bernardo/NetBeansProjects/Storm/test/0/testNegative"+String.valueOf(f0)+".txt");
            FileWriter w4 = new FileWriter("/home/bernardo/NetBeansProjects/Storm/test/4/testPositive"+String.valueOf(f4)+".txt");
            FileWriter w2 = new FileWriter("/home/bernardo/NetBeansProjects/Storm/test/2/testNeutral"+String.valueOf(f2)+".txt");
            FileWriter w0 = new FileWriter("/home/bernardo/NetBeansProjects/Storm/test/0/testNegative"+String.valueOf(f0)+".txt");
            */
            while(rs.next()){
                String currentTweet=rs.getString("TEXT").toString();
                if(listtweets.contains(currentTweet) && (rs.getString("LABEL").equals("4")||rs.getString("LABEL").equals("3"))){//rs.getString("AIRLINE_SENTIMENT").equals("4")
                    w4.write(rs.getString("TEXT")+"\n");
                    c4+=1;
                    listtweets.remove(currentTweet);
                }else if(listtweets.contains(currentTweet) && rs.getString("LABEL").equals("2")){//rs.getString("AIRLINE_SENTIMENT").equals("2")
                    w2.write(rs.getString("TEXT")+"\n");
                    c2+=1;
                    listtweets.remove(currentTweet);
                }else if(listtweets.contains(currentTweet)){
                    w0.write(rs.getString("TEXT")+"\n");
                    c0+=1;
                    listtweets.remove(currentTweet);
                }
                if(c0==20){
                    w0.close();
                    f0+=1;
                    //neg = new File("/home/bernardo/NetBeansProjects/Storm/test/0/testNegative"+String.valueOf(f0)+".txt");
                    //w0 = new FileWriter("/home/bernardo/NetBeansProjects/Storm/test/0/testNegative"+String.valueOf(f0)+".txt");
                    neg = new File("/home/bernardo/NetBeansProjects/Storm/train/0/trainNegative"+String.valueOf(f0)+".txt");
                    w0 = new FileWriter("/home/bernardo/NetBeansProjects/Storm/train/0/trainNegative"+String.valueOf(f0)+".txt");
                    c0=0;
                }
                if(c2==20){
                    w2.close();
                    f2+=1;
                    //neu = new File("/home/bernardo/NetBeansProjects/Storm/test/2/testNeutral"+String.valueOf(f2)+".txt");
                    //w2 = new FileWriter("/home/bernardo/NetBeansProjects/Storm/test/2/testNeutral"+String.valueOf(f2)+".txt");
                    neu = new File("/home/bernardo/NetBeansProjects/Storm/train/2/trainNeutral"+String.valueOf(f2)+".txt");
                    w2 = new FileWriter("/home/bernardo/NetBeansProjects/Storm/train/2/trainNeutral"+String.valueOf(f2)+".txt");
                
                    c2=0;
                }
                
                if(c4==20){
                    w4.close();
                    f4+=1;
                    //pos = new File("/home/bernardo/NetBeansProjects/Storm/test/4/testPositive"+String.valueOf(f4)+".txt");
                    //w4 = new FileWriter("/home/bernardo/NetBeansProjects/Storm/test/4/testPositive"+String.valueOf(f4)+".txt");
                    
                    pos = new File("/home/bernardo/NetBeansProjects/Storm/train/4/trainPositive"+String.valueOf(f4)+".txt");
                    w4 = new FileWriter("/home/bernardo/NetBeansProjects/Storm/train/4/trainPositive"+String.valueOf(f4)+".txt");
                    c4=0;
                }
                
            }
            w0.close();
            w2.close();
            w4.close();

        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null) {
                    conn.close();
                }
            } catch (SQLException se) {
            }// do nothing
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }//end try
        System.out.println("Goodbye!");
    }
    
    public static void main(String[] args) {
        
    /*
        try {
           new PolarityBasic(args).run();
        } catch (Throwable t) {
            System.out.println("Thrown: " + t);
            t.printStackTrace(System.out);
        }
       */
        try{
            new PolarityBasic(args).createDataset();
        } catch (Throwable t) {
            System.out.println("Thrown: " + t);
            t.printStackTrace(System.out);
        }

        /*
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey("YZcQGYa54T8Ueqg5tiXZjM3HZ")
                .setOAuthConsumerSecret("sDkH58gAGC9ePR0TTfVpKKH9i18aW84McUOUpvldpBPdqPmKvc")
                .setOAuthAccessToken("1284233485859860480-oIiyXxmeKodVRKpnuBchKejGIEA8gf")
                .setOAuthAccessTokenSecret("eWqrh4gQjh73pFM487aG128bqHeilBCVogGCPWYvR8hgY");

        TwitterFactory tf = new TwitterFactory(configurationBuilder.build());
        twitter4j.Twitter twitter = tf.getInstance();
        
        Connection conn = null;
        Statement stmt = null;
        try {
            //STEP 2: Register JDBC driver
            //Class.forName("com.mysql.jdbc.Driver");

            //STEP 3: Open a connection
            System.out.println("Connecting to a selected database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected database successfully...");

            //STEP 4: Execute a query
            System.out.println("Creating table in given database...");
            stmt = conn.createStatement();
            */
            /* Script per differenza tra tweet esistenti e non
            String query="select * from HIGHNEUTRALTWEET";
            ResultSet rs = stmt.executeQuery(query);
            int c=0;
            while (rs.next() && c<13359){
                if (rs.getString("TWEET_ID").equals("1241480350448193538")){
                    System.out.println(rs.getString("TWEET_ID"));
                    System.out.println(c);
                    
                    
                }
                c+=1;
                
            }
            System.out.println(c);
            Fine Script */

            /*
            String query="select TWEET_ID, USER_ID, NEUTRAL_PROBABILITY, LABEL from NEUTRAL WHERE NEUTRAL_PROBABILITY LIKE '0.999%'";
            ResultSet rs = stmt.executeQuery(query);
            int count=0;
            int count2=0;
            //Aggiunto per proseguire l'inserimento interrotto
            while (rs.next() && count2<15300){
                if (rs.getString("TWEET_ID").equals("1241480350448193538")){
                    System.out.println(rs.getString("TWEET_ID"));
                    System.out.println(count2);
                    
                    
                }
                if (count2!=15299){
                    count2+=1;
                }else{
                    break;
                }
            }
            System.out.println(count2);
            count=13358;
            //Fine aggiunto
            Timestamp t1 = new Timestamp(System.currentTimeMillis());
            while (rs.next() && count<50000) {
                String tweet_id = rs.getString("TWEET_ID");
                count2+=1;
                try {
                    Status status = twitter.showStatus(Long.parseLong(tweet_id));
                    if (status == null) { // 
                        // don't know if needed - T4J docs are very bad
                    } else {
                        Timestamp t2=new Timestamp(System.currentTimeMillis());
                        Statement stmt2 = null;
                        stmt2=conn.createStatement();
                        System.out.println("@" + status.getUser().getScreenName()
                                + " - " + status.getText());
                        String text = status.getText();
                        String tmp = "'";
                        char tm = tmp.charAt(0);
                        int i = 0;
                        while (i < text.length()) {
                            char c = text.charAt(i);
                            if (tm == c) {
                                String str2 = text.substring(i);
                                //System.out.println("str2"+str2);
                                String str1 = text.substring(0, i);
                            //System.out.println("str1"+str1);
                                //System.out.println(text);
                                text = str1 + "'" + str2;
                                i += 1;

                            }

                            i += 1;

                        }
                        String sql = "INSERT INTO HIGHNEUTRALTWEET (TWEET_ID,USER_ID,NEUTRAL_PROBABILITY,TEXT,LABEL) "
                            + "VALUES('" + tweet_id + "','" + rs.getString("USER_ID") + "',"+rs.getString("NEUTRAL_PROBABILITY")+",'" + text+ "','Neutral'" + ")";
                        System.out.println(sql);
                        stmt2.executeUpdate(sql);
                        count+=1;
                        if(count2%850==0){
                            long offset=t2.getTime()-t1.getTime();
                            if (offset<960000){
                                System.out.println("attendi "+ String.valueOf(960000-offset) + "msec: \n");
                                Utils.sleep(960000-offset);
                            }
                            t1=new Timestamp(System.currentTimeMillis());
                        }
                    }
                } catch (TwitterException e) {
                    System.err.print("Failed to search tweets: " + e.getMessage());
        // e.printStackTrace();
                    // DON'T KNOW IF THIS IS THROWN WHEN ID IS INVALID
                    if (e.getMessage().contains("429:Returned in API v1.1 when a request cannot be served due to the application's rate limit")){
                        System.out.println("contatore tweet ID scorsi: "+String.valueOf(count2));
                        Utils.sleep(600000);
                    }
                }

            }
            System.out.println("contatore tweet ID scorsi: "+ String.valueOf(count2));
            
            
        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null) {
                    conn.close();
                }
            } catch (SQLException se) {
            }// do nothing
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }//end try
        */
        
        /*
        String tweetID="1242437817910865920";
        try {
            Status status = twitter.showStatus(Long.parseLong(tweetID));
            if (status == null) { // 
                // don't know if needed - T4J docs are very bad
            } else {
                System.out.println("@" + status.getUser().getScreenName()
                        + " - " + status.getText());
            }
        } catch (TwitterException e) {
            System.err.print("Failed to search tweets: " + e.getMessage());
        // e.printStackTrace();
            // DON'T KNOW IF THIS IS THROWN WHEN ID IS INVALID
        }
        */
        
        
        

        // TODO code application logic here
              
        
    }
    
    
}
