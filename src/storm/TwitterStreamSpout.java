/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.codec.language.bm.Languages;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.FilterQuery;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author bernardo
 */
public class TwitterStreamSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    Random _rand;
    //private twitter4j.Twitter twitter;
    private twitter4j.TwitterStream twitter;
    private LinkedBlockingQueue<Status> tweets;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey("YZcQGYa54T8Ueqg5tiXZjM3HZ")
                .setOAuthConsumerSecret("sDkH58gAGC9ePR0TTfVpKKH9i18aW84McUOUpvldpBPdqPmKvc")
                .setOAuthAccessToken("1284233485859860480-oIiyXxmeKodVRKpnuBchKejGIEA8gf")
                .setOAuthAccessTokenSecret("eWqrh4gQjh73pFM487aG128bqHeilBCVogGCPWYvR8hgY");
        TwitterStreamFactory tf = new TwitterStreamFactory(configurationBuilder.build());
        twitter = tf.getInstance();
        tweets= new LinkedBlockingQueue<>();
        final StatusListener statusListener = new StatusListener() {

                    @Override
                    public void onStatus(Status status) {
                        tweets.offer(status);
                    }

                    @Override
                    public void onDeletionNotice(StatusDeletionNotice sdn) {
                    }

                    @Override
                    public void onTrackLimitationNotice(int i) {
                     
                    }

                    @Override
                    public void onScrubGeo(long l, long l1) {
                     }

                    @Override
                    public void onStallWarning(StallWarning sw) {
                     }

                    @Override
                    public void onException(Exception excptn) {
                     }
                };
        twitter.addListener(statusListener);
        FilterQuery filter=new FilterQuery();
        String[] query={"covid"};
        filter.language("en");
        filter.track(query);
        twitter.filter(filter);
    }

    @Override
    public void nextTuple() {
        
        Utils.sleep(100);
        String sentence = "";
        /*

        String[] sentences = new String[]{"#COVID19", "#BlackLivesMatter", "#JoeBiden2020", "#Israel", "#5G", "#BlackFriday"};//new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
        //"four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
        String[] sentences2=new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
        "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
        Query q = new Query(sentences[_rand.nextInt(sentences.length)]);
        q.setCount(1);
        q.setLang("en");
        System.out.println(q.getSince());
        String sentence = "bonanotte!";
        */
        try {
            /*
            System.out.println("query: " + q.getQuery());
            QueryResult queryResult = twitter.search(q);
            List<Status> status = queryResult.getTweets();
            
            sentence = status.get(0).getText();
            */
            sentence=tweets.poll().toString();
        } catch (Exception e) {

        }
        if(!sentence.equals("")){
            System.out.println("frase: " + sentence);
            _collector.emit(new Values(/*q.getQuery()*/"covid", sentence));

            
        }else{
            try {
                Thread.sleep(500);
                
            }catch(Exception e){
                
            }
            
        }
        /*while (sentence.isEmpty()) {
            //sentence=sentences2[_rand.nextInt(sentences2.length)];
            
            try {
                QueryResult queryResult = twitter.search(q);
                List<Status> status = queryResult.getTweets();
                sentence = status.get(0).getText();

            } catch (Exception e) {
                q = new Query(sentences[_rand.nextInt(sentences.length)]);
                q.setCount(1);
                q.setLang("en");
                System.out.println("query interna: " + q.getQuery());
            }
        }*/
        
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("query", "sentence"));
    }
    
    @Override
    public final void close() {
        this.twitter.cleanUp();
        this.twitter.shutdown();
    }

}
