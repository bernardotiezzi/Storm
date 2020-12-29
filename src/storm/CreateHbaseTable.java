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

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.HTable;
import twitter4j.FilterQuery;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

//import javax.jms.ConnectionFactory;
public class CreateHbaseTable
{
    public static void main(String[] args) throws IOException {
        // instantiate Configuration class
                ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true)
                .setIncludeEntitiesEnabled(true)
                .setOAuthConsumerKey("YZcQGYa54T8Ueqg5tiXZjM3HZ")
                .setOAuthConsumerSecret("sDkH58gAGC9ePR0TTfVpKKH9i18aW84McUOUpvldpBPdqPmKvc")
                .setOAuthAccessToken("1284233485859860480-oIiyXxmeKodVRKpnuBchKejGIEA8gf")
                .setOAuthAccessTokenSecret("eWqrh4gQjh73pFM487aG128bqHeilBCVogGCPWYvR8hgY");
        TwitterStreamFactory tf = new TwitterStreamFactory(configurationBuilder.build());
        
        twitter4j.TwitterStream twitter = tf.getInstance();
        LinkedBlockingQueue<Status> tweets= new LinkedBlockingQueue<>();
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
        filter.track(query);
        twitter.filter(filter);
        tweets.poll();
        
        /*
        String[] sentences = new String[]{"#COVID19", "#BlackLivesMatter", "#JoeBiden2020", "#Israel", "#5G", "#BlackFriday"};
        Query q = new Query(sentences[2]);
        q.setCount(4);
        q.setLang("en");
        LocalDateTime date = LocalDateTime.of(2020, Month.NOVEMBER, 20, 20, 19);
        LocalDateTime date2 = LocalDateTime.of(2020, Month.DECEMBER, 27, 20, 19);
        System.out.println(date.toString());
        q.setSince(date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        q.setUntil(date2.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        String sentence ="";
        try {
            System.out.println("query: " + q.getQuery());
            QueryResult queryResult = twitter.;
            List<Status> status = queryResult.getTweets();
            sentence = status.get(0).getText();
            System.out.println(status.get(0).getCreatedAt().toString());
            System.out.println(status.get(1).getCreatedAt().toString());
        } catch (Exception e) {

        }
        */

        // close HTable instance
        
        /*
        Configuration hconfig = HBaseConfiguration.create();
        //hconfig.set("mapr.hbase.default.db","hbase");
        //hconfig.addResource("/usr/local/Hbase/hbase-1.3.6/conf/hbase-site.xml");
        hconfig.set("hbase.zookeeper.property.clientPort", "2222");
        //hconfig.set("hbase.zookeeper.quorum", "localhost");
                
        hconfig.set("hbase.cluster.distributed", "true");
        hconfig.set("hbase.rootdir", "hdfs://localhost:54310/hbase");
        HTableDescriptor htable = new HTableDescriptor(TableName.valueOf("QueryTweetsAnalyzed2"));
        htable.addFamily( new HColumnDescriptor("Id"));
        htable.addFamily( new HColumnDescriptor("Date"));
        htable.addFamily( new HColumnDescriptor("Tweet"));
        
        System.out.println( "Connecting..." );
        HBaseAdmin hbase_admin = new HBaseAdmin(hconfig);
        System.out.println( "Creating Table..." );
        hbase_admin.createTable( htable );
        System.out.println("Done!");
        */
        /*
        try{
            HTable table = new HTable(hconfig,"A");
            System.out.println(table.getName());
            
            Put p = new Put(Bytes.toBytes("row1"));
            
            p.add(Bytes.toBytes("Id"), Bytes.toBytes("col1"),Bytes.toBytes("AAA"));
            p.add(Bytes.toBytes("Date"),Bytes.toBytes("col2"),Bytes.toBytes("BBB"));
            
            table.put(p);
            
            Get g = new Get(Bytes.toBytes("row1"));
            Result r = table.get(g);
            
            byte [] value = r.getValue(Bytes.toBytes("Id"),Bytes.toBytes("col1"));
            byte [] value1 = r.getValue(Bytes.toBytes("Date"),Bytes.toBytes("col2"));
            
            String valueStr = Bytes.toString(value);
            String valueStr1 = Bytes.toString(value1);
            System.out.println("GET: " +"Id: "+ valueStr+"Date: "+valueStr1);
            
            Scan s = new Scan();
            s.addColumn(Bytes.toBytes("Id"), Bytes.toBytes("col1"));
            s.addColumn(Bytes.toBytes("Date"), Bytes.toBytes("col2"));
            ResultScanner scanner = table.getScanner(s);
            for (Result rnext = scanner.next(); rnext != null; rnext = scanner.next())
            {
                System.out.println("Found row : " + rnext);
            }
            scanner.close();
        }catch(Exception e){
            
            
            
            
          
        }

*/
    }
}
