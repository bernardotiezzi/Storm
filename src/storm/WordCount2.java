/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 *
 * @author bernardo
 */
public class WordCount2 extends BaseBasicBolt {
    HTable table;
    String lastrow="";
    int count=0;
    
    /*
     Map<String, Integer> counts = new HashMap<>();
    

     @Override
     public void execute(Tuple tuple, BasicOutputCollector collector) {
     String word = tuple.getString(0);
     //System.out.println("word execute: " + word);
     Integer count = counts.get(word);
     if (count == null) {
     count = 0;
     }
     count++;
     counts.put(word, count);
     collector.emit(new Values(word, count));
     //System.out.println("wordcount execute: "+word+" "+count);
     }

     @Override
     public void declareOutputFields(OutputFieldsDeclarer declarer) {
     declarer.declare(new Fields("word", "count"));
     }*/

    //Create logger for this class
    private static final Logger logger = LogManager.getLogger(WordCount2.class);
    //For holding words and counts
    Map<String, Integer> counts = new HashMap<>();
    //How often to emit a count of words
    private Integer emitFrequency;

    // Default constructor
    public WordCount2() {
        //emitFrequency=5; // Default to 60 seconds
    }

    // Constructor that sets emit frequency
    public WordCount2(Integer frequency) {
        emitFrequency = frequency;
    }

  //Configure frequency of tick tuples for this bolt
    //This delivers a 'tick' tuple on a specific interval,
    //which is used to trigger certain actions
  /*
     @Override
     public Map<String, Object> getComponentConfiguration() {
     Config conf = new Config();
     conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
     return conf;
     }
     */
  //execute is called to process tuples
  /*
     @Override
     public void execute(Tuple tuple, BasicOutputCollector collector) {
     //If it's a tick tuple, emit all words and counts
     if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
     && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
     for(String word : counts.keySet()) {
     Integer count = counts.get(word);
     collector.emit(new Values(word, count));
     logger.info("Emitting a count of " + count + " for word " + word);
     }
     } else {
     //Get the word contents from the tuple
     String word = tuple.getString(0);
     //Have we counted any already?
     Integer count = counts.get(word);
     if (count == null)
     count = 0;
     //Increment the count and store it
     count++;
     counts.put(word, count);
     }
     }*/
    //Declare that this emits a tuple containing two fields; word and count
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String query = tuple.getStringByField("query");
        String tweet = tuple.getStringByField("tweet");
        String label = tuple.getStringByField("label");
        
        count+=1;
        DecimalFormat decimalFormat=new DecimalFormat("00000000");
        String ID = decimalFormat.format(count);
        Put p = new Put(Bytes.toBytes("row"+ID));
        p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Query"), Bytes.toBytes(query));
        p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"), Bytes.toBytes(tweet));
        p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"), Bytes.toBytes(label));
        try {
            table.put(p);            
        }catch(Exception e){
            System.out.println("eccezione!");
        }
        //logger.info("Emitting a label of " + label + " for word " + tweet + " of query " + query);
        System.out.println("Emitting a label of " + label + " for word " + tweet + " of query " + query);
        collector.emit(new Values(query, tweet, label));

    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context){
        
        Configuration conf=HBaseConfiguration.create();
        //conf.set("hbase.rootdir", "file:///hadoop/HBase/HFiles");
        //conf.set("hbase.zookeeper.property.dataDir", "file:///hadoop/zookeeper");
        conf.set("hbase.zookeeper.property.clientPort", "2222");
                
        conf.set("hbase.cluster.distributed", "true");
        conf.set("hbase.rootdir", "hdfs://localhost:54310/hbase");

        try {
            
            table = new HTable(conf, TableName.valueOf("QueryTweetsAnalyzed2"));
            System.out.println("Bernardo_Table is 1");
            Scan s = new Scan();
            s.setCaching(1);
            ResultScanner scanner = table.getScanner(s);
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                //System.out.println("Found Row:" + result);
                String row=Bytes.toString(result.getRow());
                System.out.println("Row "+row);
                lastrow = row;
            }
            scanner.close();
            //}
        }catch(Exception e){
            System.out.println("Bernardo_Table is 2");
            e.printStackTrace();
        }
        if(!lastrow.equals("")){
            StringBuilder sb= new StringBuilder(lastrow);
            sb.delete(0, 3);
            while(sb.charAt(0)=='0'){
                sb.deleteCharAt(0);
            }
            System.out.println("latrow is "+sb.toString());
            lastrow=sb.toString();
            count=Integer.parseInt(lastrow);
        }
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("query", "tweet", "label"));
    }

    /*

     public static void main(String[] args) throws Exception {
     //System.setProperty("storm.jar", "/opt/apache-storm-2.2.0/lib/storm-client-2.2.0.jar");
     TopologyBuilder builder = new TopologyBuilder();

     builder.setSpout("spout", new RandomSentenceSpout(), 1);//5
     builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");//8
     builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));//12

     Config conf = new Config();
     //conf.setMessageTimeoutSecs(120);
     conf.setDebug(false);

     if (args != null && args.length > 0) {
     conf.setNumWorkers(3);

     StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
     } else {
     conf.setMaxTaskParallelism(3);
     final LocalCluster cluster = new LocalCluster();
     try {
     cluster.submitTopology("word-count", conf, builder.createTopology());
     //System.setProperty("storm.jar", "/opt/apache-storm-2.2.0/lib/storm-client-2.2.0.jar");
     //StormSubmitter.submitTopology("word-count2", conf, builder.createTopology());
     Utils.sleep(10000);

     //Thread.sleep(60000);
     // System.out.println("finish!");
     cluster.killTopology("word-count");
     } finally {
     System.out.println("finish!");
     //cluster.killTopology("word-count");
     cluster.shutdown();
     //cluster.close();
                
     //System.exit(0);

     }
        

     }
     System.out.println("finish!");
     }*/

    @Override
    public void cleanup() {
        super.cleanup();
        lastrow="";
        count=0;//To change body of generated methods, choose Tools | Templates.
    }
}
