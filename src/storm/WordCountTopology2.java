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
import com.aliasi.classify.DynamicLMClassifier;
import com.aliasi.lm.NGramProcessLM;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.storm.generated.Nimbus.Client;

public class WordCountTopology2 {

    
    //Entry point for the topology
    public static void main(String[] args) throws Exception {
        //Used to build the topology
        TopologyBuilder builder = new TopologyBuilder();
        //Add the spout, with a name of 'spout'
        //and parallelism hint of 5 executors
        builder.setSpout("spout", new RandomSentenceSpout2(), 2);
        //Add the SplitSentence bolt, with a name of 'split'
        //and parallelism hint of 8 executors
        //shufflegrouping subscribes to the spout, and equally distributes
        //tuples (sentences) across instances of the SplitSentence bolt
        //builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
        builder.setBolt("split", new SplitSentence2(), 1).shuffleGrouping("spout");
        //Add the counter, with a name of 'count'
        //and parallelism hint of 12 executors
        //fieldsgrouping subscribes to the split bolt, and
        //ensures that the same word is sent to the same instance (group by field 'word')
        builder.setBolt("count", new WordCount2(), 3).fieldsGrouping("split", new Fields("query"));

        //new configuration
        Config conf = new Config();
        //Map conf = Utils.readStormConfig();
        //Set to false to disable debug information when
        // running in production on a cluster
        conf.setDebug(true);
        //NLP.init();
        //If there are arguments, we are running on a cluster
        if (args != null && args.length > 0) {
            //parallelism hint to set the number of workers
            conf.setNumWorkers(1);
            //submit the topology
            conf.put("nimbus.host","localhost");
            conf.put(Config.NIMBUS_THRIFT_PORT,6627);  
            conf.put(Config.STORM_ZOOKEEPER_PORT,2222);
            System.setProperty("storm.jar", "/home/bernardo/NetBeansProjects/Storm/dist/AllStormJar.jar");  
            StormSubmitter.submitTopology("TwitterTopology", conf, builder.createTopology());
            //Map config = Utils.readStormConfig();
            /*Nimbus.Iface client= NimbusClient.getConfiguredClient(conf).getClient();
            KillOptions killopt=new KillOptions();
            killopt.set_wait_secs(30);
            client.killTopologyWithOpts("TwitterTopology", killopt);*/
            
        } //Otherwise, we are running locally
        else {
            //Cap the maximum number of executors that can be spawned
            //for a component to 3
            //conf.setMaxTaskParallelism(3);
            //LocalCluster is used to run locally
            //submit the topology
            try {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("TwitterTopology", conf, builder.createTopology());
                //sleep
                Thread.sleep(80000);
                //Thread.sleep(50000);
                //shut down the cluster
                cluster.deactivate("TwitterTopology");
                Thread.sleep(10000);
                cluster.killTopology("TwitterTopology");
                //Utils.sleep(20000);
                cluster.shutdown();
            } catch (Exception e) {
                //cluster.shutdown();
                System.exit(0);

            } finally {
                //System.out.println("becat" + cluster.getClusterInfo());
                //cluster.shutdown();
                System.exit(0);
                

            }
            //System.exit(0);
        }
    }

    
}
