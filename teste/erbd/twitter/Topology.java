package erbd.twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;





public class Topology {

	static final String TOPOLOGY_NAME = "storm-twitter";

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException {
		Config config = new Config();
		

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
		builder.setBolt("WordSplitterBolt", new WordSplitterBolt(1)).shuffleGrouping("TwitterSampleSpout");
		builder.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt()).shuffleGrouping("WordSplitterBolt");
		builder.setBolt("WordCounterBolt", new WordCounterBolt(10, 5 * 60, 50)).shuffleGrouping("IgnoreWordsBolt");


		
		 if (args != null && args.length > 0) {
		      config.setNumWorkers(1);

		      StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
		    }
		    else {
		      config.setMaxTaskParallelism(1);

		      LocalCluster cluster = new LocalCluster();
		      cluster.submitTopology("word-count", config, builder.createTopology());

		      Thread.sleep(1000);

		      cluster.shutdown();
		    }

	}

}
