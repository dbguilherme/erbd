/**
 * Taken from the storm-starter project on GitHub
 * https://github.com/nathanmarz/storm-starter/ 
 */
package erbd.twitter;
//import backtype.storm.Config;
//import backtype.storm.spout.SpoutOutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.base.BaseRichSpout;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Values;
//import backtype.storm.utils.Utils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Reads Twitter's sample feed using the twitter4j library.
 * 
 * @author davidk
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterSampleSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;
	private TwitterStream twitterStream;

	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;
		//  TwitterStream stream = TwitterStreamBuilderUtil.getStream();
		StatusListener listener = new StatusListener() {
			
			public void onStatus(Status status) {
				queue.offer(status);
			}

			
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			
			public void onTrackLimitationNotice(int i) {
			}

			
			public void onScrubGeo(long l, long l1) {
			}

			
			public void onStallWarning(StallWarning stallWarning) {
			}

			
			public void onException(Exception e) {
			}
		};

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("MSARdU0w4uNd6vc1l6GAelv6p")
		  .setOAuthConsumerSecret("OGqxvOcmil0KgtX3XRwaev8Dz4SP0OcFW5dMMoAtOrncRuWvqr")
		  .setOAuthAccessToken("511021726-Dpw5bQNj1gwd8AdDUan9xZLzLVHqheMEkY5RTOXI")
		  .setOAuthAccessTokenSecret("Jo0Diyawt0wlnwpp85gIvm7O2AwPP8K14pjuEkVwA4YQq");
		
		TwitterStreamFactory factory = new TwitterStreamFactory(cb.build());
		twitterStream = factory.getInstance();
		//twitterStream.addListener(listener);
		//twitterStream.sample("en");
		
		   FilterQuery qry = new FilterQuery();
           String[] keywords = { "brasil","trump","politica" };

           qry.track(keywords);

           twitterStream.addListener(listener);
           twitterStream.filter(qry);
	}

	
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(1);
		} else {
			collector.emit(new Values(ret));
			//System.out.println((new Values(ret)));
		}
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

	

}
