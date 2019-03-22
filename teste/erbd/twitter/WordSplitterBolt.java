package erbd.twitter;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.Status;


public class WordSplitterBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 5151173513759399636L;

	private final int minWordLength;

    private OutputCollector collector;

    public WordSplitterBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet");
        String lang = tweet.getUser().getLang();
        String text = tweet.getText().replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
        String[] words = text.split(" ");
       
        for (String word : words) {
            if (word.length() >= minWordLength) {
            	//System.out.println("text " + text);
                collector.emit(new Values(lang, word));
            }
        }
    }

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		 arg0.declare(new Fields("lang", "word"));
		
	}

   


}
