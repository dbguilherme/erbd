package erbd.twitter;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WordCounterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2706047697068872387L;
	
	private static final Logger logger = LoggerFactory.getLogger(WordCounterBolt.class);
    
	/** Number of seconds before the top list will be logged to stdout. */
    private final long logIntervalSec;
    
    /** Number of seconds before the top list will be cleared. */
    private final long clearIntervalSec;
    
    /** Number of top words to store in stats. */
    private final int topListSize;

    private Map<String, Long> counter;
    private long lastLogTime;
    private long lastClearTime;

    public WordCounterBolt(long logIntervalSec, long clearIntervalSec, int topListSize) {
        this.logIntervalSec = logIntervalSec;
        this.clearIntervalSec = clearIntervalSec;
        this.topListSize = topListSize;
    }

    
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        counter = new HashMap<String, Long>();
        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("lang", "word"));
    }

    
    public void execute(Tuple input) {
        String word = (String) input.getValueByField("word");
        String lang= (String) input.getValueByField("lang");
       
        Long count = counter.get(word);
        count = count == null ? 1L : count + 1;
        counter.put(word, count);
        System.out.println( word+ " " + count);

    }

    @Override
    public void cleanup() {
    	  SortedMap<Long, String> top = new TreeMap<Long, String>();
          for (Map.Entry<String, Long> entry : counter.entrySet()) {
              long count = entry.getValue();
              String word = entry.getKey();

              top.put(count, word);
              if (top.size() > topListSize) {
                  top.remove(top.firstKey());
              }
          }

          // Output top list:
          for (Map.Entry<Long, String> entry : top.entrySet()) {
             System.out.println( entry.getValue()+ " " + entry.getKey());
          }

    }
}
