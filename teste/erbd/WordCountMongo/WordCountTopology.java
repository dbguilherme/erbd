/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package erbd.WordCountMongo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
//import org.apache.storm.starter.spout.RandomSentenceSpout;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	
  public static class SplitSentence extends BaseBasicBolt {


	public void execute(Tuple tuple, BasicOutputCollector collector) {
	      String word = tuple.getString(0);
	      String vec[]=word.split(" ");
	      for (int i = 0; i < vec.length; i++) {
			collector.emit(new Values(vec[i]));
	      }

	   }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static class WordCount extends BaseBasicBolt {
 
	private static final long serialVersionUID = 1L;
	Map<String, Integer> counts = new HashMap<String, Integer>();

    
    public void execute(Tuple tuple, BasicOutputCollector collector) {
     	Date date = new Date();
  //  	System.out.println(dateFormat.format(date)); //2016/11/16 12:08:43

      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      System.out.println(word +" ---------------------------------_" + count);
      collector.emit(new Values(word, count,date));
    }

    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count","date"));
    }
  }

  public static void main(String[] args) throws Exception {

    
	  
	
	
	String url = "mongodb://127.0.0.1:27017/test";
	String collectionName = "wordcount"; 
	 TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new RandomSentenceSpout(), 5);

    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    
    MongoMapper mapper = new SimpleMongoMapper().withFields("word", "count","date");

    MongoInsertBolt insertBolt = new MongoInsertBolt(url, collectionName, mapper);
        
    
    builder.setBolt("mongo", insertBolt, 1).allGrouping("count");
    
    Config conf = new Config();
    conf.setDebug(false);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(100000);

      cluster.shutdown();
    }
  }
}


//MongoLookupMapper mapper = new SimpleMongoLookupMapper()
//.withFields("word", "count");
//
//QueryFilterCreator filterCreator = new SimpleQueryFilterCreator()
//.withField("word");
//
//MongoLookupBolt lookupBolt = new MongoLookupBolt(url, collectionName, filterCreator, mapper);



//MongoUpdateMapper mapper = new SimpleMongoUpdateMapper()
//.withFields("word", "count");
//
//QueryFilterCreator updateQueryCreator = new QueryFilterCreator() {
//@Override
//public Bson createFilter(ITuple tuple) {
//return Filters.gt("count", 3);
//}
//};
//
//MongoUpdateBolt updateBolt = new MongoUpdateBolt(url, collectionName, updateQueryCreator, mapper);
//
////if a new document should be inserted if there are no matches to the query filter
////updateBolt.withUpsert(true);