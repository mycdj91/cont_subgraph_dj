package com.dj;


import java.io.Serializable;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class WordCountTopology implements Serializable {

  //Entry point for the topology
  public static void main(String[] args) throws Exception {
  //Used to build the topology
    TopologyBuilder builder = new TopologyBuilder();

    int window = 40;
    int batch = 5;
    int numQuery = 100;
    int numSlave = 1; //sbolt
    int querySize = 100;
    int mode = MasterBolt.TOP_K;
    int numWorker = 4;
    
    //#TODO
    String dataset = "wiki"; //"you" or "wiki" or "live"
    
    if(args.length > 0 ) {
    	dataset = args[1];
    	mode = Integer.valueOf(args[2]);   	
    	numSlave = Integer.valueOf(args[3]);
    	numQuery = Integer.valueOf(args[4]);
    	querySize = Integer.valueOf(args[5]);
    	window = Integer.valueOf(args[6]);
    	batch = Integer.valueOf(args[7]);
    }
    
    builder.setSpout("spout", new StreamSpout(window, batch, dataset), 1);
    
    
    builder.setSpout("qspout", new QuerySpout(numQuery, querySize), 1);
    
    
    builder.setBolt("split", new MasterBolt(window, batch, numSlave, mode), 1)
    .shuffleGrouping("spout", "dstream")
    .shuffleGrouping("qspout", "qstream");
    
    if(mode == MasterBolt.TURBO) {
    	builder.setBolt("qproc_0", new QProcessBolt(batch), 1)
    	.shuffleGrouping("split", "query_1_TB")
    	.shuffleGrouping("split", "data_1_TB");
    	
    	 builder.setBolt("merge", new MergeBolt(batch, mode), 1)
 	    .shuffleGrouping("qproc_0", "sub_core")
 	    .shuffleGrouping("qproc_0", "subgraph") 	    
 	    .shuffleGrouping("split", "new_query");
    }
    else if(mode == MasterBolt.TOP_K) {
    	builder.setBolt("qproc_0", new QProcessBolt(batch), 1)
    	.shuffleGrouping("split", "query_1_TK")
    	.shuffleGrouping("split", "data_1_TK");
    	
    	 builder.setBolt("merge", new MergeBolt(batch, mode), 1)
 	    .shuffleGrouping("qproc_0", "sub_core")
 	    .shuffleGrouping("qproc_0", "subgraph") 	    
 	    .shuffleGrouping("split", "new_query");
    }
    
    else {
	    for(int i=0; i<numSlave; i++) {
	    	
	    	if(mode == MasterBolt.PROPOSED|| mode == MasterBolt.PROPOSED_EXT) {
			    builder.setBolt("qproc_"+i, new QProcessBolt(batch), 1)
			    .shuffleGrouping("split", "query_"+i)
			    .shuffleGrouping("split", "data_"+i);
	    	}
	    	else if(mode == MasterBolt.NPV) {
	    	    builder.setBolt("qproc_"+i, new QProcessBolt(batch), 1)
	    	    .shuffleGrouping("split", "query_"+i+"_NNT")
	    	    .shuffleGrouping("split", "data_"+i+"_NNT");
		    
	    	}
	    	else {
	    	    builder.setBolt("qproc_"+i, new QProcessBolt(batch), 1)
	    	    .shuffleGrouping("split", "query_"+i+"_DAG")
	    	    .shuffleGrouping("split", "data_"+i+"_DAG");
	    	}
	    }
	    
	    switch(numSlave) {
	    	case 1 :  
    		   builder.setBolt("merge", new MergeBolt(batch, mode), 1)
    		    .shuffleGrouping("qproc_0", "sub_core")
    		    .shuffleGrouping("qproc_0", "subgraph")
    		    .shuffleGrouping("split", "new_query");
	    		break;
	    	case 2 : 
    		   builder.setBolt("merge", new MergeBolt(batch, mode), 1)
    		    .shuffleGrouping("qproc_0", "sub_core")
    		    .shuffleGrouping("qproc_0", "subgraph")
    		    .shuffleGrouping("qproc_1", "sub_core")
    		    .shuffleGrouping("qproc_1", "subgraph")
    		    .shuffleGrouping("split", "new_query");
    		   break;
	    	case 4 :
    		  builder.setBolt("merge", new MergeBolt(batch, mode), 1)
    		    .shuffleGrouping("qproc_0", "sub_core")
    		    .shuffleGrouping("qproc_0", "subgraph")
    		    .shuffleGrouping("qproc_1", "sub_core")
    		    .shuffleGrouping("qproc_1", "subgraph")
    		    .shuffleGrouping("qproc_2", "sub_core")
    		    .shuffleGrouping("qproc_2", "subgraph")
    		    .shuffleGrouping("qproc_3", "sub_core")
    		    .shuffleGrouping("qproc_3", "subgraph")
    		    .shuffleGrouping("split", "new_query");	    		
	    		break;
	    	case 8 : 
    		  builder.setBolt("merge", new MergeBolt(batch, mode), 1)
    		    .shuffleGrouping("qproc_0", "sub_core")
    		    .shuffleGrouping("qproc_0", "subgraph")
    		    .shuffleGrouping("qproc_1", "sub_core")
    		    .shuffleGrouping("qproc_1", "subgraph")
    		    .shuffleGrouping("qproc_2", "sub_core")
    		    .shuffleGrouping("qproc_2", "subgraph")
    		    .shuffleGrouping("qproc_3", "sub_core")
    		    .shuffleGrouping("qproc_3", "subgraph")
    		    .shuffleGrouping("qproc_4", "sub_core")
    		    .shuffleGrouping("qproc_4", "subgraph")
    		    .shuffleGrouping("qproc_5", "sub_core")
    		    .shuffleGrouping("qproc_5", "subgraph")    
    		    .shuffleGrouping("qproc_6", "sub_core")
    		    .shuffleGrouping("qproc_6", "subgraph")
    		    .shuffleGrouping("qproc_7", "sub_core")
    		    .shuffleGrouping("qproc_7", "subgraph")
    		    .shuffleGrouping("split", "new_query");	    		
	    		break;
	    	case 16 : 
    		  builder.setBolt("merge", new MergeBolt(batch, mode), 1)
    		    .shuffleGrouping("qproc_0", "sub_core")
    		    .shuffleGrouping("qproc_0", "subgraph")
    		    .shuffleGrouping("qproc_1", "sub_core")
    		    .shuffleGrouping("qproc_1", "subgraph")
    		    .shuffleGrouping("qproc_2", "sub_core")
    		    .shuffleGrouping("qproc_2", "subgraph")
    		    .shuffleGrouping("qproc_3", "sub_core")
    		    .shuffleGrouping("qproc_3", "subgraph")
    		    .shuffleGrouping("qproc_4", "sub_core")
    		    .shuffleGrouping("qproc_4", "subgraph")
    		    .shuffleGrouping("qproc_5", "sub_core")
    		    .shuffleGrouping("qproc_5", "subgraph")    
    		    .shuffleGrouping("qproc_6", "sub_core")
    		    .shuffleGrouping("qproc_6", "subgraph")
    		    .shuffleGrouping("qproc_7", "sub_core")
    		    .shuffleGrouping("qproc_7", "subgraph")
    		    .shuffleGrouping("qproc_8", "sub_core")
    		    .shuffleGrouping("qproc_8", "subgraph")
    		    .shuffleGrouping("qproc_9", "sub_core")
    		    .shuffleGrouping("qproc_9", "subgraph")
    		    .shuffleGrouping("qproc_10", "sub_core")
    		    .shuffleGrouping("qproc_10", "subgraph")
    		    .shuffleGrouping("qproc_11", "sub_core")
    		    .shuffleGrouping("qproc_11", "subgraph")
    		    .shuffleGrouping("qproc_12", "sub_core")
    		    .shuffleGrouping("qproc_12", "subgraph")
    		    .shuffleGrouping("qproc_13", "sub_core")
    		    .shuffleGrouping("qproc_13", "subgraph")
    		    .shuffleGrouping("qproc_14", "sub_core")
    		    .shuffleGrouping("qproc_14", "subgraph")
    		    .shuffleGrouping("qproc_15", "sub_core")
    		    .shuffleGrouping("qproc_15", "subgraph")
    		    .shuffleGrouping("split", "new_query");	    		
		    		break;
	    		
	    		
	    }
	  
	    
    }
    
   
    
//    		
//    .shuffleGrouping("split", "")
    
//    .shuffleGrouping("split", "new_query_qvs");

//    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    Config conf = new Config();
    conf.put(Config.TOPOLOGY_DEBUG, false);
    conf.setDebug(false);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(8);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    //Otherwise, we are running locally
    else {
      conf.setMaxTaskParallelism(numWorker);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());
      //sleep
      
      
//      Thread.sleep(10000);
//      cluster.shutdown();
    }
  }
}