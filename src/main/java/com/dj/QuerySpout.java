package com.dj;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class QuerySpout extends BaseRichSpout {
	
	int num;
	SpoutOutputCollector _collector;
	boolean isFirst;
	int qSize;
	
	public QuerySpout(int _num, int _qSize) {
		// TODO Auto-generated constructor stub
		this.num = _num;
		this.isFirst = true;
		this.qSize = _qSize;
	}
	

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		if(isFirst) {
			try {
				FileReader in = new FileReader("query_stream_comp_"+qSize);
				BufferedReader br = new BufferedReader(in);	
//				br.readLine();
				for(int i=0; i<num; i++) {
//					Utils.sleep(100);
					StringBuffer sb = new StringBuffer();
					int edges = Integer.valueOf(br.readLine());
//					sb.append(edges+"\n");
//					int edges = br.read();
					for(int j=0; j< edges; j++) {
						
	//					String edge[] = br.readLine().split(" ");
						sb.append(br.readLine()+"\n");
					}
					_collector.emit("qstream", new Values(sb.toString()));
				}
				isFirst = !isFirst;
				System.out.println("QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ");
				
			} catch (Exception e) {
				// TODO: handle exception
			}
		}else {
			try {
				Thread.sleep(1000000);
				System.exit(0);
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
		
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 declarer.declareStream("qstream", new Fields("qstream"));
	}

}
