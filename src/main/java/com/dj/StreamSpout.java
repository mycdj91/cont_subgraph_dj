package com.dj;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import com.lmax.disruptor.SleepingWaitStrategy;


public class StreamSpout extends BaseRichSpout implements Serializable {
	  //Collector used to emit output
	  SpoutOutputCollector _collector;
	  transient FileReader in;
	  transient BufferedReader br;	  
	  
	  int window;	//	5
	  int batch;	//	3 	
	  int now;
	  StringBuffer sb[];
	  boolean isFirst;
	  String dataset;

	  public StreamSpout(int _window, int _batch, String _dataset) {
		  this.window = _window;
		  this.batch = _batch;
		  this.now = batch-1;
		  this.isFirst = true;
		  this.dataset = _dataset;
		 
	  } 
	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	  //Set the instance collector to the one passed in
	    _collector = collector;
	    //For randomness
	    sb = new StringBuffer[batch];
	    try {
			in = new FileReader("edge_stream_"+dataset+"_ext");
			br = new BufferedReader(in);
			for(int i=0; i<batch; i++)
			{
				sb[i] = new StringBuffer();
			}
//			System.out.println(br.readLine());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }

	  //Emit data to the stream	  
	  public void nextTuple(){
	  //Sleep for a bit
		if(isFirst) {
			Utils.sleep(10);
		}
	    String temp = null;
	    try {	    	
	    	String tuple = read();
	    	
			_collector.emit("dstream",new Values(tuple));
//			System.out.println(tuple);
			
	    } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
	  }

	  //Ack is not implemented since this is a basic example
	  @Override
	  public void ack(Object id) {
	  }

	  //Fail is not implemented since this is a basic example
	  @Override
	  public void fail(Object id) {
	  }

	  //Declare the output fields. In this case, an sentence	  
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declareStream("dstream", new Fields("dstream"));
	   
	  }
	  
	  public String read() throws Exception{
//			sb.delete(0, 5);
			if(isFirst) {
				int count =0;
				int temp = 0;
				for(int i=0; i<(window*batch); i++) {
//					System.out.println(br);
//					System.out.println(br.readLine());
					sb[temp].append(br.readLine()+"\n");
					count++;
					if(count == window) {
						count =0;
						temp++;
					}				
				}
				isFirst = !isFirst;			
			}
			else {
				sb[now].setLength(0);
				for(int i=0; i<window; i++) {
					sb[now].append(br.readLine()+"\n");
				}
			}
			
			
			now = (now+1)%batch;
			
			int temp = now;
			StringBuffer lsb = new StringBuffer();
			for(int i =0; i<batch; i++) {
				lsb.append(sb[temp]);
				temp = (temp+1)%batch;
			}
			String rtv = lsb.toString();
			if(rtv.contains("null")) {
				Thread.sleep(100000000);
				throw new Exception("File END");
			}
//			System.out.println(lsb);
			return rtv;
			
		}
	}
