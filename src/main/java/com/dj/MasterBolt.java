package com.dj;

import static java.util.stream.Collectors.toCollection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.core.appender.rewrite.MapRewritePolicy.Mode;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//There are a variety of bolt types. In this case, use BaseBasicBolt
public class MasterBolt extends BaseBasicBolt {

	int window;
	int batch;
	
	HashMap<Integer, Vertex> vs;
//	HashMap<Integer, HashMap<Integer, Vertex>> queries;  //query vertices
	
	ConcurrentHashMap<Integer, ArrayList<CoreTable>> q_cts; // 
	
	HashMap<Integer, ArrayList<CoreTable>> q_cts2;
	ArrayList<ArrayList<CoreTable>> qSize; 
	int[] numStream;
	
	int numSlave;
	
	ArrayList<CoreTable> cts; 
	int qIds;
	boolean isFirst;
	
	double alpha, beta;
	
	long constTime;
	int rr;
	
	int numLab;
	
	
	//for NNT
	HashMap<Integer, ArrayList<NNT>> nnts;  //lab -> NNT list -> for data sending
	ArrayList<Integer> boltIds;
	
	//for DAG
	ArrayList<DAG> dags;
	
	//for TF
	int gid;
	
	int MODE = -1;
	
	static final int PROPOSED 			= 1;
	static final int NPV 				= 2;
	static final int DAG 				= 3;
	static final int TURBO 				= 4;
	
	static final int PROPOSED_EXT 		= 5;
	static final int TOP_K 				= 6;
	
	int size;
	
	public MasterBolt(int _window, int _batch, int _numSlave, int _mode) {
		// TODO Auto-generated constructor stub
		
		numLab = 5;
		
		window = _window;
		batch = _batch;
		vs = new HashMap<Integer, Vertex>();
//		queries = new HashMap<Integer, HashMap<Integer, Vertex>>();
		cts = new ArrayList<CoreTable>();
		q_cts = new ConcurrentHashMap<Integer, ArrayList<CoreTable>>();
		q_cts2 = new HashMap<Integer, ArrayList<CoreTable>>();
		
		for(int i=0; i<numLab; i++) {
			q_cts2.put(i,new ArrayList<CoreTable>());
		}
		
		
		qSize = new ArrayList<ArrayList<CoreTable>>();
		numStream = new int[_numSlave];
		numSlave = _numSlave;
		for(int i=0; i<numSlave; i++) {
			qSize.add(new ArrayList<CoreTable>());
//			numStream.add(0);
		}
		
		qIds = 0;		
		isFirst = true;
		
		alpha = 0.7;
		beta = 1- alpha;
		rr =0; //round robin
		constTime = 0L;
		
		//for NNT
		
		nnts = new HashMap<Integer, ArrayList<NNT>>();
		boltIds = new ArrayList<Integer>();
		for(int i=0; i<numLab; i++) {
			nnts.put(i, new ArrayList<NNT>());
		}
		
		//for DAG
		dags = new ArrayList<DAG>();
//		for(int i=0; i<numLab; i++) {
//			dags.put(i, new ArrayList<DAG>());
//		}
		MODE = _mode;
		
		gid = 0;
		size =0; 
	}
	
  //Execute is called to process tuples
  public void execute(Tuple tuple, BasicOutputCollector collector) {    

	String stream = tuple.getString(0);
	String sp[] = stream.split("\n");
	
//	System.out.println("data " + stream);
	//data
    if(tuple.getSourceStreamId().equals("dstream")) {

    	if(isFirst) {
    		for(String s : sp) {
        		sendData(collector, s);
        	}
    		isFirst = !isFirst;
    	}
    	for(int i=sp.length-1; i > sp.length-(1+window); i--) {
    		sendData(collector, sp[i]);
//    		System.out.println("data " + sp[i]);
    	}
    	
    	
    //query
    }else {
//    	System.out.println(stream);
//    	long st = System.nanoTime();
    	HashMap<Integer, Vertex> qvs = new HashMap<Integer, Vertex>();    	
    	for(int i=0; i<sp.length; i++) {
    		
    		String es[] = sp[i].split(" ");
    		int esrc = Integer.valueOf(es[0]);
    		int edst = Integer.valueOf(es[1]);
    		int esrcL =Integer.valueOf(es[2]); 
    		int edstL = Integer.valueOf(es[3]);
    		double weight = 0.0;
    		int op = -1;
    		
    		if(MODE >= 5) {
    			weight = Double.valueOf(es[4]);
    			op = Integer.valueOf(es[5]);
    		}
    		
    		if(qvs.get(esrc) == null) {
    			Vertex temp = new Vertex(esrc, esrcL);
    			qvs.put(esrc, temp);
    		}
    		if(qvs.get(edst) == null) {
    			Vertex temp = new Vertex(edst, edstL);
    			qvs.put(edst, temp);
    		}
    		
    		Vertex sv = qvs.get(esrc);
    		Vertex dv = qvs.get(edst);    		
    		
    		Edge se = new Edge(esrc, edst, esrcL, edstL, 0, weight, op);
    		sv.addEdge(se);
    		
    		if(MODE != TURBO) {
	    		Edge de = new Edge(edst, esrc, edstL, esrcL, 0, weight, op);
	    		dv.addEdge(de);
    		}	
    	
    	}
    	
    	switch (MODE) {
			case PROPOSED:
			case PROPOSED_EXT :
				
				q_cts.put(qIds, new ArrayList<CoreTable>());
		    	reuseCheck(qvs, collector, qIds);
//		    	System.out.println("reuse");
		    	if(qvs.size() > 0) {
		    		int s = q_cts.get(qIds).size();
		    		if( s > 0)
		    			makeCoreTable(qIds, qvs, q_cts.get(qIds).get(s-1));
		    		else
		    			makeCoreTable(qIds, qvs, null);
		    	}
		    	
		    	//send core tables to slave bolts
		    	sendQuery(collector, qIds);
		    	
		    	//send queries to merging bolt
		    	collector.emit("new_query", new Values(q_cts));
		    	int ssize =0 ;
		    	for(ArrayList<CoreTable> qs :  qSize) {
		    		for(CoreTable c : qs) {
		    			ssize += c.vls.size()*batch;
		    		}
		    	}
		    	System.out.println(qIds +" , size = " + ssize);
		    	
//		    	System.out.println("QIDS = "+qIds);
				break;
	
			case NPV :
				ArrayList<NNT> qNNT = makeNNT(qIds, qvs);
		    	//send nnts to the slave bolt and the merging bolt  
		    	sendQueryNNT(collector, qNNT, qvs);
//		    	Set<Integer> keys = nnts.keySet();
		    	int nnt_size = 0; 
		    	for(int i=0; i<5; i++) {
		    		for(NNT n : nnts.get(i)) {
		    			nnt_size += 5;
		    		}
		    	}
		    	System.out.println(qIds +" , nnt_size = " + nnt_size);
				break;
			case DAG :
				ArrayList<DAG> temp = makeDAG(qIds, qvs);
				
				
				sendQueryDAG(collector, temp, qvs);
				
				int dag_size = 0;
				for(DAG d : dags) {
					dag_size += (d.vs.size()*10) + (d.source.size() * 3) + 3;
					for(int key : d.vs.keySet()) {
						dag_size += (d.vs.get(key).degree * 5);
					}
					
				}
				System.out.println(qIds + ", dag_size = " + dag_size);
				break;
			case TURBO :
				
				Set<Integer> copy = new HashSet<>(qvs.keySet());
				size += qvs.size();
				
				for(int key : qvs.keySet()) {
					Vertex vt = qvs.get(key);
					if(vt.degree == 0)
					{
						vt.type = 1; // leaf node
					}
					size += vt.edges.size();
					for(Edge eg : vt.edges) {
						copy.remove(eg.dst);
					}
				}
				
				for(int key : copy) {
					qvs.get(key).type = 2; // source node
				}
				
				collector.emit("query_1_TB", new Values(qvs));
				
 			    Collection<Vertex> vv = qvs.values();
 			    CopyOnWriteArrayList<Vertex> list = vv.stream().map(l -> l.deepCopy()).collect(toCollection(CopyOnWriteArrayList::new));	  
			    collector.emit("new_query", new Values(new SubgraphNNT(list, qIds)));
				
				System.out.println(qIds + ", qvs_size = " + size);
				break;
			case TOP_K :
				
				collector.emit("query_1_TK", new Values(qvs));
				break;
		}
    
    	
			/*
			 * constTime += System.nanoTime() - st; System.out.println(qIds +
			 * " Construction Time (ms) : " + constTime );
			 */
    	qIds++;
    	
    }
   
  }
  
  private void sendQueryDAG(BasicOutputCollector collector, ArrayList<DAG> temp, HashMap<Integer, Vertex> qvs) {
	// TODO Auto-generated method stub
	  
	  for(DAG d : temp) {
		  int n = getNodeRR();
		  d.boltId = n;
//		  for(Vertex v : d.source)
//		  {
//			  dags.get(v.lab).add(d);
//		  }
//		  System.out.println("send query "+ d.source.toString());
//		  System.out.println("send query id"+ d.boltId);
		  collector.emit("query_"+n+"_DAG", new Values(d));
	  }
	  dags.addAll(temp);
	  Collection<Vertex> vv = qvs.values();
	  CopyOnWriteArrayList<Vertex> list = vv.stream().map(l -> l.deepCopy()).collect(toCollection(CopyOnWriteArrayList::new));	  
	  collector.emit("new_query", new Values(new SubgraphNNT(list, temp.get(0).qid)));
}

private ArrayList<DAG> makeDAG(int qIds, HashMap<Integer, Vertex> qvs) {
	// TODO Auto-generated method stub
	Vertex sink = null;
	ArrayList<DAG> temp_dags = new ArrayList<DAG>();
	
	HashMap<Integer, Vertex> cqvs = (HashMap<Integer, Vertex>)qvs.clone();
	
	Set<Integer> keys = cqvs.keySet();
	int highest = 0;
	for(int k : keys) {
		if(cqvs.get(k).degree > highest) {
			sink = cqvs.get(k);
			highest = sink.degree;
		}
	}	
	ArrayList<Integer> temp = new ArrayList<Integer>();
//	System.out.println("sink = " + sink.toString());
//	System.out.println("sink edges = " + sink.edges.toString());
	for(int i=0; i<highest; i++) {
		if(sink.edges.size() != i ) {
			Edge e = sink.edges.get(i);
			Vertex v = new Vertex(sink.id, sink.lab);
	//		v.addEdge(e);		
			temp_dags.add(new DAG(v, e, qIds));
			cqvs.get(e.dst).remove(sink.id);
			temp.add(e.dst);
		}
	}
//	System.out.println("SINK = " + sink.toString() + ", " + temp);
	int level = 0;
	while(temp.size() != 0) {
		level++;
		ArrayList<Integer> ttemp = new ArrayList<Integer>();
		for(int tkey : temp) {
			for(DAG dag : temp_dags) {
//				System.out.println("is COntain +" + tkey);
				if(dag.isContain(tkey) && cqvs.get(tkey).edges.size()!= 0) {
					
					dag.addEdges(cqvs.get(tkey).edges, level);
//					System.out.println("EDGES" + tkey + ",, " + cqvs.get(tkey).edges.toString());
					for(Edge e : cqvs.get(tkey).edges) {
						if(!dag.isContain(e.dst)) {						
							cqvs.get(e.dst).remove(e.src);
							ttemp.add(e.dst);
						}
					}
				}
			}
		}
		temp.clear();
		temp.addAll(ttemp);
	}
	System.out.println(temp_dags);
	return temp_dags;
}

private void sendQueryNNT(BasicOutputCollector collector, ArrayList<NNT> qNNT,
		HashMap<Integer, Vertex> qvs) {
	// TODO Auto-generated method stub
	  
	  int n = getNodeRR();
	  
	  boltIds.add(n);
	  Collection<Vertex> vv = qvs.values();
	  CopyOnWriteArrayList<Vertex> list = vv.stream().map(l -> l.deepCopy()).collect(toCollection(CopyOnWriteArrayList::new));
//	  ArrayList<Vertex> list = vv.stream()
//			  .map( l -> l)			  
//			  .collect(toCollection(ArrayList::new));
	  
	  collector.emit("query_"+n+"_NNT", new Values(qNNT));
	  collector.emit("new_query", new Values(new SubgraphNNT(list, qNNT.get(0).qId)));
}

private ArrayList<NNT> makeNNT(int qIds, HashMap<Integer, Vertex> qvs) {
	// TODO Auto-generated method stub
	Set<Integer> keys = qvs.keySet();
	
//	HashMap<Integer, Vertex> qvClone = (HashMap<Integer, Vertex>)qvs.clone();
	
	ArrayList<NNT> tempNNTs = new ArrayList<NNT>();
	for(int k : keys) {
		Vertex temp = qvs.get(k);
		int[] _npv = new int[numLab];
		for(Edge e : temp.edges) {
			_npv[e.dstLab]++;
//			qvClone.get(e.dst).remove(temp.id);
		}
		
		tempNNTs.add(new NNT(qIds, temp.lab, _npv));
		
//		nnts.get(temp.lab).add(new NNT(qIds, temp.lab, _npv));
	}
	
	for(NNT n : tempNNTs) {
		nnts.get(n.lab).add(n);
	}
	
	return tempNNTs;

  }

private void sendQuery(BasicOutputCollector collector, int qid) {
	// TODO Auto-generated method stub
	  
	  for(CoreTable c  : q_cts.get(qid)) {
		  //new table
		  if(c.quries.get(0) == qid) {
			 int nid = getNode();
//			 int nid = getNodeRR();
			 collector.emit("query_"+nid, new Values(c));
			 qSize.get(nid).add(c);
			 
			 c.sid = nid;
		  }else {
			  int nid = getNode(c);			  
			  collector.emit("query_"+nid, new Values(c));
			  
			  c.sid = nid;
		  }
	  }
}

  private int getNodeRR() {
	// TODO Auto-generated method stub
	int rtv = rr % numSlave;
	rr = (rr+1)%numSlave;
	return rtv;
}

private int getNode(CoreTable c) {  
//	  int rtv = -1;
	  for(int i=0; i<numSlave; i++) {
		  for(int j = 0; j < qSize.get(i).size(); j++) {
			  if(qSize.get(i).get(j).tid == c.tid) {
				  qSize.get(i).set(j, c);
				  return i;
			  }
		  }
	  }
	  return 0;
  }
  //proposedMethod
private int getNode() {
	// TODO Auto-generated method stub
	int rtv = -1;
	double minLoad = Double.MAX_VALUE;
	int maxTable = 0;
	int maxStream = 0;
	
	
	for(int i=0; i<numSlave; i++) {
		int tempTable = 0;
		for(CoreTable c : qSize.get(i)) {
			tempTable += c.vls.size();
		}
		if(maxTable < tempTable)
			maxTable = tempTable;
		
		if(maxStream < numStream[i])
			maxStream = numStream[i];
	}
	
	for(int i=0; i<numSlave; i++) {
		int tempTable = 0;
		for(CoreTable c : qSize.get(i)) {
			tempTable += c.vls.size();
		}
		double load = 0;
		if(maxTable !=0 )
			load += (tempTable/maxTable)*alpha;
		if(maxStream != 0)
			load += (numStream[i]/maxStream)*beta;
		if(load < minLoad)
		{
			minLoad = load;
			rtv = i;
		}
	}
	
	return rtv;
}

private void sendData(BasicOutputCollector collector, String data) {
	// TODO Auto-generated method stub
	String ds[] = data.split(" ");
	int src = Integer.valueOf(ds[0]);
	int dst = Integer.valueOf(ds[1]);
	int srcLab = Integer.valueOf(ds[2]);
	int dstLab = Integer.valueOf(ds[3]);
	int time = Integer.valueOf(ds[4]);
	double w = 0.0;
	int op = 0;
	
	if(MODE == PROPOSED_EXT || MODE == TOP_K) {
		w = Double.valueOf(ds[5]);
		op = Integer.valueOf(ds[6]);
	}
	
	switch(MODE){
		case PROPOSED : 
		case PROPOSED_EXT : 

			long sn = System.nanoTime();
			Set<Integer> temp = new HashSet<Integer>();
			for(CoreTable c : q_cts2.get(srcLab)) {
				temp.add(c.sid);
			}
			
			for(CoreTable c : q_cts2.get(dstLab)) {
				temp.add(c.sid);
			}
			
			for(int i : temp) {
				numStream[i]++;
				collector.emit("data_"+i, new Values(new Edge(src, dst, srcLab, dstLab, time, w, op)));
				
			}
			
			long en = System.nanoTime();
//			System.out.println("DATA time1 = " + (en-sn));
		
//			
//			long sn3 = System.nanoTime();
//			
//			long en3 = System.nanoTime();
//			System.out.println("DATA time2 = " + (en3-sn3));
		
			break;
		case NPV : 
			HashSet<Integer> qids = new HashSet<Integer>();
			for(NNT n : nnts.get(srcLab))
			{
				qids.add(n.qId);
			}
			for(NNT n : nnts.get(dstLab))
			{
				qids.add(n.qId);
			}
			for(int q : qids) {
				collector.emit("data_"+boltIds.get(q)+"_NNT", new Values(new Edge(src, dst, srcLab, dstLab, time)));
			}
			break;
		case DAG : 
			HashSet<Integer> qids_dag = new HashSet<Integer>();
			
			for(DAG d : dags) {
				if(d.isContainLab(srcLab) || d.isContainLab(dstLab) )
					qids_dag.add(d.boltId);
			}
			
			for(int q : qids_dag) {
//				System.out.println(src+"___" + dst);
//				System.out.println(q);
				collector.emit("data_"+q+"_DAG", new Values(new Edge(src, dst, srcLab, dstLab, time)));
			}
			
			break;
		case TURBO : 
			long sn2 = System.nanoTime();
			gid++;
			collector.emit("data_1_TB", new Values(new Edge(src,dst,srcLab, dstLab, time)));			
			long en2 = System.nanoTime();
//			System.out.println("DATA time = " + (en2-sn2));
			break;
			
		case TOP_K : 
//			long sn2 = System.nanoTime();
			collector.emit("data_1_TK", new Values(new Edge(src,dst,srcLab, dstLab, time, w, op)));			
//			long en2 = System.nanoTime();
			break;
		
	}
	/*
	for(CoreTable c : cts ) {
		if((c.coreLab == srcLab || c.coreLab == dstLab) && 
			(c.vls.contains(srcLab) || c.vls.contains(dstLab))) {
			collector.emit("data_0", new Values(new Edge(src, dst, srcLab, dstLab, time)));
		}
	}
	*/ 
}
	
private boolean containV(HashMap<Integer, Vertex> h, int lab) {
	  Set<Integer> keys = h.keySet();
	  for(int key : keys) {
		  if(h.get(key).lab == lab)
			  return true;
	  }
	
	  return false;
  }

  private void reuseCheck(HashMap<Integer, Vertex> qvs, BasicOutputCollector collector, int qid) {
	// TODO Auto-generated method stub
//	ArrayList<Integer> cs = new ArrayList<Integer>();
	while(qvs.size() > 0 ) {
		boolean wFlag = true;
		for(int i =0 ; i<cts.size(); i++) {
			CoreTable c = cts.get(i);
			int size = qvs.size();
			int flag = c.containCheck(qvs, qid);
			
			
//			System.out.println("f = "+flag+", "+qvs.size());
			//reuse
			if(flag != -1) {
				wFlag = false;
				q_cts.get(qid).add(c);
				q_cts2.get(c.coreLab).add(c);
				//split
				if(flag == 2) {
					ArrayList<Integer> splits = new ArrayList<Integer>();					
					for(int vl : c.vls) {
						if(!containV(qvs, vl)) {
							splits.add(vl);
						}
					}
					CoreTable nc = new CoreTable(cts.size(), c.coreId, c.coreLab, (ArrayList<Integer>)c.quries.clone(), splits, window);
					
					nc.w = (ArrayList<Double>)c.w.clone();
					nc.op = (ArrayList<Integer>) c.op.clone();
					
					c.remove(splits);					
					c.addQuery(qid);
//					c.quries.add(qid);
					
					q_cts.get(qid).add(nc);
					q_cts2.get(nc.coreLab).add(nc);
					collector.emit("new_query", new Values(q_cts));
					cts.add(nc);
					qvs.clear();
				}else {
					
					if(size == qvs.size())
						return;
				}
					
//					  if(!cs.contains(i)) cs.add(i);
					 
			}
		}
		if(wFlag)
			break;
  	}
		/*
		 * if(cs.size() > 0) { ArrayList<CoreTable> ecs = new ArrayList<CoreTable>();
		 * for(int cid : cs) { ecs.add(cts.get(cid));
		 * 
		 * cts.get(cid) } collector.emit("query", new Values(ecs)); }
		 */
		 
	
  }

  private void makeCoreTable(int qid, HashMap<Integer, Vertex> qvs, CoreTable prevCore) {
	// TODO Auto-generated method stub	
	Set<Integer> keys = qvs.keySet();
	int maxD = -1;
	int coreId = -1;
	//find core
	for(int k : keys) {
		if(qvs.get(k).degree > maxD)
		{
			maxD = qvs.get(k).degree ;
			coreId = k;
		}
	}
	
	Vertex core = qvs.get(coreId);
	CoreTable ct = new CoreTable(cts.size(), coreId, core.lab, qid);
	
	cts.add(ct);
	q_cts.get(qid).add(ct);
	q_cts2.get(core.lab).add(ct);
	
	for(int i=0; i<core.degree; i++) {
		Edge e = core.edges.get(i);
		ct.vls.add(e.dstLab);
		ct.w.add(e.weight);
		ct.op.add(e.op);
		
		if(qvs.get(e.dst).degree == 1) {
			if(prevCore!= null && prevCore.joins.get(qid).contains(e.dstLab))
				ct.joins.get(qid).add(e.dstLab);
			qvs.remove(e.dst);
		}else {//join Vertex
			ct.joins.get(qid).add(e.dstLab);
			qvs.get(e.dst).remove(coreId);
		}
	}
	if(prevCore != null) {
//		q_cts.get(qid).get(q_cts.get(qid).size()-1);
		if(prevCore.joins.get(qid).contains(core.lab))
			ct.joins.get(qid).add(core.lab);
	}
	
	ct.makeTables(batch);
	qvs.remove(coreId);
	if(qvs.size() != 0) {
		makeCoreTable(qid, qvs, ct);
	}
  }

//Declare that emitted tuples contain a word field  
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	for(int i=0; i<numSlave; i++) {
		
		if(MODE == PROPOSED || MODE == PROPOSED_EXT ) {
		//proposed method
		    declarer.declareStream("data_"+i, new Fields("data"));
		    declarer.declareStream("query_"+i, new Fields("query"));
		}
	    
		if(MODE == NPV) {
			//NNT method
		    declarer.declareStream("data_"+i+"_NNT", new Fields("data"));
		    declarer.declareStream("query_"+i+"_NNT", new Fields("query"));	    
		}
		
		if(MODE == DAG) {	    
		    declarer.declareStream("data_"+i+"_DAG", new Fields("data"));
		    declarer.declareStream("query_"+i+"_DAG", new Fields("query"));
		}
	    
	
	}
	//proposed method
//	if(MODE == PROPOSED)
		declarer.declareStream("new_query", new Fields("new_query"));
    
    if(MODE == TURBO) {
    //turbo flux
	    declarer.declareStream("data_1_TB", new Fields("data"));
	    declarer.declareStream("query_1_TB", new Fields("query"));
    }
    if(MODE == TOP_K)
    {
    	 declarer.declareStream("data_1_TK", new Fields("data"));
 	    declarer.declareStream("query_1_TK", new Fields("query"));
    }
    
    //NNT method   
//    declarer.declareStream("new_query", new Fields("new_query"));
  }
}
