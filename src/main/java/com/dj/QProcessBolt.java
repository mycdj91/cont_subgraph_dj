package com.dj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class QProcessBolt extends BaseBasicBolt {

	
	
	HashMap<Integer, ArrayList<CoreTable>> l_cores;
	
	Set<Integer> checkLab;
	
	
	int now;
	int batch;
	
	ArrayList<DAG> dags;
	
	
	ArrayList<ArrayList<NNT>> qnnts;
	
	ArrayList<CopyOnWriteArrayList<Vertex>> vertices_DAG;
	
	ArrayList<CopyOnWriteArrayList<Vertex>> vertices_NNT;
	ArrayList<Integer[]> domiInfo;
	
	ConcurrentHashMap<Integer, Vertex> vs;    	
	
	
	ArrayList<HashMap<Integer, Vertex>> qtf;
//	ArrayList<ArrayList<TEdge>> dtf;
	
//	HashMap<Integer, ArrayList<TEdge>> dcg; 
	
//	ArrayLisT<ArrayList<NPV>> 
	
	
	HashMap<Integer, ArrayList<Vertex>> NTF;
	
	HashMap<Integer, ArrayList<Edge>> EF;
	
	long sn, en;
	int size;
	
	public QProcessBolt(int _batch) {
		// TODO Auto-generated constructor stub
		l_cores = new HashMap<Integer, ArrayList<CoreTable>>();
		
		now = -1;
		batch = _batch;
		
		qnnts = new ArrayList<ArrayList<NNT>>();
		vertices_NNT = new ArrayList<CopyOnWriteArrayList<Vertex>>();
		vertices_DAG = new ArrayList<CopyOnWriteArrayList<Vertex>>();
		
		vs = new ConcurrentHashMap<Integer, Vertex>();
		domiInfo = new ArrayList<Integer[]>();
		
		dags = new ArrayList<DAG>();
		
		
		sn = System.nanoTime();
		
		checkLab = new HashSet<Integer>();
		
		//tf
		qtf = new ArrayList<HashMap<Integer, Vertex>>();
		
		NTF = new HashMap<Integer, ArrayList<Vertex>>();
		EF = new HashMap<Integer, ArrayList<Edge>>();
		
		for(int i =0; i<5; i++) {
			NTF.put(i, new ArrayList<Vertex>());
		}
//		dtf = new ArrayList<ArrayList<TEdge>>();		
//		dcg = new HashMap<Integer, ArrayList<TEdge>>(); 
		
		size = 0;
	}
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		if(input.getSourceStreamId().contains("DAG")){
			if(input.getSourceStreamId().contains("query")) {
				DAG d = (DAG)input.getValue(0);
				dags.add(d);		
				vertices_DAG.add(new CopyOnWriteArrayList<Vertex>());
				
				size += d.source.size() * 32 ;
			}else{
				Edge e = (Edge)input.getValue(0);
				try {
					Thread.sleep(0, 1);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
//				System.out.println("check DAG2");
				//subgraph check
				if(e.time != now && now != -1) {
					if(e.time > MergeBolt.latest)
						checkDAG(collector, input.getSourceStreamId());
					System.out.println(e.time + " prev " + size);
				}
				//set index ok
				addEdgesDAG(e);
				
				//remove edges ok
				if(e.time != now) {
					now = e.time;
					if(now-batch >= 0)
						removeVerticesDAG();
				}
			}
		}
		else if(input.getSourceStreamId().contains("NNT")){
			
			//query NNT
			if(input.getSourceStreamId().contains("query")) {
				ArrayList<NNT> ns = (ArrayList<NNT>)input.getValue(0);
				qnnts.add(ns);
				vertices_NNT.add(new CopyOnWriteArrayList<Vertex>());
				domiInfo.add(new Integer[ns.size()]);
				size += ns.size() * 2;
			}else{
				Edge e = (Edge)input.getValue(0);
//				try {
////					Thread.sleep(0, 1);
//				} catch (InterruptedException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}

				//subgraph check
				if(e.time != now && now != -1) {
//					checkDominate(collector);
//					System.out.println("nnt " + e.time);
					
					en = System.currentTimeMillis();			
					System.out.println(e.time + " prev " + size);
//					System.out.println("Total = " + e.time + " Elapsed = " + (en-sn));
				}
				//set index
				addEdgesNNT(e);
				
				//remove edges
				if(e.time != now) {
					now = e.time;
					if(now-batch >= 0)
						removeVerticesNNT();
				}
			}			
		}
		else if(input.getSourceStreamId().contains("TB")) {
			//query TB
			if(input.getSourceStreamId().contains("query")) {
				
				HashMap<Integer, Vertex> qvs;
				qvs = (HashMap<Integer, Vertex>)input.getValue(0);
				qtf.add(qvs);
				size += qvs.size();
				
			}else{
				Edge e = (Edge)input.getValue(0);
				try {
					Thread.sleep(0, 1);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				//subgraph check
				if(e.time != now && now != -1) {
					
					Set<Integer> labs = l_cores.keySet();
					
					
					checkTF(collector);
					System.out.println(e.time + " prev " + size);
					
//					System.out.println("sub " + e.time);
				}
				//set index
				addEdgesTF(e);
				
				//remove edges
				if(e.time != now) {
					now = e.time;
					if(now-batch >= 0)
						removeTF(now-batch);
				}
				
				long end  = System.nanoTime();
//				System.out.println("F = " + (end - sn));
			
			}			
			
		}		
		
		else if(input.getSourceStreamId().contains("TK")) {
			//query TB
			if(input.getSourceStreamId().contains("query")) {
				
				HashMap<Integer, Vertex> qvs;
				qvs = (HashMap<Integer, Vertex>)input.getValue(0);
				for(int i : qvs.keySet()) {
					size += qvs.get(i).edges.size() * 32;
				}
				
				qtf.add(qvs);
				size += qvs.size()*32;
				
			
				
			}else{
				Edge e = (Edge)input.getValue(0);
				try {
					Thread.sleep(0, 1);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				//subgraph check
				if(e.time != now && now != -1) {
					
					Set<Integer> labs = l_cores.keySet();
					
					
					checkTK(collector, e.time);
					
					long end  = System.nanoTime();
					System.out.println(e.time + " prev " + size);
//					System.out.println(e.time + " " + (end-sn) + " ");
				}
				//set index
			
				addEdgesTK(e);
//				System.out.println(e.time + " next " + size);
				//remove edges
				if(e.time != now) {
					now = e.time;
					if(now-batch >= 0)
						removeTK(now-batch);
				}
				
//				long end  = System.nanoTime();
//				System.out.println("F = " + (end - sn));
			
			}			
			
		}	
		else if(input.getSourceStreamId().contains("query")) {
//			System.out.println("coming query");
			CoreTable qcs = (CoreTable)input.getValue(0);
			
			size += qcs.vls.size();
//			for(CoreTable c : qcs) {
				containCheck(qcs);
				
//				c.coreLab
//				qcs.get(i)
//				System.out.println(c.toString());
//			}
			//data
		}else if(input.getSourceStreamId().contains("data")){
			Edge e = (Edge)input.getValue(0);
			try {
				Thread.sleep(0, 1);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
//			System.out.println("data = " +e.toString());
			
			if(e.time != now && now != -1) {
				
//				
//				Set<Integer> labs = l_cores.keySet();
				
				if(e.time > MergeBolt.latest) {
					boolean subCheck = false;
						for(int lab : checkLab) {
						
		//					System.out.println(lab+"start-------------");
							for(CoreTable c : l_cores.get(lab)) {
								
								if(c.subgraphCheck(e.time)) {
		//							System.out.println("subgraph " + e.time);
									subCheck = true;
									collector.emit("sub_core", new Values(c));
								}
								
							}
			
						}
						
						if(subCheck)
							collector.emit("subgraph", new Values(e.time));
						
						System.out.println(e.time + " prev " + size);
				}
				checkLab.removeAll(checkLab);
				en = System.currentTimeMillis();						
//				System.out.println("Total = " + e.time + " Elapsed = " + (en-sn));
			}
			
			if(l_cores.get(e.srcLab) != null)
			{
				checkLab.add(e.srcLab);
				addEdge(e.srcLab, e, e.dstLab, collector);
			}
			if(l_cores.get(e.dstLab) != null)
			{
				checkLab.add(e.dstLab);
				addEdge(e.dstLab, e, e.srcLab, collector);
			}
			if(e.time != now) {
				now = e.time;
				if(now-batch >= 0)
					removeEdges();
			}
			
			
			long end  = System.nanoTime();
//			System.out.println("F = " + (end - sn));
		}
		
	}
	
	private void checkTK(BasicOutputCollector collector, int time) {
		// TODO Auto-generated method stub
		for(int i=0; i < qtf.size(); i++) {
			Set<Integer> keys = qtf.get(i).keySet();
			HashMap<Integer, HashSet<Integer>> candidates = new HashMap<Integer, HashSet<Integer>>();
			
			
			for(int key : keys) {
				candidates.put(key, new HashSet<Integer>());
//				System.out.println("candidate Check3");
			}
			for(int key : keys) {
				
				Vertex v = qtf.get(i).get(key);
				
				for(Edge e : v.edges) {
					int lkey = v.lab * 10 + e.dstLab;
					double tw = e.weight;  
					if(EF.get(lkey)!= null) {
						for(Edge ef : EF.get(lkey)) {
//							if(ef.weight > tw) {
								candidates.get(key).add(ef.src);
								candidates.get(e.dst).add(ef.dst);
//							}
						}
					}
				}
			}
			
			boolean check = true;
			for(int key : keys) {
//				System.out.println("candidate Check2");
				if(candidates.get(key).size() > 0) {
//					System.out.println("candidate Check");
					HashSet<Integer> temp = candidates.get(key);
					ArrayList<Integer> rms = new ArrayList<Integer>();
					
					for(int id : temp) {
						Vertex v = qtf.get(i).get(key);
						int [] nnt = v.NNT;
						for(Vertex tv : NTF.get(v.lab)) {
							if(tv.id == id) {
								int []tnnt  = tv.NNT;
								for(int j=0; j<nnt.length; j++) {
									if(nnt[j] > tnnt[j]) {
//										rms.add(id);
										break;
									}
								}
							}
						}
					}
					for(int r : rms) {
						temp.remove(r);
					}
				}
				if(candidates.get(key).size() == 0 ) {
					check = false;
					break;
				}
			}
			if(check) {
				collector.emit("subgraph", new Values(time));
//				System.out.println("tt"+time);
			}
		}
		
		
	}

	private void removeTK(int now) {
		// TODO Auto-generated method stub
//		int lm = batch;
		for(int key : EF.keySet()) {
			int i = 0;
			ArrayList<Integer> temp = new ArrayList<Integer>();
			
			for(Edge e : EF.get(key)) {
				
				if( e.time < now ) {
					temp.add(i);
				}
				i++;
			}
			
			for(i = temp.size()-1; i>0; i--) {
				EF.get(key).remove(temp.get(i));
			}
			
			size -= temp.size() * 32;
		}
		
		for(int i=0; i<5; i++) {		
			for(Vertex v : NTF.get(i)) {
				int t = v.removeTK(now);
				
				size -= t *32 ;
			}
			sortNTF(i);
		}
	}

	private void addEdgesTK(Edge e) {
		// TODO Auto-generated method stub
		
		if(vs.get(e.src) == null) {
			
			Vertex temp = new Vertex(e.src, e.srcLab);
			vs.put(e.src, new Vertex(e.src, e.srcLab));
			NTF.get(e.srcLab).add(temp);
			
			size += 32 * 2;
		}
		
		if(vs.get(e.dst) == null) {
			Vertex temp = new Vertex(e.dst, e.dstLab);
			vs.put(e.dst, new Vertex(e.dst, e.dstLab));
			NTF.get(e.dstLab).add(temp);
			size += 32 * 2;
		}
		int key = e.srcLab*10 + e.dstLab;
		
		if(EF.get(key) == null)
		{
			EF.put(key, new ArrayList<Edge>());
		}
		ArrayList<Edge> es = EF.get(key);
		
		int idx = -1;
		for(int i=0;i <es.size(); i++) {
			
			if(es.get(i).weight < e.weight)
			{
				idx = i;
				break;
			}
		}
		if(idx == -1) {
			idx = 0;
		}
		es.add(idx, e);
		size += 32;
		
		Edge cloneE;
		try {
			cloneE = (Edge)e.clone();
			Edge dstE = new Edge(e.dst, e.src, e.dstLab, e.srcLab, e.time, e.weight, e.op); 
			
			vs.get(e.src).addEdge(cloneE);	
			vs.get(e.dst).addEdge(dstE);
			size += (32*2);
			
			sortNTF(e.srcLab);
			sortNTF(e.dstLab);
		
		} catch (CloneNotSupportedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	private void sortNTF(int lab) {
		
		ArrayList<Vertex> src_vs = NTF.get(lab);
//		ArrayList<Vertex> dst_vs = NTF.get(e.dstLab);
		boolean check = true;
		//sort
		while(check) {
			check = false;
			for(int i=0; i<src_vs.size()-1; i++) {
				Vertex fv = src_vs.get(i);
				Vertex sv = src_vs.get(i+1);
				if(fv.degree < sv.degree) {
					src_vs.remove(i+1);
					src_vs.add(i+1, fv);
					src_vs.remove(i);
					src_vs.add(i, sv);
					check = true; 
				}
			}
		}
//		check = true;
//		
//		while(check) {
//			check = false;
//			for(int i=0; i<dst_vs.size()-1; i++) {
//				Vertex fv = dst_vs.get(i);
//				Vertex sv = dst_vs.get(i+1);
//				if(fv.degree < sv.degree) {
//					dst_vs.remove(i+1);
//					dst_vs.add(i+1, fv);
//					dst_vs.remove(i);
//					dst_vs.add(i, sv);
//					check = true; 
//				}
//			}
//		}
	}

	private boolean cascadeCheck(Vertex v, HashMap<Integer, Vertex> q) {
		ArrayList<Integer> ids = v.getImplicits();						
		
		for(int i = ids.size()-1 ; i> 0 ; i--) {
			if(q.get(i).type == 1)
			{
				v.setStatus(ids.remove(i));
			}
		}
		
		while(ids.size() > 0) {
			int id = ids.remove(0);
			Vertex vt = q.get(id);
			
			//also recheck
			if(vt.getStatus()!=2 ) {
				if(vt.isFull()) {
					if(!cascadeCheck(vt, q))
					{
						return false;
					}else {
						v.setStatus(id);
					}
				}else {
					return false;
				}
			}
		}	
		
		return true;
	}
	
	//TODO
	private void checkTF(BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		int i=0;
		for( HashMap<Integer, Vertex> q :  qtf) {
			
			boolean check = true;
			
			for( int k : q.keySet()) {
				Vertex v = q.get(k);
				if(v.type == 2 && v.getStatus() != 2) {
					
					//recheck
					if(v.isFull()) {
						check = cascadeCheck(v, q);
						if(!check)
							break;
					}else{
						check = false;
						break;
					}
					
				}
			}
			if(check) {
				//send query resut;
//				System.out.println("TF subgraph");
				
				CopyOnWriteArrayList<Vertex> temp = new CopyOnWriteArrayList<Vertex>();
				for( int k : q.keySet()) {
					
					Vertex v = q.get(k);
					
					for(ArrayList<Edge> es : v.dcg.values()) {
						temp.addAll(constVertices(es));
					}
				}
				
				int latest = 0 ;
				for(Integer key : q.keySet()) {

					Vertex v = q.get(key);
					int t = v.getLatestTime();
					if(latest < t) {
						latest = t;
					}
				}
				temp.get(0).domSize = i;
				temp.get(1).domSize = latest;
				collector.emit("subgraph", new Values(temp));
			}
			i++;
		}
	}
	
	private void removeTF(int t) {
		// TODO Auto-generated method stub
		for( HashMap<Integer, Vertex> q :  qtf) {
			for( int k : q.keySet()) {

				Vertex v = q.get(k);
				size -= v.removeTF(t);
			}
		}
	}

	private void addEdgesTF(Edge e) {
		// TODO Auto-generated method stub
		
		int s = e.srcLab;
		int d = e.dstLab;
		
//		int i=0; 
		for( HashMap<Integer, Vertex> q :  qtf) {
			for( int k : q.keySet()) {

				Vertex v = q.get(k);
				if(v.lab == s && v.isContains(d)) {
					
					int j = 0;
					for(Edge ve : v.edges) {
						if(ve.dstLab == d)
						{
							v.dcg.get(j).add(e);
							size += 20;
							boolean check = false;
							if(v.status.get(j) == 0) {
//								v.status.set(j, 1);
								Vertex dv = q.get(ve.dst);
								switch(dv.type) {
									case 0 : // normal
										int ds = dv.getStatus();
										if(ds == 2) {
											v.status.set(j, ds);
											check = true;
										}
										else		
											v.status.set(j, 1);
										
										break;
									case 1 : 
										v.status.set(j, 2);
										check = true;
										break;
								}
							}
							
							//cascade check
							if(check) {
								ArrayList<Integer> cas = new ArrayList<Integer>();
								
								if(v.type != 2) {
									cas.add(v.id);
								}
								while(cas.size() != 0 ) {
									int tid = cas.remove(0);
									
									Set<Integer> temp2 = q.keySet();
									for(int key2 : temp2) {
										Vertex v2 = q.get(key2);
										if(v2.id != tid) {
											int idx = v2.getIdx(tid);
											if(idx != -1 && v2.status.get(idx) == 1) {
												v2.status.set(idx, 2);
												if(v2.type != 2 && v2.getStatus() == 2)
												{
													cas.add(v2.id);
												}
											}
										}
									}
								}
									
							}
							
						}
						j++;
					}
					
//					ArrayList<ArrayList<TEdge>> dtf;
					
//					HashMap<Integer, TEdge> dcg; 
					
					/*
					 * for(TEdge dt : dtf.get(i)) { if(dt.status != 2 ) { int temp = dt.dst;
					 * 
					 * } }
					 */
				}
			}
			
//			i++;
		}
		
		
	}

	

	private void checkDAG(BasicOutputCollector collector, String id) {
		// TODO Auto-generated method stub
//		ConcurrentHashMap<Integer, Vertex> tvs = vs.clone();
			
//		if(id.equals("data_0_DAG")) {
//			System.out.println(id + " CHECK DAG " );
//			System.out.println(id + " DAG == " + dags.get(0).vs.toString());
//			System.out.println(id + " VS == " + vs.toString());
//			System.out.println(id + " VS DAG == " + vertices_DAG.get(0).toString());
			for(int idx = 0; idx<dags.size(); idx++) {
				DAG d  = dags.get(idx); 
				boolean chk = true;
				for(Vertex v : d.source) {
					if(!cotainLab(v.lab, idx)) {
						chk = false;
					}
				}			
				//send Message source to sink
				if(chk) {
//					System.out.println("check 1 " + idx);
					HashSet<Vertex> temp = new HashSet<Vertex>();
					for(Vertex v1 : d.source) {
//						System.out.println("source = " + v.toString() );
						ArrayList<Integer> edgeLabels1 = v1.getEdgeLabels();
						temp.addAll(d.getNeighbors(v1.id));
						for(Vertex vd :  vertices_DAG.get(idx)) {
							if(vd.lab == v1.lab) {
								for(Edge e : vd.edges) {
									if(edgeLabels1.contains(e.dstLab)) {
										Vertex t =vs.get(e.dst); 
//										System.out.println(t);
										if(t != null)
											t.addMessage(v1.id, v1.lab);
									}
								}
							}
						}
						
					}					
					while(temp.size()!= 0 ) {
//						System.out.println("temp = "+ temp.toString());
//						System.out.println("check 2 " );
						HashSet<Vertex> ttemp = new HashSet<Vertex>();
						for(Vertex  v : temp) {
							ArrayList<Integer> edgeLabels = v.getEdgeLabels();
							//add all
							ttemp.addAll(d.getNeighbors(v.id));
//							System.out.println("TTEMP = " + ttemp.toString());
							for(Vertex vd :  vertices_DAG.get(idx)) {
								if(vd.lab == v.lab) {
									collector.emit("subgraph", new Values(new SubgraphNNT(vertices_DAG.get(idx), d.qid)));
									if(d.sink.equals(v) && vd.sources.size() > 0) {
//										System.out.println("subgraph");
										collector.emit("subgraph", new Values(new SubgraphNNT(vertices_DAG.get(idx), d.qid)));
										break;
									}
									else {
										if(v.type == 1) {
											
//											System.out.println(v.toString() + " check type");
											int dom = v.domSize;
											HashSet<Integer> tempLabs = new HashSet<Integer>();
											
											for(Integer lab : vd.sourceLabs) {
												tempLabs.add(lab);
											}
//											System.out.println("DOMI CHECK " + dom);
//											System.out.println("DOMI CHECK 2 " + tempLabs.toString());
											if(tempLabs.size() != dom)
											{
												vd.sources.clear();
												vd.sourceLabs.clear();
												temp.clear();
												continue;
											}
//											System.out.println("Domi ok");
										}
										for(Edge e : vd.edges) {
											if(edgeLabels.contains(e.dstLab)) {
												if(vs.get(e.src).sources.size() !=0) {
													vs.get(e.dst).addMessage(vs.get(e.src));											
												}
											}									
										}
									}
								}
							}
						}
						temp.clear();
						temp.addAll(ttemp);
					}
					clearSources();
				}
			}
//		}
	}
	
	
	private void clearSources() {
		// TODO Auto-generated method stub
		Set<Integer> keys = vs.keySet();
		for(int k : keys) {
			vs.get(k).sourceLabs.clear();
			vs.get(k).sources.clear();
		}
	}

	private boolean cotainLab(int lab, int idx) {
		// TODO Auto-generated method stub
		for(Vertex v : vertices_DAG.get(idx)) {
			if(v.lab == lab) 
				return true;
		}
		return false;
	}

	
	private ArrayList<Vertex> constVertices(ArrayList<Edge> es) {
		// TODO Auto-generated method stub
		ConcurrentHashMap<Integer, Vertex> temp = new ConcurrentHashMap<Integer, Vertex>();
		for(Edge e : es ) {
			
			if(temp.get(e.src) == null) {
				temp.put(e.src, new Vertex(e.src, e.srcLab));
			}
			
			if(temp.get(e.dst) == null) {
				temp.put(e.dst, new Vertex(e.dst, e.dstLab));
			}
			
			Edge cloneE;
			try {
				cloneE = (Edge)e.clone();
				Edge dstE = new Edge(e.dst, e.src, e.dstLab, e.srcLab, e.time); 
				
				temp.get(e.src).addEdge(cloneE);
				temp.get(e.dst).addEdge(dstE);
			
			} catch (CloneNotSupportedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return new ArrayList<Vertex>(temp.values());
		
	}



	private void addEdgesDAG(Edge e) {
		// TODO Auto-generated method stub
		
		if(vs.get(e.src) == null) {
			vs.put(e.src, new Vertex(e.src, e.srcLab));
			size += 32;
			for(int i=0; i< dags.size(); i++) {
				DAG d = dags.get(i);
				if(d.isContainLab(e.srcLab)) {
					vertices_DAG.get(i).add(vs.get(e.src));	
					size += 32;
				}	
			}
		}
		
		if(vs.get(e.dst) == null) {
			vs.put(e.dst, new Vertex(e.dst, e.dstLab));
			size += 32;
			for(int i=0; i< dags.size(); i++) {
				DAG d = dags.get(i);
				if(d.isContainLab(e.dstLab)) {
					vertices_DAG.get(i).add(vs.get(e.dst));			
					size += 32;
				}	
			}
		}
		
		Edge cloneE;
		try {
			cloneE = (Edge)e.clone();
			Edge dstE = new Edge(e.dst, e.src, e.dstLab, e.srcLab, e.time); 
			
			vs.get(e.src).addEdge(cloneE);
			vs.get(e.dst).addEdge(dstE);
			size += 40;
		
		} catch (CloneNotSupportedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}

	private void checkDominate(BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		for(int i=0; i<qnnts.size(); i++) {
			ArrayList<NNT> temp = qnnts.get(i);
			CopyOnWriteArrayList<Vertex> tempVertices = vertices_NNT.get(i);
			Integer[] tempDomi = domiInfo.get(i);
		
			Arrays.fill(tempDomi, 0);
			
			ArrayList<Integer> an = new ArrayList<Integer>();
			for(int j=0; j<domiInfo.get(i).length; j++) {
				an.add(0);
			}
			
			
			for(Vertex v : tempVertices) {
				for(int j=0; j<temp.size(); j++) {
					NNT tempN = temp.get(j);
					if(tempN.lab == v.lab) {
						int dom = tempN.getDomiInfo(v.NNT);
						if(an.get(j) < dom)
						{
							an.set(j, dom);
						}
//						if(tempDomi[j] < dom) {
//							tempDomi[j] = dom;
//						}
					}
				}
			}
			boolean check = true;
//			for(int j=0; j<tempDomi.length; j++){
//				if(tempDomi[j] != 5) {
//					check = false;
//					break;
//				}
//			}
//			
//			for(int j=0; j<an.size(); j++){
//				if(an.get(j) != 5) {
//					check = false;
//					break;
//				}
//			}
			
			
			if(check && tempVertices.size() != 0) {
//				
//				for(Vertex v : tempVertices) {
//					for(Edge e : v.edges)
//					{	
//					}
//				}
				collector.emit("subgraph", new Values(new SubgraphNNT(tempVertices, temp.get(0).qId)));
			}
		}
	}

	private void addEdgesNNT(Edge e) {
		// TODO Auto-generated method stub
		
		if(vs.get(e.src) == null) {
			vs.put(e.src, new Vertex(e.src, e.srcLab));
			size += 32;
			
			for(int i=0 ;i < qnnts.size(); i++) {
				for(NNT n : qnnts.get(i)) {
					if(n.lab == e.srcLab) {
						vertices_NNT.get(i).add(vs.get(e.src));
						size += 32;
						break;
					}
				}	
			}
		}
		
		if(vs.get(e.dst) == null) {
			vs.put(e.dst, new Vertex(e.dst, e.dstLab));
			for(int i=0 ;i < qnnts.size(); i++) {
				for(NNT n : qnnts.get(i)) {
					if(n.lab == e.dstLab) {
						vertices_NNT.get(i).add(vs.get(e.dst));
						size += 32;
						break;
					}
				}	
			}
		}
		
		Edge cloneE;
		try {
			cloneE = (Edge)e.clone();
			Edge dstE = new Edge(e.dst, e.src, e.dstLab, e.srcLab, e.time); 
			
			vs.get(e.src).addEdge(cloneE);
			vs.get(e.dst).addEdge(dstE);
			size += 40;
		} catch (CloneNotSupportedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	private void removeVerticesDAG() {
		
		ArrayList<Integer> rmids = new ArrayList<Integer>();
		for (Enumeration<Vertex> keys = vs.elements(); keys.hasMoreElements();) 
		{		

			Vertex v = keys.nextElement();
			for(int i= v.edges.size()-1; i>=0; i--)
			{
				Edge e = v.edges.get(i);
				if(now-batch > e.time)//time 
				{
					size -= 20;
					int d = v.remove(e.dst);
					if(d == 0 ) {
						removeVertexDAG(v);
						rmids.add(v.id);
					}
					if(vs.get(e.dst) != null) {
						size -= 32;
						d = vs.get(e.dst).remove(e.src);
						if(d == 0 ) {
							removeVertexDAG(vs.get(e.dst));
							rmids.add(v.id);
						}
					}
				}
			}
		}
		
		for (CopyOnWriteArrayList<Vertex> vertices : vertices_DAG) {
			for(Vertex v : vertices) {
				for(int i= v.edges.size()-1; i>=0; i--)
				{
					Edge e = v.edges.get(i);
					if(now-batch > e.time)//time 
					{
						size -= 20;
						int d = v.remove(e.dst);
						if(d == 0 ) {
							removeVertexDAG(v);
							rmids.add(v.id);
						}
//						System.out.println("e = " + e.toString());
						if(vs.get(e.dst) != null) {
							d = vs.get(e.dst).remove(e.src);
							size -= 32;
							if(d == 0 ) {
								removeVertexDAG(vs.get(e.dst));
								rmids.add(v.id);
							}
						}
					}
				}
			}
		}
		
		for(int k : rmids) {
			vs.remove(k);
			size -= 32;
		}
	}

	private void removeVertexDAG(Vertex v) {
		// TODO Auto-generated method stub
		for(CopyOnWriteArrayList<Vertex> vDAG : vertices_DAG) {
			if(vDAG.contains(v)) {
				vDAG.remove(v);
			}
		}
	}

	private void removeVerticesNNT() {
		// TODO Auto-generated method stub
		ArrayList<Integer> rmids = new ArrayList<Integer>();
		for (Enumeration<Vertex> keys = vs.elements(); keys.hasMoreElements();) 
		{		

			Vertex v = keys.nextElement();
			for(int i= v.edges.size()-1; i>=0; i--)
			{
				Edge e = v.edges.get(i);
				if(now-batch > e.time)//time 
				{
					size -= 20;
					int d = v.remove(e.dst);
					if(d == 0 ) {
						removeVertex(v);
						rmids.add(v.id);
					}
					if(vs.get(e.dst) != null) {
						d = vs.get(e.dst).remove(e.src);
						size -= 20;
						if(d == 0 ) {
							removeVertex(vs.get(e.dst));
							rmids.add(v.id);
						}
					}
				}
			}
		}
		
		for (CopyOnWriteArrayList<Vertex> vertices : vertices_NNT) {
			for(Vertex v : vertices) {
				for(int i= v.edges.size()-1; i>=0; i--)
				{
					Edge e = v.edges.get(i);
//					System.out.println("now - btach = "+ (now-batch) + ",, " + e.time);
					if(now-batch > e.time)//time 
					{
						int d = v.remove(e.dst);
						size -= 20;
						if(d == 0 ) {
							removeVertex(v);
							rmids.add(v.id);
						}
//						System.out.println("e = " + e.toString());
						if(vs.get(e.dst) != null) {
							d = vs.get(e.dst).remove(e.src);
							size -= 20;
							if(d == 0 ) {
								removeVertex(vs.get(e.dst));
								rmids.add(v.id);
							}
						}
					}
				}
			}
		}
		
		for(int k : rmids) {
			vs.remove(k);
			size -= 20;
		}
		
	}

	private void removeVertex(Vertex v) {
		// TODO Auto-generated method stub
//		vs.remove(v.id);
		for(CopyOnWriteArrayList<Vertex> vNNT : vertices_NNT) {
			if(vNNT.contains(v)) {
				vNNT.remove(v);
			}
		}
	}

	private void removeEdges() {
		Set<Integer> labs = l_cores.keySet();
		for(int lab : labs) {
			for(CoreTable c : l_cores.get(lab)) {
				size -= c.removeEdges(now) * 20;
			}
		}
	}
	

	private void addEdge(int cLab, Edge e, int dLab, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		
		for(CoreTable c : l_cores.get(cLab)) {
			if(c.vls.contains(dLab))
			{
				EdgeTable temp = c.et.get(dLab);
				/*
				 * if(e.time != temp.now && temp.now != -1) { if(c.subgraphCheck(e.time)) {
				 * System.out.println("subgraph " + e.time); collector.emit("subgraph", new
				 * Values(c)); } }
				 */
				
				int idx = c.vls.indexOf(dLab);
				int op = c.op.get(idx) ;
				if(c.op.get(idx) != 0) {
					double weight = c.w.get(idx);
					switch(op) {
						case 1 : 
							
							if(weight >= e.weight )
								return;
							break;
						case 2 : 
							if(weight <= e.weight )
								return;
							break;
						case 3 : 
							if(weight != e.weight )
								return;
							break;
					}
				}
				c.addEdge(e, dLab);
				size += 20;
			}
		}
		
	}

	private void containCheck(CoreTable c) {
		// TODO Auto-generated method stub
		
		if(l_cores.get(c.coreLab) == null) {
			l_cores.put(c.coreLab, new ArrayList<CoreTable>());
			l_cores.get(c.coreLab).add(c);
//			return false;
		}else {
			boolean isNone = true;
			for(CoreTable lc :  l_cores.get(c.coreLab)) {
				if(lc.tid == c.tid) {
					
					lc.migrate(c);
					//migrate data
					//if(lc.)
					
					isNone = false;
				}
			}
			if(isNone){
				l_cores.get(c.coreLab).add(c);
			}
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("sub_core", new Fields("sub_core"));
		declarer.declareStream("subgraph", new Fields("subgraph"));
//		declarer.declareStream("subgraph_NNT", new Fields("subgraph_NNT"));
	}

}

