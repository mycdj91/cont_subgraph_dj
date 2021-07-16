package com.dj;

import java.util.ArrayList;
import java.util.HashMap;

import static java.util.stream.Collectors.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class MergeBolt extends BaseBasicBolt{
	

	public ConcurrentHashMap<Integer, ArrayList<CoreTable>> q_cts;
	
	
	public ArrayList<CopyOnWriteArrayList<Vertex>> q_vertices; 
	ArrayList<Integer> subCount;
	long totalTime;
//	ArrayList<Integer> q_sub_check ;
	int batch;
	
	boolean subCheck;
	
	int mode;
	
	long sn;
	long en;
	
	static int latest = 0 ; 
	
	public MergeBolt(int _batch, int _mode) {
		// TODO Auto-generated constructor stub
//		q_cts = new HashMap<Integer, ArrayList<CoreTable>>();
//		q_sub_check = new ArrayList<Integer>();
		batch = _batch;
		totalTime = 0;
		subCheck = false;
		subCount = new ArrayList<Integer>();
		mode = _mode;
		
		q_vertices = new ArrayList<CopyOnWriteArrayList<Vertex>>();
		
		sn = System.nanoTime();
	}
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		
		if(input.getSourceStreamId().equals("sub_core")) {
			
			CoreTable c = (CoreTable)input.getValue(0);
//			System.out.println("subraph table " + c.toString());
			ArrayList<Integer> qs = new ArrayList<Integer>();
			ArrayList<Integer> is = new ArrayList<Integer>();
			
			/*
			 * while(subCheck) { Utils.sleep(100); }
			 */
			Iterator<Integer> iter = ((ArrayList<Integer>)c.quries.clone()).iterator();
			while(iter.hasNext()) {
				int qid  = iter.next();
//				q_sub_check.set(qid, q_sub_check.get(qid)+1);
				for(int i= 0; i<q_cts.get(qid).size(); i++)
				{
					if(q_cts.get(qid).get(i).tid == c.tid) {
						
						qs.add(qid);
						is.add(i);
					}
				}
			}
			
			for(int i=0; i < qs.size(); i++) {
				q_cts.get(qs.get(i)).set(is.get(i), c);
				if(qs.get(i) >= subCount.size())
					subCount.add(1);
				else
					subCount.set(qs.get(i),subCount.get(qs.get(i))+1);
			}
			
		}else if(input.getSourceStreamId().equals("subgraph")) {
			switch(mode) {
			case MasterBolt.PROPOSED : 
			case MasterBolt.PROPOSED_EXT : 
				subCheck = true;
				int time = input.getInteger(0);
				
				if(latest < time)
					latest = time;
				
//				runSubgraph(time);
				subCheck = false;
				en = System.nanoTime();						
//				System.out.println("Total = " + time + " Elapsed = " + (en-sn));
//				System.out.println(time + " " + (en-sn));
				
				
				break;
			case MasterBolt.NPV : 
				//subgraph 
				SubgraphNNT temp = (SubgraphNNT)input.getValue(0);
				
				SubgraphNNT snnt = null;

				try {
					snnt = (SubgraphNNT)temp.clone();
				} catch (CloneNotSupportedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if(snnt.vs.size() != 0 && snnt.vs.get(0).edges.size() !=0 ) {
//					System.out.println("snnt = "+ snnt.qid+", time = " + snnt.vs.get(0).edges.get(0).time + ", " + snnt.vs.get(0).edges.get(0).toString());
					
					boolean isSub = subgraphCheck(snnt.vs, snnt.qid);
					if(isSub){					
//						System.out.println(snnt.qid+" Query OK");
						
					}
					else {
//						System.out.println(snnt.qid+" not sub");
					}
					if(snnt.vs.size() != 0 && snnt.vs.get(0).edges.size() !=0 ) {
//						System.out.println("snnt = "+ snnt.qid+", time = " + snnt.getTime());
					}
//					", " + snnt.vs.get(0).edges.get(0).toString());
					
				}
				
								en = System.nanoTime();										
			//	System.out.println("Total = " + snnt.getTime() + " Elapsed = " + (en-sn));
//				System.out.println(snnt.getTime() + " " + (en-sn));
				break;
			case MasterBolt.DAG : 
				SubgraphNNT tempDAG = (SubgraphNNT)input.getValue(0);
				
				SubgraphNNT snntDAG = null;
				
				try {
					snntDAG = (SubgraphNNT)tempDAG.clone();
				} catch (CloneNotSupportedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if(snntDAG.vs.size() != 0 && snntDAG.vs.get(0).edges.size() !=0 ) {
//					System.out.println("snnt = "+ snntDAG.qid+", time = " + snntDAG.vs.get(0).edges.get(0).time + ", " + snntDAG.vs.get(0).edges.get(0).toString());					
					boolean isSub = subgraphCheck(snntDAG.vs, snntDAG.qid);
					if(isSub){					
//						System.out.println(snntDAG.qid+" Query OK");
						
					}
					else {
//						System.out.println(snntDAG.qid+" not sub");
					}
					if(snntDAG.vs.size() != 0 && snntDAG.vs.get(0).edges.size() !=0 ) {
//						System.out.println("snnt = "+ snntDAG.qid+", time = " + snntDAG.getTime());
					}
//					", " + snnt.vs.get(0).edges.get(0).toString());
					
				}
				if(latest < snntDAG.getTime())
					latest = snntDAG.getTime();
				
				en = System.nanoTime();										
//				System.out.println("Total = " + snntDAG.getTime() + " Elapsed = " + (en-sn));
//				System.out.println(snntDAG.getTime() + " " + (en-sn));
				
				break;
				
				
			case MasterBolt.TURBO : 
				CopyOnWriteArrayList<Vertex> result = (CopyOnWriteArrayList<Vertex>)input.getValue(0);
				
				
				boolean isSub = subgraphCheck(result, result.get(0).domSize);
				int latest = result.get(1).domSize; 
				en = System.nanoTime();										
//				System.out.println("Total = " + latest+ " Elapsed = " + (en-sn));
//				System.out.println(latest+ " " + (en-sn));
				
				break;
			case MasterBolt.TOP_K :
				int t = input.getInteger(0);
				en = System.nanoTime();
//				System.out.println(t+ " " + (en-sn));
				break;
		
			
			}
		
			
		}else {
			
			switch(mode) {
				case MasterBolt.PROPOSED :
				case MasterBolt.PROPOSED_EXT : 
					q_cts = (ConcurrentHashMap<Integer, ArrayList<CoreTable>>)input.getValue(0);
					subCount.clear();
					for(int i=0; i<q_cts.size(); i++)
						subCount.add(0);
					break;
				case MasterBolt.NPV : 
					SubgraphNNT qnnt = (SubgraphNNT)input.getValue(0);
					q_vertices.add(qnnt.vs);
					break;
				case MasterBolt.DAG :
					SubgraphNNT qdag = (SubgraphNNT)input.getValue(0);
					q_vertices.add(qdag.vs);
					break;
					
				case MasterBolt.TURBO :
					SubgraphNNT qtb = (SubgraphNNT)input.getValue(0);
					q_vertices.add(qtb.vs);
					break;
					
			}
		}
		
	}

	private void runSubgraph(int time) {
		// TODO Auto-generated method stub
		ArrayList<Integer> sub_list = new ArrayList<Integer>();
		for(int q=0; q< q_cts.size(); q++) {
			if(q_cts.size() == subCount.size() && subCount.get(q) >= q_cts.get(q).size())
			{
				subCount.set(q, 0);
				for(CoreTable c : q_cts.get(q)) {
					Iterator<Integer> iter = ((ArrayList<Integer>)c.quries.clone()).iterator();
					while(iter.hasNext()) {
						int qid  = iter.next();
						if(qid >= q && !sub_list.contains(q)) {
							int lastIndex = c.st.size()-1;
							if(c.st.size() !=0 && c.st.get(lastIndex).removeTime >= time - batch) {
								int cid = c.st.get(lastIndex).coreId;
								Set<Integer> joins = new HashSet<Integer>();
								for(CoreTable fJoin : q_cts.get(qid)) {
									if(fJoin.joins.get(qid)!= null)
										joins.addAll(fJoin.joins.get(qid));
								}
	//							System.out.println("joins = "+joins);
	//							c.co
	//							c.st.get(c.st.size()-1).cor
	//							int rmTime = c.st.get(lastIndex).removeTime;
								Set<Edge> edges = c.st.get(lastIndex).vertices;
	//							System.out.println("cid = "+ cid +", "+edges.toString());
								boolean joinCheck = true;
								for(int j : joins) {
									
									ArrayList<Integer> joinVertices = new ArrayList<Integer>();
									if(c.coreLab == j)
										joinVertices.add(cid);
									else {
										for(Edge e : edges) {
											if(e.srcLab == j)
												joinVertices.add(e.src);
											else if(e.dstLab == j)
												joinVertices.add(e.dst);
										}
									}
	//								System.out.println("j = "+ j +", "+joinVertices.toString());
//										int count = 0;
									for(CoreTable innerC : q_cts.get(qid)) {
	//									innerC.
										if(innerC.joins.get(qid)!= null) {
											if(innerC.joins.get(qid).contains(j)) {
												int s = innerC.st.size();
												boolean containCheck = false;
												ArrayList<Integer> temp = new ArrayList<Integer>();
												if(j == innerC.coreLab) {
		//											System.out.println("INNNNNN" +innerC.toString());
		//											System.out.println(innerC.st.toString());
													for(int i = s-1; i >= 0; i--) {
		//												System.out.println("OUTINNN0"+innerC.st.get(i).toString());
														if(innerC.st.get(i).removeTime >= time-batch) {
															if(joinVertices.size() == 0) {
																temp.add(innerC.st.get(i).coreId);
																containCheck = true;
															}else if(joinVertices.contains(innerC.st.get(i).coreId)) {
																containCheck = true;
															}
														}else {
															break;
														}
													}
		//											System.out.println(containCheck);
												}else {
												
													for(int i = s-1; i >= 0; i--) {
														if(innerC.st.get(i).removeTime >= time-batch) {
		//													System.out.println("innerC +"+innerC.st.get(i).vertices);
															for(Edge e : innerC.st.get(i).vertices) {
																if(e.srcLab == j) {
																	if(joinVertices.size() == 0) {
																		temp.add(e.src);
																		containCheck = true;
																	}else if(joinVertices.contains(e.src)) {
																		containCheck = true;
																	}
																}else if(e.dstLab == j) {
																	if(joinVertices.size() == 0) {
																		temp.add(e.dst);
																		containCheck = true;
																	}else if(joinVertices.contains(e.dst)) {
																		containCheck = true;
																	}
																}
																
															}
														}else{
															break;
														}
													}
												}
												if(temp.size() > 0 )
													joinVertices.addAll(temp);
												if(!containCheck)
												{
													joinCheck = false;
													break;
												}
											}
										}
									
									}
									if(!joinCheck) {
//										System.out.println(qid+" not sub");
										sub_list.add(qid);
										break;
									}
								}
								if(joinCheck) {
//									System.out.println(qid+" Query OK");
									sub_list.add(qid);
								}
								
							}
						}
					}
					
				}
			}
		}
	}

	private boolean subgraphCheck(CopyOnWriteArrayList<Vertex> vs, int qid) {
		// TODO Auto-generated method stub
		class STP {
			Vertex p1;
			int p2;
			public STP(Vertex _p1, int _p2) {
				// TODO Auto-generated constructor stub
				this.p1 = _p1;
				this.p2 = _p2;
			}
		}
		CopyOnWriteArrayList<Vertex> qvs = (CopyOnWriteArrayList<Vertex>)q_vertices.get(qid).clone();
		Stack<STP> st = new Stack<STP>();
		boolean flag = true;
		
		int id = qvs.get(0).id;
		int lab = qvs.get(0).lab;
		Vertex temp = null;
		
		CopyOnWriteArrayList<Vertex> tvs = (CopyOnWriteArrayList<Vertex>)vs.clone();
				
//				vs.stream().map(list -> list.deepCopy()).collect(toCollection(CopyOnWriteArrayList::new));
		
		ArrayList<Vertex> filtered = new ArrayList<Vertex>();
		
		while(true) {
			temp = getVertex(id, qvs);
			
//			st.push(new STP(qvs.get(0), 0));
//			temp.NNT
//			int tempNPV[] = new int[5];
			for(Edge e : temp.edges)
			{
//				tempNPV[e.dstLab]++;
				int d = removeEdge(e.dst, e.src, qvs );
				if(d == 0) {
					temp.remove(e.dst);
				}
			}
			
			for(Vertex v : tvs) {
				if(temp.lab == v.lab && v.isDominate(temp.NNT))
					filtered.add(v);
			}
			
			if(filtered.size() != 0) {
				if(temp.degree != 0 ) {
					id = temp.edges.get(0).dst;
					lab = temp.edges.get(0).dstLab;
					
					tvs = getVertices(lab, vs, filtered);
					filtered.clear();
				}
				//subgraph
				else{
					return true;
				}
			}else {
				return false;
			}
//			for()
//			ArrayList<Vertex> filterd = (ArrayList<Vertex>) vs.stream().filter(l -> l.lab == temp.lab);
		}
//		return false;
		
	}

	private CopyOnWriteArrayList<Vertex> getVertices(int lab, CopyOnWriteArrayList<Vertex> vs, ArrayList<Vertex> filtered) {
		// TODO Auto-generated method stub
		CopyOnWriteArrayList<Vertex> rtv = new CopyOnWriteArrayList<Vertex>();
		for(Vertex f : filtered)
		{
			int id = f.getEdgeId(lab);
			for(Vertex v : vs) {
				if(v.id == id)
				{
					rtv.add(v);
				}
			}			
		}
		return rtv;
	}

	private Vertex getVertex(int id, CopyOnWriteArrayList<Vertex> qvs) {
		// TODO Auto-generated method stub
		for(Vertex q : qvs) {
			if(q.id == id)
				return q;
		}
		return null;
	}

	private int removeEdge(int dst, int src, CopyOnWriteArrayList<Vertex> qvs) {
		// TODO Auto-generated method stub
		for(Vertex q : qvs) {
			if(q.id == dst) {
				if(q.degree == 1)			
					return q.remove(src);
			}
		}
		return -1;
	}
	
	
	

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
