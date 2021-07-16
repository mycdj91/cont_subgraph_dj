package com.dj;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class CoreTable implements Serializable {
	int tid;
	
	int sid;
	
	
	int coreId;
	int coreLab;
	
	
	ArrayList<Double> w;
	ArrayList<Integer> op; // 0 = *, 1 = less, 2 = over, 3 = equal
	
	ArrayList<Integer> vls; //neighbor labels
	ArrayList<Integer> quries; 
	HashMap<Integer, ArrayList<Integer>> joins; //join vertices
	
	HashMap<Integer, EdgeTable> et;
	ArrayList<SubgraphTable> st;
	
	boolean ch;
	
	int numMatch;
	int batch;
	
	public CoreTable(int _tid, int _id, int _lab, int _qid) {
		// TODO Auto-generated constructor stub
		tid = _tid;
		coreId = _id;
		coreLab = _lab;
		numMatch = 0;
		sid = 0;
		
		vls = new ArrayList<Integer>();
		quries = new ArrayList<Integer>();
		joins = new HashMap<Integer, ArrayList<Integer>>();
		et = new HashMap<Integer, EdgeTable>();
		st = new ArrayList<SubgraphTable>();
		w = new ArrayList<Double>();
		op = new ArrayList<Integer>();
		
		quries.add(_qid);
		joins.put(_qid, new ArrayList<Integer>());
		
		ch = false;
	}
	
	public CoreTable(int _tid, int _id, int _lab, ArrayList<Integer> _quries, ArrayList<Integer> _vls, int _batch) {
		// TODO Auto-generated constructor stub
		tid = _tid;
		coreId = _id;
		coreLab = _lab;
		numMatch = 0;
		vls = _vls;
		quries = _quries;
		sid = 0;
		
		et = new HashMap<Integer, EdgeTable>();
		st = new ArrayList<SubgraphTable>();
		joins = new HashMap<Integer, ArrayList<Integer>>();
		for(int q : quries) {
			joins.put(q, new ArrayList<Integer>());
			joins.get(q).add(coreLab);
		}
		makeTables(_batch);
	}

	public void makeTables(int _batch) {
		// TODO Auto-generated method stub
		
		for(int lab : vls) {
			et.put(lab, new EdgeTable(coreLab, lab, _batch));
		}
		
//		st = new SubgraphTable();
		
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return 	"Id : "+tid+"\n"+coreId+", "+coreLab+", "+"neighbors = "+vls.toString()+"\n joins = "+joins.toString()+"\n quries ="+quries.toString();
	}

	public int containCheck(HashMap<Integer, Vertex> qvs, int qid) {
		// TODO Auto-generated method stu
		
//		int count = 0;
		boolean coreCheck = false;
		Set<Integer> s = qvs.keySet();
		ArrayList<Integer> contain = new ArrayList<Integer>(); 
		for(int id : s) {
			int tl = qvs.get(id).lab;
			if(tl == coreLab)
			{
				coreCheck = true;
				contain.add(id);
			}
			else if(vls.contains(tl)) {
				contain.add(id);
			}else {
				continue;
			}
		}
		//not core
		if(!coreCheck)
			return -1;
		
		if(vls.size()+1 >= qvs.size()) {
			//reuse and split
			if(contain.size()  == qvs.size()) {				
				if(vls.size()+1 == contain.size()) {
					quries.add(qid);
					for(int q : quries) {
						if(joins.get(q) != null && joins.get(q).size() > 0) {
							joins.put(qid, (ArrayList<Integer>)joins.get(q).clone());
							break;
						}
					}
					if(joins.get(qid) == null)
						joins.put(qid, new ArrayList<Integer>());
					qvs.clear();
					return 0; //reuse all
				}else {					
					return 2; //split 
				}
				// split 
			}
		}else {
			//reuse all
			if(contain.size() == vls.size()+1) {
				quries.add(qid);
				joins.put(qid, new ArrayList<Integer>());
				boolean isReuse = false;
				for( int id : contain) {
					Vertex t = qvs.get(id);
					int tl = t.lab;
					if(coreLab == tl)
					{
						int count = 0;
						for(int vl : vls) {
							for(Edge e : t.edges) {
								if(e.dstLab == vl) {
									count++;
									break;
								}
							}
						}
						if(count == vls.size()) {
							ArrayList<Integer> rmList = new ArrayList<Integer>();
							for(Edge e : t.edges) {
								if(vls.contains(e.dstLab)) {
									qvs.get(e.dst).remove(id);
									if(qvs.get(e.dst).degree >= 1) {
										joins.get(qid).add(e.dstLab);
									}else {
										qvs.remove(e.dst);
									}
									rmList.add(e.dst);
								}
							}
							
							for(int r : rmList) {
								t.remove(r);
							}
							//remove Edges
							isReuse = true;
							if(t.degree > 0)
								joins.get(qid).add(tl);
							else
								qvs.remove(id);
							break;
						}
					}
					
				}
				
				if(isReuse) {
					return 1;
				}
				/*
				 * for( int id : contain) { Vertex t = qvs.get(id); int tl = t.lab;
				 * 
				 * boolean check = false; for(int i=0; i< t.edges.size(); i++) { Edge e =
				 * t.edges.get(i); if( e.dstLab != coreLab && !(vls.contains(e.dstLab))) { check
				 * = true; }else { qvs.get(e.dst).remove(id); } } if(check) {
				 * joins.get(qid).add(tl);
				 * 
				 * }else { qvs.remove(id); }
				 * 
				 * }
				 */
				
//				return 1; // reuse all
			}
		}
		
		return -1;
		
	}

	public void remove(ArrayList<Integer> splits) {
		// TODO Auto-generated method stub
		
		for(int s : splits) {
//			vls.remove
			vls.remove(new Integer(s));
						
		}
		Set<Integer> j = joins.keySet();
		for(int key : j) {
			for(int s : splits) {
				if(joins.get(key).contains(s))
					joins.get(key).remove(new Integer(s));
			}
			if(!joins.get(key).contains(coreLab))
				joins.get(key).add(coreLab);
		}
		
	}

	public void addQuery(int qid) {
		
		quries.add(qid);
		ArrayList<Integer> j = new ArrayList<Integer>();
		j.add(coreLab);
		
		joins.put(qid, j);
	}

	public void migrate(CoreTable c) {
		// TODO Auto-generated method stub
		ArrayList<Integer> rms = new ArrayList<Integer>();
		joins = (HashMap<Integer, ArrayList<Integer>>)c.joins.clone();
		quries = (ArrayList<Integer>)c.quries.clone();
		for(int l : vls) {
			if(!c.vls.contains(l)) {
				rms.add(l);
			}
		}	
		for(int r : rms) {
			vls.remove(new Integer(r));
			et.remove(r);
		}
	}

	public void addEdge(Edge e, int dLab) {
		// TODO Auto-generated method stub
//		EdgeTable temp = et.get(dLab);
		
//		temp.now = e.time;
//		if(e.time != temp.now && temp.now != -1) {
//			
//		
//		}
		et.get(dLab).addEdge(e);
		ch = true;
	}
	
	public boolean subgraphCheck(int rmTime) {
		if(ch) {
			ch = false;
			
			for(int vl : vls) {
				if(et.get(vl).numEdge == 0) {
					return false;
				}
			}
			
	//		System.out.println("subgraph Check");
			
	
			if(vls.size() == 0 ) 
				return false;
			Set<Integer> cores = et.get(vls.get(0)).getCores();
			
			
			int stSize = st.size() ;
	//		System.out.println("subgraph Check 1"+stSize);
			for(int cid : cores) {
	//			long sn  = System.nanoTime();
				
	//			long sn2  = System.nanoTime();
				Set<Edge> vertices = et.get(vls.get(0)).getVertices(cid, w.get(0), op.get(0));
				if(vertices.size() == 0)
					break;
				int count = 1;
				double diff = et.get(vls.get(0)).minDist;
	//			long end2  = System.nanoTime();
	//			System.out.println("First = " + (end2 - sn2));
				for(int i= 1; i<vls.size(); i++) {	
	//				long sn3  = System.nanoTime();
					if(et.get(vls.get(i)).isContain(cid)) {
						
						count++;
						vertices.addAll(et.get(vls.get(i)).getVertices(cid, w.get(i), op.get(i)));
						diff += et.get(vls.get(i)).minDist;
					}else {
						break;
					}
					
	//				long end3  = System.nanoTime();
	//				System.out.println("Second = " + (end3 - sn3));
				}
				
				if(count == vls.size())
				{
					st.add(new SubgraphTable(cid, rmTime, new HashSet<Edge>()));
	//				
					st.get(st.size()-1).minDist = diff;
//					System.out.println("");
				}
				
	//			long end  = System.nanoTime();
	//			System.out.println("Total = " + (end - sn));
			}
			
			
	//		System.out.println("subgraph Check 2"+st.size());
			if(stSize < st.size())
				return true;
			else
				return false;
		}
		return false;
	}

	public int removeEdges(int now) {
		// TODO Auto-generated method stub
		
		int rtv = 0;
		for(int lab : vls) {
//				System.out.println(coreLab+","+lab+ " q size = "+et.get(lab).q.size());
				
//				System.out.println("q size = "+et.get(lab).q.peekFirst());
			EdgeTable e = et.get(lab);
			LinkedList<ArrayList<Edge>> temp = e.q;
			ArrayList<Edge> ff = temp.peekFirst();
			 if(temp.size()!= 0 && ff.get(0).time < now-batch){ 
				 
				 for(Edge fe : ff) {
					 e.cids.remove(fe.src);
					 e.cids.remove(fe.dst);
				 }
				 e.numEdge -= ff.size(); 
				 rtv += temp.removeFirst().size();
			 }
		}
		return rtv;
	}

}
