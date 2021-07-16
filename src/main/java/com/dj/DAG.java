package com.dj;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DAG implements Serializable{

	ArrayList<Vertex> source;
	Vertex sink;
	ConcurrentHashMap<Integer, Vertex> vs;
	int qid; 
	int boltId;
	int tempLevel;
	
	public DAG(Vertex v, Edge e, int _qid) {
		// TODO Auto-generated constructor stub
		vs = new ConcurrentHashMap<Integer, Vertex>();
		vs.put(v.id, v);
		this.sink  = v;
		this.qid = _qid;
		tempLevel = 0;
		
//		Edge temp = v.edges.get(0);
		source = new ArrayList<Vertex>();
		addEdge(e);
		
	}
	
	public boolean isContainLab(int _lab) {
		Set<Integer> keys = vs.keySet();
		boolean rtv = false;
		for(int k : keys) {
			if(vs.get(k).lab == _lab) {
				rtv = true;
				break;
			}
		}
		return rtv;
		
	}
	
	public boolean isContain(int _id){	
//		System.out.println("VSS" + vs.get(_id));
//		System.out.println("RECEIVED" + _id + ", IS "+ vs.contains(_id));
		return vs.get(_id) != null;
	}
	public void addEdge(Edge e) {
		if(!vs.contains(e.dst)) {
			Vertex target = new Vertex(e.dst, e.dstLab);
			target.addEdge(new Edge(e.dst, e.src, e.dstLab, e.srcLab, e.time));
			vs.put(target.id, target);
//			source  = target;
		}
	}
	

	public void addEdges(CopyOnWriteArrayList<Edge> edges, int level) {
		// TODO Auto-generated method stub
		System.out.println(edges.size() + "Temp Source = "+ source.toString());
		if(tempLevel != level) {
			source.clear();
			tempLevel = level;
		}
		for(Edge e : edges) {
			addEdge(e);
			source.add(vs.get(e.dst));
		}
		if(edges.size() > 1) {
			vs.get(edges.get(0).src).type = 1;
			vs.get(edges.get(0).src).domSize = edges.size();
		}
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "SINK = " + sink.toString() + "\n Vertices" + vs.toString() + ", SOURCE = " + source.toString();
	}

	public HashSet<Vertex> getNeighbors(int id) {
		// TODO Auto-generated method stub
		HashSet<Vertex> rtv = new HashSet<Vertex>();
		
		for(Edge e : vs.get(id).edges) {
			rtv.add(vs.get(e.dst));
		}
		return rtv;
	}
	
}
