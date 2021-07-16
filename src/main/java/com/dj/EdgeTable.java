package com.dj;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class EdgeTable implements Serializable{

//	int coreId;
	int coreLab;
	int lab;
	LinkedList<ArrayList<Edge>> q;
	int batch;
	int now;
	Set<Integer> cids;
	
	int numEdge;
	
	double minDist = 0.0;
	
	public EdgeTable(int _cLab, int _lab, int _batch) {
		// TODO Auto-generated constructor stub
		coreLab 	= _cLab;
		lab 		= _lab;
		batch 		= _batch;
		now 		= -1;
		numEdge 	= 0;
		q 			= new LinkedList<ArrayList<Edge>>();
		cids 		= new HashSet<Integer>();
	}

	public void addEdge(Edge e) {
		// TODO Auto-generated method stub
		if(now != e.time) {
			now = e.time;
//			q.add
			q.addLast(new ArrayList<Edge>());
//			System.out.println("time changed");
		}
		if(q.peekLast()==null) {
			q.addLast(new ArrayList<Edge>());
		}
			
		cids.add(e.src);
		cids.add(e.dst);
		
		q.peekLast().add(e);
		numEdge++;
		
//		System.out.println(e.toString()+"\n num Edge ="+numEdge);
	}

	public Set<Integer> getCores() {
		// TODO Auto-generated method stub
		
		
		return cids;
		/*
		 * ArrayList<Integer> rtv = new ArrayList<Integer>(); for(ArrayList<Edge> es :
		 * q) { for(Edge e : es) { if(e.srcLab == coreLab && !rtv.contains(e.src))
		 * rtv.add(e.src); else if(e.dstLab == coreLab && !rtv.contains(e.dst))
		 * rtv.add(e.dst); } }
		 * 
		 * return rtv;
		 */
	}

	public boolean isContain(int cid) {
		// TODO Auto-generated method stub
		
		return cids.contains(cid);
	}

	public Set<Edge> getVertices(int cid, double w, int op) {
		// TODO Auto-generated method stub
		minDist = 0.0;
		Set<Edge> rtv = new HashSet<Edge>();
		for(ArrayList<Edge> es : q) {
			for(Edge e : es) {
				if(e.src == cid || e.dst == cid ) {
//					if(!rtv.contains(e)) 
					if(op != 0) {
						double temp = Math.abs(e.weight - w);
						if(minDist > temp ) {
							minDist = temp;
						}	
					}
					rtv.add(e);
					
				}
			}
		}
		
		return rtv;
	}

	
}
