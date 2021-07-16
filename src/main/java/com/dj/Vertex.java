package com.dj;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Vertex implements Serializable{
	int degree;
	int id;
	int lab;
	int NNT[];
	int type; //0 => normal, 1 => dominate, todo: 2 => branch
	
	//0 = normal  1=> end vertex, 2 => source vertex in TurboFlux
	
//	int NUM[];
	
	int domSize;
	
	ArrayList<Integer> sources;
	ArrayList<Integer> sourceLabs;
	CopyOnWriteArrayList<Edge> edges;
	
	HashMap<Integer, ArrayList<Edge>> dcg;
	ArrayList<Integer> status;
	
	public Vertex(int _id, int _lab) {
		// TODO Auto-generated constructor stub
		id = _id;
		lab = _lab;
		degree = 0;
		edges = new CopyOnWriteArrayList<Edge>();
		
		NNT = new int[5];
		
		sources = new ArrayList<Integer>();
		sourceLabs = new ArrayList<Integer>();
		type = 0;
		domSize = 0;
		
		
		dcg = new HashMap<Integer, ArrayList<Edge>>();
		status = new ArrayList<Integer>();
	}
	
	
	public Vertex(int _id, int _lab, int _degree,  CopyOnWriteArrayList<Edge> _edges, int []_NNT) {
		// TODO Auto-generated constructor stub
		id = _id;
		lab = _lab;
		degree = _degree;
		edges = _edges;
		NNT = _NNT;
	}
	
	public int getEdgeId(int _lab) {
		for(Edge e : edges)
		{
			if(e.dstLab == _lab)
				return e.dst;
		}
		
		return -1;		
	}
	
	
	public boolean isContains(int _lab) {
		for(Edge e : edges)
		{
			if(e.dstLab == _lab)
				return true;
		}
		
		return false;		
	}
	
	public boolean isDominate(int _npv[]) {
		
		for(int i =0;i <NNT.length; i++) {
			if(_npv[i] > NNT[i])
				return false;
		}
		return true;
	}
	
	public void addEdge(Edge e) {
		
		NNT[e.dstLab]++;
		edges.add(e);
		
		dcg.put(degree, new ArrayList<Edge>());
		status.add(0);
		
		degree++;
	}

	public int remove(int coreId) {
		// TODO Auto-generated method stub
	
		for(int i=0; i< edges.size(); i++) {
			Edge e = edges.get(i);
			if(e.dst == coreId) {
				NNT[e.dstLab]--;
				edges.remove(e);
				degree--;
				return degree;
			}
		}
		return -1;
	}
	
	public Vertex deepCopy() {
		return new Vertex(this.id, this.lab, this.degree, this.edges, this.NNT);
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "Degree = " + this.degree + ", ID = " + this.id + ", Lab = " + this.lab;
	}


//	public void sendMsg(CopyOnWriteArrayList<Edge> srcEdges) {
//		// TODO Auto-generated method stub
//		for(srcEdges)
//		
//		
//	}


	public ArrayList<Integer> getEdgeLabels() {
		// TODO Auto-generated method stub
		ArrayList<Integer> rtv = new ArrayList<Integer>();
		
		for(Edge e : edges) {
			rtv.add(e.dstLab);
		}
		return rtv;
	}


	public void addMessage(int sourceId, int sourceLab) {
		// TODO Auto-generated method stub
//		System.out.println("ss"+sources);
//		System.out.println("ss"+sourceLabs);
		sources.add(sourceId);
		sourceLabs.add(sourceLab);
	}


	public void addMessage(Vertex vertex) {
		// TODO Auto-generated method stub		
		sources.addAll(vertex.sources);
		sourceLabs.addAll(vertex.sourceLabs);
		
	}


	public int getStatus() {
		// TODO Auto-generated method stub
		int c = 0;
		for(int s : status) {
			if(s == 2)
				c++;
			if(s == 1)
				return 1; //some explicit
		}
		
		
		if(c == status.size())
			return 2; // all explicit
		else
			return 0; // all zero
	}


	public int getIdx(int tid) {
		// TODO Auto-generated method stub
		for(int i=0; i<edges.size(); i++ ) {
			if (edges.get(i).dst == tid)
				return i;
		}
		
		return -1;
	}

	public int removeTK(int t) {
		// TODO Auto-generated method stub
		
		ArrayList<Integer> temp = new ArrayList<Integer>();
		for(int i=0; i<edges.size(); i++ ) {
			
			if(edges.get(i).time < t) {
				temp.add(i);
			}
		}
		
		for(int i = temp.size()-1; i>0; i--) {
			edges.remove(temp.get(i));
		}
		degree -= temp.size();
		return temp.size();
		
	}
	

	public int removeTF(int t) {
		// TODO Auto-generated method stub
		int rtv = 0;
		for(int i=0; i<edges.size(); i++ ) {
			
			 ArrayList<Edge> temp = dcg.get(i);
			 
			 for(int j=temp.size()-1; j>0; j--) {
				 
				 if(temp.get(j).time < t) {
					 temp.remove(j);
					 rtv++;
				 }
			 }
			 if(temp.size() == 0) {
				 status.set(i, 0);
				 
			 }
		}
		return rtv;
		
		
	}


	public boolean isFull() {
		// TODO Auto-generated method stub
		for(int i=0; i<edges.size(); i++ ) {			
			 ArrayList<Edge> temp = dcg.get(i);
			 if(temp.size() == 0 )
				 return false;
		}
		return true;
	}


	public ArrayList<Integer> getImplicits() {
		// TODO Auto-generated method stub
		
		ArrayList<Integer> temp = new ArrayList<Integer>();
		
		for(int i=0; i<edges.size(); i++)
		{	
			int s = status.get(i);
			if(s != 2) {
				temp.add(edges.get(i).dst);
			}
		}
		return temp;
	}


	public void setStatus(int id) {
		// TODO Auto-generated method stub
		int i=0;
		for(Edge e : edges) {
			if(e.dst == id) {
				status.set(i, 2);
				return;
			}
			i++;
		}
	}


	public int getLatestTime() {
		// TODO Auto-generated method stub
		int l = 0;
		for(int i=0; i<edges.size(); i++)
		{	
			for(Edge e : dcg.get(i))
			{
				if(e != null) {
					if( l < e.time)
						l = e.time;
				}
			}
		}
		return l;
	}

	
}
