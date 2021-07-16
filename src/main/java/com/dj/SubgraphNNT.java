package com.dj;

import java.io.Serializable;
import java.util.concurrent.CopyOnWriteArrayList;



public class SubgraphNNT implements Cloneable, Serializable{
	
	
	CopyOnWriteArrayList<Vertex> vs;
	int qid;
	
	
	public SubgraphNNT(CopyOnWriteArrayList<Vertex> vs, int qid) {
		super();
		this.vs = vs;
		this.qid = qid;
	}
	
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}


	public int getTime() {
		// TODO Auto-generated method stub
		if(vs.size()!=0) {
			Vertex v = vs.get(vs.size()-1);
			if(v.edges.size() !=0 )
			{
				return v.edges.get(v.edges.size()-1).time;
			}
		}
		return -1;
	}
}
