package com.dj;

import java.util.Set;

public class SubgraphTable {
	
	int coreId;
	Set<Edge> vertices;
	int removeTime;
	
	double minDist = 0.0;
	
	
	public SubgraphTable(int _coreId, int _removeTime, Set<Edge> _vertices) {
		// TODO Auto-generated constructor stub
		coreId = _coreId;
		removeTime = _removeTime;
		vertices = _vertices;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "cid = " + coreId +" , removeTime = " + removeTime + ", \n Edges \n " + vertices.toString();
	}
}
