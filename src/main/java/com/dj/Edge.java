package com.dj;

import java.io.Serializable;

public class Edge implements Cloneable, Serializable{
	int src;
	int dst;
	int srcLab;
	int dstLab;
	int time;
	
	double weight;
	int op;
	
	public Edge() {
		// TODO Auto-generated constructor stub
	}
	
	public Edge(int _src, int _dst, int _srcLab, int _dstLab, int _time) {
		// TODO Auto-generated constructor stub
		src = _src;
		dst = _dst;
		srcLab = _srcLab;
		dstLab = _dstLab;
		time = _time;
	}
	
	public Edge(int _src, int _dst, int _srcLab, int _dstLab, int _time, double _weight, int _op) {
		this(_src, _dst, _srcLab, _dstLab, _time);
		weight = _weight;
		op = _op;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return src+" "+dst+" "+srcLab+" "+dstLab+" "+time + " "+weight;
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}
}
