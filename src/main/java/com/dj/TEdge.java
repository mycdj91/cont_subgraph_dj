package com.dj;

public class TEdge extends Edge{
	
	int status; //0,1,2 => null, implicit, explicit
	int id;
	
	
	public TEdge(int _src, int _dst, int _srcLab, int _dstLab, int _time, int _status, int _id) {
		super(_src, _dst, _srcLab, _dstLab, _time);
		this.status = _status;
		this.id = _id;
	}
	
	public void setStatus(int _status) {
		this.status = _status;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return src+" "+dst+" "+srcLab+" "+dstLab+" "+time+" "+status;
	}
}
