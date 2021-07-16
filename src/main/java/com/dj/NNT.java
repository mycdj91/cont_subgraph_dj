package com.dj;

import java.io.Serializable;

public class NNT implements Serializable{
	
	int qId;
	int lab;
	int npv[];
	
	
	public NNT(int qId, int lab, int npv[]) {
		super();
		this.qId = qId;
		this.lab = lab;
		this.npv = npv;
	}


	public int getDomiInfo(int[] target) {
		// TODO Auto-generated method stub
		int count = 0;
		
		for(int i=0; i<npv.length; i++) {
			
			if(npv[i] <= target[i])
				count++;
		}
		
		return count;
	}
	
}
