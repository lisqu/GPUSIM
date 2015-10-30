package edu.umich.clarity;

import java.util.ArrayList;

public class MemCpy implements Comparable<MemCpy> {
	private Kernel k;
	private float progress;		//progress of the memcpy (how much data has been transfered)
	private int parallel;		//Number of parallel memcpies in the stage
	private float update_time;
	private float used_time;
	private float expected_left_time;	//the left transfer time if no more memcpies are added
	
	public MemCpy(Kernel ker) {
		this.k = ker;
		this.update_time = this.k.getStart_time();
		this.progress = 0.0f;
		this.setParallel(1);
		this.used_time = 0.0f;
	}
	
	@Override
	public int compareTo(MemCpy o) {
		// TODO Auto-generated method stub
		return Float.compare(k.getEnd_time(), o.k.getEnd_time());
	}

	public String toString() {
			return k.getStart_time() + "-->" + k.getEnd_time();
	}
	  
	public Kernel getK() {
		return k;
	}

	public void setK(Kernel k) {
		this.k = k;
	}

	public float getProgress() {
		return progress;
	}

	public void setProgress(float progress) {
		this.progress = progress;
	}

	public float getUpdate_time() {
		return update_time;
	}

	public void setUpdate_time(float update_time) {
		this.update_time = update_time;
	}

	public int getParallel() {
		return parallel;
	}

	public void setParallel(int parallel) {
		this.parallel = parallel;
	}

	public float getUsed_time() {
		return used_time;
	}

	public void setUsed_time(float used_time) {
		this.used_time = used_time;
	}

	public float getExpected_left_time() {
		return expected_left_time;
	}

	public void setExpected_left_time(float expected_left_time) {
		this.expected_left_time = expected_left_time;
	}
}
