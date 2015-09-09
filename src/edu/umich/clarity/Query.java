package edu.umich.clarity;

import java.util.LinkedList;
import java.util.Queue;

/**
 * 
 * @author hailong
 *
 */
public class Query {
	public static final int TARGET_QUERY = 0;
	private String query_name;
	private int query_type;
	private float start_time;
	private float end_time;
	private Queue<Kernel> kernelQueue;
	private Queue<Kernel> finishedKernelQueue;
	private boolean finished;
	private boolean seqconstraint;
	private float ready_time;	//the ready time of the first unexecuted kernel
	
	public Query() {
		this.start_time = 0.0f;
		this.ready_time = 0.0f;
		this.finished = false;
		this.seqconstraint = false;
		this.kernelQueue = new LinkedList<Kernel>();
		this.finishedKernelQueue = new LinkedList<Kernel>();
	}

	public String getQuery_name() {
		return query_name;
	}

	public void setQuery_name(String query_name) {
		this.query_name = query_name;
	}

	public int getQuery_type() {
		return query_type;
	}

	public void setQuery_type(int query_type) {
		this.query_type = query_type;
	}

	public float getStart_time() {
		return start_time;
	}

	public void setStart_time(float start_time) {
		this.start_time = start_time;
	}

	public float getEnd_time() {
		return end_time;
	}

	public void setEnd_time(float end_time) {
		this.end_time = end_time;
	}

	public Queue<Kernel> getKernelQueue() {
		return kernelQueue;
	}

	public void setKernelQueue(Queue<Kernel> kernelQueue) {
		this.kernelQueue = kernelQueue;
	}

	public Queue<Kernel> getFinishedKernelQueue() {
		return finishedKernelQueue;
	}

	public void setFinishedKernelQueue(Queue<Kernel> finishedKernelQueue) {
		this.finishedKernelQueue = finishedKernelQueue;
	}

	public boolean isFinished() {
		return finished;
	}

	public void setFinished(boolean finished) {
		this.finished = finished;
	}

	public boolean isSeqconstraint() {
		return seqconstraint;
	}

	public void setSeqconstraint(boolean seqconstraint) {
		this.seqconstraint = seqconstraint;
	}

	public static int getTargetQuery() {
		return TARGET_QUERY;
	}

	public float getReady_time() {
		return ready_time;
	}

	public void setReady_time(float ready_time) {
		this.ready_time = ready_time;
	}

}
