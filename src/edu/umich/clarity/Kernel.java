package edu.umich.clarity;

/**
 * 
 * @author hailong
 *
 */
public class Kernel {
	private String kernel_name;
	private int query_type;
	private float start_time;
	private float end_time;
	private float duration;
	private float occupancy;
	private boolean finished;
	private int execution_order;
	private int warps_per_batch;
	private int warps;
	private float slack_time;	//the delta time between previous kernel completes and the current kernel is issued
	private float sole_start_time;	//the start time of the kernel when the query runs alone
	private float real_duration;	//the real duration of the kernel, only valid for memcpies
	private int cuda_malloc;
	private int cuda_free;
	private int pinned;
	private int direction;			//data transfer direction if the kernel is a memcpy kernel. 0: not memcpy, 1: host to device, 2: device to host
	private long overlapped_warps;	//warps executed during kernel overlapping time
	private float nonfull_time;		//the time when the kernel cannot take all the SMs
	private int sms;				//the number of SMs occupied in the last part.
	
	public Kernel() {
		this.start_time = 0.0f;
		this.finished = false;
	}

	public String getKernel_name() {
		return kernel_name;
	}

	public void setKernel_name(String kernel_name) {
		this.kernel_name = kernel_name;
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

	public float getDuration() {
		return duration;
	}

	public void setDuration(float duration) {
		this.duration = duration;
	}

	public float getOccupancy() {
		return occupancy;
	}

	public void setOccupancy(float occupancy) {
		this.occupancy = occupancy;
	}

	public boolean isFinished() {
		return finished;
	}

	public void setFinished(boolean finished) {
		this.finished = finished;
	}

	public int getExecution_order() {
		return execution_order;
	}

	public void setExecution_order(int execution_order) {
		this.execution_order = execution_order;
	}

	public int getWarps_per_batch() {
		return warps_per_batch;
	}

	public void setWarps_per_batch(int warps_per_batch) {
		this.warps_per_batch = warps_per_batch;
	}

	public int getWarps() {
		return warps;
	}

	public void setWarps(int warps) {
		this.warps = warps;
	}

	public float getSlack_time() {
		return slack_time;
	}

	public void setSlack_time(float slack_time) {
		this.slack_time = slack_time;
	}

	public float getSole_start_time() {
		return sole_start_time;
	}

	public void setSole_start_time(float sole_start_time) {
		this.sole_start_time = sole_start_time;
	}

	public float getReal_duration() {
		return real_duration;
	}

	public void setReal_duration(float real_duration) {
		this.real_duration = real_duration;
	}

	public int getCuda_malloc() {
		return cuda_malloc;
	}

	public void setCuda_malloc(int cuda_malloc) {
		this.cuda_malloc = cuda_malloc;
	}

	public int getCuda_free() {
		return cuda_free;
	}

	public void setCuda_free(int cuda_free) {
		this.cuda_free = cuda_free;
	}

	public int getPinned() {
		return pinned;
	}

	public void setPinned(int pinned) {
		this.pinned = pinned;
	}

	public int getDirection() {
		return direction;
	}

	public void setDirection(int direction) {
		this.direction = direction;
	}

	public long getOverlapped_warps() {
		return overlapped_warps;
	}

	public void setOverlapped_warps(long overlapped_warps) {
		this.overlapped_warps = overlapped_warps;
	}

	public float getNonfull_time() {
		return nonfull_time;
	}

	public void setNonfull_time(float nonfull_time) {
		this.nonfull_time = nonfull_time;
	}

	public int getSms() {
		return sms;
	}

	public void setSms(int sms) {
		this.sms = sms;
	}
}