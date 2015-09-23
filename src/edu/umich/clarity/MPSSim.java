package edu.umich.clarity;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;

import com.opencsv.CSVReader;

/**
 * A simulator to study the MPS scheduling policy. There are two types of
 * queries exist for the simulation, 1) target query, require QoS satisfaction;
 * 2) background query, improve system utilization. All the queries are
 * submitted in a closed loop, however it is easy to extend to support open
 * loop.
 * 
 * @author hailong
 *
 */

public class MPSSim {
	// the total compute slots
	public static final int COMPUTE_SLOTS = 15;
	// there is a time slack before next kernel can be issued
	// TODO not used for now
	public static final float KERNEL_SLACK = 0.0f;
	// the location of kernel profiles
	public static final String PROFILE_PATH = "input/formated/";
	// the location of simulation configuration
	public static final String CONFIG_PATH = "input/";
	public static final String RESULT_PATH = "simulated/";

	public static final String LOAD_PATH="input/load/";
//	public static final String BG_LOAD="input/load/bg_load.csv";
	
//	public static int[] bg_start_points = {0,1226,2311,3327,4289,5359,6349,7122};
	public static float target_start_point;
	
	public static ArrayList<Float> bg_start_points = new ArrayList<Float>();
	
	public static ArrayList<LinkedList<Query>> targetQueries;
	public static ArrayList<LinkedList<Query>> backgroundQueries;
	public static ArrayList<Float> slacks = new ArrayList<Float>();	//slacks between two queries in a client
		
	private Random randQuery = new Random();	//random delay between queries
	private static ArrayList<LinkedList<Query>> finishedQueries;	//finished query lists
	
	private static ArrayList<Map.Entry<Float, Float>> utilization;

	private static ArrayList<Float> microDelays = new ArrayList<Float>();	//Slow down due to co-location
	private static ArrayList<Integer> startVariations = new ArrayList<Integer>();	//Start time variation of different apps
	
	private static ArrayList<Integer> target_load = new ArrayList<Integer>();
	private static ArrayList<Integer> bg_load=new ArrayList<Integer>();
	
	private static ArrayList<Integer> query_id = new ArrayList<Integer>();
	private ArrayList<Query> issuingQueries;
	private ArrayList<Integer> issueIndicator;
	private Queue<Kernel> kernelQueue;
	private ArrayList<Kernel> memCpies = new ArrayList<Kernel>();
	
	public static float COMPLETE_TIME;	//The complete time of target queries
	
	public static int n_bg;
	public static int n_tg;
	public static String bg_name;
	private final static boolean Detail = false;
	private final static boolean MPS_enabled = true;
	
	private boolean pcie_transfer = false;
	/*
	 * kernel scheduling policy
	 */
	public static String schedulingType;

	public static final String FIFO_SCHEDULE = "fifo";
	public static final String PRIORITY_FIRST_SCHEDULE = "priority";
	public static final String FAIRNESS_FIRST_SCHEDULE = "fairness";
	public static final String OCCUPANCY_FIRST_SCHEDULE = "occupancy";
	public static final String SHORTEST_FIRST_SCHEDULE = "shortest";
	private int available_slots = MPSSim.COMPUTE_SLOTS;

	/**
	 * add each type of query into the issuing list at the initial stage.
	 */
	public MPSSim() {
		finishedQueries = new ArrayList<LinkedList<Query>>();
		this.issueIndicator = new ArrayList<Integer>();
		this.kernelQueue = new PriorityQueue<Kernel>(100,
				new KernelComparator<Kernel>());
		this.issuingQueries = new ArrayList<Query>();
		targetQueries = new ArrayList<LinkedList<Query>>();
		backgroundQueries = new ArrayList<LinkedList<Query>>();
		utilization = new ArrayList<Map.Entry<Float, Float>>();
	}

	/**
	 * initialize the simulator
	 */
	private void init() {
		for (LinkedList<Query> queries : targetQueries) {
			issuingQueries.add(queries.poll());

			issuingQueries.get(issuingQueries.size()-1).setReady_time(target_start_point);
			issuingQueries.get(issuingQueries.size()-1).setStart_time(target_start_point);		
//			i = i+1.0f;
		}
		
		int i = 0;
		for (LinkedList<Query> queries : backgroundQueries) {
			issuingQueries.add(queries.poll());
//			issuingQueries.get(issuingQueries.size()-1).setReady_time(start_time);
//			issuingQueries.get(issuingQueries.size()-1).setStart_time(start_time);
			issuingQueries.get(issuingQueries.size()-1).setReady_time(bg_start_points.get(i));
			issuingQueries.get(issuingQueries.size()-1).setStart_time(bg_start_points.get(i));
			i++;

		}
		for (int m = 0; m < (issuingQueries.size()); m++) {
			issueIndicator.add(1);
		}
//		enqueueKernel(0.0f);
		enqueueKernel_quan(0.0f,0.0f,0);
	}

	/**
	 * Choose the query with FIFO order, default MPS scheduling policy
	 * 
	 * @param select_range
	 *            the target queries to select from
	 * @return the index of the target query
	 */

	private int FIFOSchedule(ArrayList<Integer> select_range) {
		int chosen_query = 0;

		Random random = new Random();
		int chosen_index = random.nextInt(select_range.size());
		chosen_query = select_range.get(chosen_index);
		
		float earliest=100000000.0f;
		for(Integer id : select_range) {
			if(issuingQueries.get(id).getReady_time() < earliest) {
				chosen_query = id;
				earliest = issuingQueries.get(id).getReady_time();
			}
//			System.out.println(issuingQueries.get(id).getReady_time());
		}
//		issuingQueries.get(chosen_query).setReady_time(10000000.0f);
//		System.out.println("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
		return chosen_query;
	}

/*	
	private int FIFOSchedule(ArrayList<Integer> select_range) {
		int chosen_query;

		Random random = new Random();
		int chosen_index = random.nextInt(select_range.size());
		chosen_query = select_range.get(chosen_index);
		return chosen_query;
	}
*/
	/**
	 * Always choose the target query over the background query. When multiple
	 * target queries available, use FIFO
	 * 
	 * @param select_range
	 *            the target queries to select from
	 * @return the index of the target query
	 */
	private int PriorityFirstSchedule(ArrayList<Integer> select_range) {
		int chosen_query = 0;
		ArrayList<Integer> first_priority = new ArrayList<Integer>();
		for (int i = 0; i < targetQueries.size(); i++) {
			if (select_range.contains(i)) {
				first_priority.add(i);
			}
		}
		/*
		 * no priority query exists, use MPS default FIFO
		 */
		if (first_priority.size() == 0) {
			chosen_query = FIFOSchedule(select_range);
		} else {// multiple priority queries exist, pick one with FIFO
			chosen_query = FIFOSchedule(first_priority);
		}
		return chosen_query;
	}

	/**
	 * Whenever there are multiple target queries to choose, make sure each type
	 * of query use the compute resource equally
	 * 
	 * TODO not sure to support this
	 * 
	 * @param select_range
	 *            the target queries to select from
	 * @return the index of the target query
	 */
	private int FairnessFirstSchedule(ArrayList<Integer> select_range) {
		int chosen_query = 0;
		return chosen_query;
	}

	/**
	 * Always choose the kernel with largest occupancy to schedule
	 * 
	 * @param select_range
	 *            the target queries to select from
	 * @return the index of the target query
	 */
	private int OccupancyFirstSchedule(ArrayList<Integer> select_range) {
		int chosen_query = 0;
		int max_occupancy = 0;
		for (Integer index : select_range) {
			int current_occupancy = issuingQueries.get(index).getKernelQueue()
					.peek().getOccupancy();
			if (current_occupancy >= max_occupancy) {
				max_occupancy = current_occupancy;
				chosen_query = index;
			}
		}
		return chosen_query;
	}

	/**
	 * Always choose the kernel with least execution time to schedule
	 * 
	 * @param select_range
	 *            the target queries to select from
	 * @return the index of the target query
	 */
	private int ShortestFirstSchedule(ArrayList<Integer> select_range) {
		int chosen_query = 0;
		float min_duration = Float.POSITIVE_INFINITY;
		for (Integer index : select_range) {
			float current_duration = issuingQueries.get(index).getKernelQueue()
					.peek().getDuration();
			if (current_duration <= min_duration) {
				min_duration = current_duration;
				chosen_query = index;
			}
		}
		return chosen_query;
	}

	/**
	 * Select the right kernel to be issued to the kernel queue. A couple of
	 * constraints need to be met in order to mimic the real experiment setup.
	 * 1.kernel within the same type of query should be issued sequentially 2.
	 * kernels from different types of queries can be executed concurrently as
	 * long as their accumulated occupancy not exceeds the resource threshold.
	 * 3. unless the running queries have been finished, the same type of
	 * queries cannot be issued
	 * 
	 * @param elapse_time
	 *            the elapsed time since simulation starts
	 */
	private void enqueueKernel(float elapse_time) {
		/*
		 * 1. if the issue list is empty, then all the queries at least have
		 * been issued for processing
		 */
		int issueSize = 0;
		for (int indicator : issueIndicator) {
			issueSize += indicator;
		}
		if (issueSize != 0) {
			/*
			 * 2. make sure the query selection range within the current issue
			 * list
			 */
			ArrayList<Integer> select_range = new ArrayList<Integer>();
			for (int i = 0; i < issuingQueries.size(); i++) {
				select_range.add(i);
			}
			for (int i = 0; i < issueIndicator.size(); i++) {
				if (issueIndicator.get(i) == 0) {
					select_range.remove(select_range.indexOf(i));
				}
			}
			/*
			 * 3. pick the kernels satisfying the sequential constraints as well
			 * as fitting the computing slots
			 */
			while (available_slots >= 0 && select_range.size() != 0) {
				// System.out.println("select_range size " +
				// select_range.size());
				/*
				 * 4. random select one query candidate to mimic FIFO within MPS
				 */
				int chosen_query = 0;
				if (MPSSim.schedulingType
						.equalsIgnoreCase(MPSSim.FIFO_SCHEDULE)) {
					chosen_query = FIFOSchedule(select_range);
				} else if (MPSSim.schedulingType
						.equalsIgnoreCase(MPSSim.PRIORITY_FIRST_SCHEDULE)) {
					chosen_query = PriorityFirstSchedule(select_range);
				} else if (MPSSim.schedulingType
						.equalsIgnoreCase(MPSSim.OCCUPANCY_FIRST_SCHEDULE)) {
					chosen_query = OccupancyFirstSchedule(select_range);
				} else if (MPSSim.schedulingType
						.equalsIgnoreCase(MPSSim.FAIRNESS_FIRST_SCHEDULE)) {
					chosen_query = FairnessFirstSchedule(select_range);
				} else if (MPSSim.schedulingType
						.equalsIgnoreCase(SHORTEST_FIRST_SCHEDULE)) {
					chosen_query = ShortestFirstSchedule(select_range);
				}
				/*
				 * 4.1 priority first, always choose the kernel from the target
				 * query
				 */
				// int chosen_query;
				// int chosen_index;
				// if (select_range.get(0) == 0) {
				// chosen_index = 0;
				// chosen_query = 0;
				// } else {
				// Random random = new Random();
				// chosen_index = random.nextInt(select_range.size());
				// chosen_query = select_range.get(chosen_index);
				// }
				/*
				 * 5. test whether the candidate meet the requirements
				 */
				Kernel kernel = issuingQueries.get(chosen_query)
						.getKernelQueue().peek();
				boolean isConstrained = issuingQueries.get(chosen_query)
						.isSeqconstraint();
				if (!isConstrained
						&& available_slots - kernel.getOccupancy() >= 0) {
					if ((kernel.getOccupancy() != 0)
							|| (kernel.getOccupancy() == 0 && !pcie_transfer)) {
						/*
						 * 6. if all good, get the kernel from selected query
						 * type
						 */
						kernel = issuingQueries.get(chosen_query)
								.getKernelQueue().poll();
						/*
						 * 7. set the kernel's start time and end time
						 */
						issuingQueries.get(chosen_query).setSeqconstraint(true);
						kernel.setStart_time(elapse_time);
						kernel.setEnd_time(kernel.getDuration() + elapse_time
								+ MPSSim.KERNEL_SLACK);
						kernelQueue.offer(kernel);
						if (Detail) {	
							System.out.println("MPS enqueues kernel "
								+ kernel.getExecution_order() + " from query "
								+ kernel.getQuery_type()
								+ " with kernel occupacy "
								+ kernel.getOccupancy()
								+ " and estimated finish time "
								+ kernel.getEnd_time());
						}
						/*
						 * 8. assign the compute slot for the kernel
						 */
						available_slots -= kernel.getOccupancy();
						if (kernel.getOccupancy() == 0) {
							pcie_transfer = true;
						}
					}
				}
				/*
				 * 9. this type of query should not be considered during next
				 * round
				 */
				select_range.remove(select_range.indexOf(chosen_query));
			}
		} else {
			System.out
					.println("At time "
							+ elapse_time
							+ "(ms): all queries have been submitted, wait for the kernel queue to become empty");
		}
	}

	/**
	 * 
	 */
	public void mps_simulate() {
		/*
		 * initial the simulator to time zero
		 */
		init();
		Kernel kernel = null;
		/*
		 * process the queue while it is not empty
		 */
		while (!kernelQueue.isEmpty()) {
			/*
			 * 1. fetch the queue from queue and execute it (hypothetically)
			 */
			kernel = kernelQueue.poll();
			/*
			 * 2. mark the kernel as finished and relinquish the pcie bus
			 */
			kernel.setFinished(true);
			if (kernel.getOccupancy() == 0) {
				pcie_transfer = false;
			}
			/*
			 * 3. mark the query as not sequential constrained to issue kernel
			 */
			issuingQueries.get(kernel.getQuery_type()).setSeqconstraint(false);
			/*
			 * 4. relinquish the computing slots back to the pool
			 */
			float util_percent = (MPSSim.COMPUTE_SLOTS - available_slots)
					/ (MPSSim.COMPUTE_SLOTS * 1.0f);
			utilization.add(new SimpleEntry<Float, Float>(kernel.getEnd_time(),
					util_percent));
			available_slots += kernel.getOccupancy();
			/*
			 * 5. add the finished kernel back to the query's finished kernel
			 * queue
			 */
			issuingQueries.get(kernel.getQuery_type()).getFinishedKernelQueue()
					.offer(kernel);
			/*
			 * 6. if all the kernels from the query have been finished
			 */
			if (issuingQueries.get(kernel.getQuery_type()).getKernelQueue()
					.isEmpty()) {
				/*
				 * 7. set the finish time (global time) of the query
				 */
				issuingQueries.get(kernel.getQuery_type()).setEnd_time(
						kernel.getEnd_time());
				Query query = issuingQueries.get(kernel.getQuery_type());
				// remove the finished query from the issue list
				/*
				 * 8. if the target query, save the finished query to a list
				 */
				if (query.getQuery_type() < targetQueries.size()) {
					finishedQueries.get(query.getQuery_type()).add(query);
				}
				/*
				 * 9. instead of removing the finished query from the issue list
				 * mark the indicator of the corresponding issuing slot as
				 * invalid
				 */
				// issuingQueries.remove(kernel.getQuery_type());
				issueIndicator.set(kernel.getQuery_type(), 0);
				/*
				 * 10. add the same type of query to the issue list unless the
				 * query queue is empty for that type of query
				 */
				if (kernel.getQuery_type() < targetQueries.size()) {
					if (!targetQueries.get(kernel.getQuery_type()).isEmpty()) {
						Query comingQuery = targetQueries.get(
								kernel.getQuery_type()).poll();
						comingQuery.setStart_time(kernel.getEnd_time());
						issuingQueries.set(kernel.getQuery_type(), comingQuery);
						issueIndicator.set(kernel.getQuery_type(), 1);
					}
				} else {
					if (!backgroundQueries.get(
							kernel.getQuery_type() - targetQueries.size())
							.isEmpty()) {
						Query comingQuery = backgroundQueries.get(
								kernel.getQuery_type() - targetQueries.size())
								.poll();
						comingQuery.setStart_time(kernel.getEnd_time());
						issuingQueries.set(kernel.getQuery_type(), comingQuery);
						issueIndicator.set(kernel.getQuery_type(), 1);
					}
				}
			}
			/*
			 * 11. select the kernel to be issued to the queue
			 */
			if (Detail) {
				System.out.println("At time " + kernel.getEnd_time() + "(ms)"
					+ " finished executing kernel "
					+ kernel.getExecution_order() + " from query "
					+ kernel.getQuery_type());
				System.out.println("Enqueue new kernels from the issuing list...");
			}
			enqueueKernel(kernel.getEnd_time());
		}
		System.out
				.println("At time "
						+ kernel.getEnd_time()
						+ "(ms): All kernel have been executed, the simulation will stop");
	}

	/**
	 * Select the right kernel to be issued to the kernel queue. A couple of
	 * constraints need to be met in order to mimic the real experiment setup.
	 * 1.kernel within the same type of query should be issued sequentially 2.
	 * kernels from different types of queries can be executed concurrently as
	 * long as their accumulated occupancy not exceeds the resource threshold.
	 * 3. unless the running queries have been finished, the same type of
	 * queries cannot be issued
	 * 
	 * @param elapse_time
	 *            the elapsed time since simulation starts
	 */
	private void enqueueKernel_quan(float elapse_time, float overlapping_time, int left_sm) {
		/*
		 * 1. if the issue list is empty, then all the queries at least have
		 * been issued for processing
		 */
		int issueSize = 0;
		for (int indicator : issueIndicator) {
			issueSize += indicator;
		}
		if (issueSize != 0) {
			/*
			 * 2. make sure the query selection range within the current issue
			 * list
			 */
			ArrayList<Integer> select_range = new ArrayList<Integer>();
			for (int i = 0; i < issuingQueries.size(); i++) {
				select_range.add(i);
			}
			for (int i = 0; i < issueIndicator.size(); i++) {
				if (issueIndicator.get(i) == 0) {
					select_range.remove(select_range.indexOf(i));
				}
			}
			/*
			 * 3. pick the kernels satisfying the sequential constraints as well
			 * as fitting the computing slots
			 */
//			while (available_slots >= 0 && select_range.size() != 0) {
				// System.out.println("select_range size " +
				// select_range.size());
				/*
				 * 4. random select one query candidate to mimic FIFO within MPS
				 */
				int chosen_query = 0;
				if (MPSSim.schedulingType
						.equalsIgnoreCase(MPSSim.FIFO_SCHEDULE)) {
					chosen_query = FIFOSchedule(select_range);
				} else if (MPSSim.schedulingType
						.equalsIgnoreCase(MPSSim.PRIORITY_FIRST_SCHEDULE)) {
					chosen_query = PriorityFirstSchedule(select_range);
				} else if (MPSSim.schedulingType
						.equalsIgnoreCase(MPSSim.OCCUPANCY_FIRST_SCHEDULE)) {
					chosen_query = OccupancyFirstSchedule(select_range);
				} else if (MPSSim.schedulingType
						.equalsIgnoreCase(MPSSim.FAIRNESS_FIRST_SCHEDULE)) {
					chosen_query = FairnessFirstSchedule(select_range);
				} else if (MPSSim.schedulingType
						.equalsIgnoreCase(SHORTEST_FIRST_SCHEDULE)) {
					chosen_query = ShortestFirstSchedule(select_range);
				}

				/*
				 * 5. test whether the candidate meet the requirements
				 */
				Kernel kernel = issuingQueries.get(chosen_query)
						.getKernelQueue().peek();
				boolean isConstrained = issuingQueries.get(chosen_query)
						.isSeqconstraint();
//				if (!isConstrained
//						&& available_slots - kernel.getOccupancy() >= 0) {
				if (!isConstrained) {
					if ((kernel.getOccupancy() != 0)
							|| (kernel.getOccupancy() == 0 && !pcie_transfer)) {
						/*
						 * 6. if all good, get the kernel from selected query
						 * type
						 */
						kernel = issuingQueries.get(chosen_query)
								.getKernelQueue().poll();
												/*
						 * 7. set the kernel's start time and end time
						 */
						issuingQueries.get(chosen_query).setSeqconstraint(true);
						float st = Math.max(elapse_time-overlapping_time, issuingQueries.get(kernel.getQuery_type()).getReady_time());
						kernel.setStart_time(st);
						

						if(kernel.getOccupancy() == 0) {
							kernel.setReal_duration(kernel.getDuration());  	//Initialize the duration
							memCpies.add(kernel);								//add to the memcpy queue
							kernel.setReal_duration(calMemcpyDuration(kernel, st));		//update the duration of the kernel
//							System.out.println(kernel.getExecution_order()+" : "+kernel.getStart_time()+", duration:"+kernel.getDuration()+", client: "+kernel.getQuery_type());
						} 
						
/*****Todo: manage cudaMalloc contention*******/
//						if(kernel.getKernel_name()=="")

//						kernel.setDuration(kernel.getDuration()*microDelays.get(kernel.getQuery_type()));
//						kernel.setReal_duration(kernel.getDuration()*microDelays.get(kernel.getQuery_type()));
						if(MPS_enabled) {
							int overlapped=0;
							int batches=1;
							int org_batches=1;
							
							if(kernel.getOccupancy()!=0) {
								overlapped = kernel.getWarps_per_batch()/15 * left_sm;
								org_batches = (int) Math.ceil(kernel.getWarps()/(float)(kernel.getWarps_per_batch()));
						
//								batches = (int) Math.ceil( (kernel.getWarps()-overlapped)/(float)(kernel.getWarps_per_batch()));
								batches = 1 + (int) Math.ceil( (kernel.getWarps()-overlapped)/(float)(kernel.getWarps_per_batch()));						
//								batches = (int) Math.ceil(kernel.getWarps()/(float)(kernel.getWarps_per_batch()));
								kernel.setReal_duration(kernel.getDuration()*batches/org_batches);
							}
					
							else kernel.setReal_duration(kernel.getDuration());						
						}
						
//						if(kernel.getQuery_type() >=1)
							kernel.setReal_duration(kernel.getReal_duration()*1.05f);
						
//						if(kernel.getQuery_type() == 0)
//							System.out.println(kernel.getExecution_order()+" : "+kernel.getDuration()+"->"+kernel.getReal_duration());

//						kernel.setStart_time(elapse_time-overlapping_time);
//						System.out.println("ready: "+issuingQueries.get(kernel.getQuery_type()).getStart_time()+", start: "+ (elapse_time-overlapping_time) );

						kernel.setEnd_time(kernel.getReal_duration() + st + MPSSim.KERNEL_SLACK);
						
//						kernel.setEnd_time(kernel.getDuration() + elapse_time + MPSSim.KERNEL_SLACK);
						kernelQueue.offer(kernel);
//						if(kernel.getQuery_type() >= targetQueries.size() && elapse_time <1000)
//							System.out.println("Duration: "+kernel.getDuration()+", start: "+kernel.getStart_time()+", end: "+kernel.getEnd_time());

						if (Detail) {	
							System.out.println("MPS enqueues kernel "
								+ kernel.getExecution_order() + " from query "
								+ kernel.getQuery_type()
								+ " with kernel occupacy "
								+ kernel.getOccupancy()
								+ " and estimated finish time "
								+ kernel.getEnd_time());
						}
						/*
						 * 8. assign the compute slot for the kernel
						 */
						available_slots -= kernel.getOccupancy();
						
//						if (kernel.getOccupancy() == 0) {
//							pcie_transfer = true;
//						}
					}
				}
				/*
				 * 9. this type of query should not be considered during next
				 * round
				 */
				select_range.remove(select_range.indexOf(chosen_query));
//			}
		} else {
			System.out
					.println("At time "
							+ elapse_time
							+ "(ms): all queries have been submitted, wait for the kernel queue to become empty");
		}
	}

	public float calMemcpyDuration(Kernel kernel, float current_time) {
		float ret = 0.0f;
		
		LinkedList<Integer> active_memcpies = new LinkedList<Integer>();
		
		for(Kernel k : memCpies) {
			if(k.getStart_time()+k.getReal_duration() >= current_time + kernel.getDuration()) {
				active_memcpies.add(new Integer(memCpies.indexOf(k)));			
				ret += k.getDuration()-(current_time - k.getStart_time());
			}
		}
		
		ret = kernel.getDuration();
		
		if(active_memcpies.size() <= 3) {
			ret =  kernel.getDuration() * 1.0f;
		} else {
			float slow_down = (float)(active_memcpies.size()) / 3.0f;
			for(int i=0;i<active_memcpies.size();i++) {
				ret = memCpies.get(active_memcpies.get(i)).getDuration() * slow_down;	//calculate new duration
				float delta_time = ret - memCpies.get(active_memcpies.get(i)).getReal_duration();
//				System.out.println("delta_time is: "+delta_time);
				
				memCpies.get(active_memcpies.get(i)).setReal_duration(ret);		//update new duration
				
				issuingQueries.get(memCpies.get(active_memcpies.get(i)).getQuery_type()).setReady_time(
						issuingQueries.get(memCpies.get(active_memcpies.get(i)).getQuery_type()).getReady_time() + delta_time);
			}
//			System.out.println(ret/3 + kernel.getDuration());
//			ret = ret/3.0f + kernel.getDuration();
//			ret = kernel.getDuration()* (n_tg + n_bg)/3.86f;
		}
		
		for (int i=0;i<memCpies.size();i++) {
			if(memCpies.get(i).getStart_time() + memCpies.get(i).getReal_duration() < current_time) {
				memCpies.remove(i);
			}
		}
/*		
		if(kernel.getQuery_type() < 1)
			System.out.println("client: "+kernel.getQuery_type()+", duration updates from: "+kernel.getDuration()+" to "+ret+", active is: "+active_memcpies+", current time: "+current_time
					+", size: "+memCpies.size()+", ret is: "+ret+", start: "+kernel.getStart_time());
*/		
		return ret;
	}
	
	/**
	 * 
	 */
	public void mps_simulate_quan() {
		/*
		 * initial the simulator to time zero
		 */
		init();
		Kernel kernel = null;
		/*
		 * process the queue while it is not empty
		 */
		while (!kernelQueue.isEmpty()) {
			/*
			 * 1. fetch the queue from queue and execute it (hypothetically)
			 */
			kernel = kernelQueue.poll();
			/*
			 * 2. mark the kernel as finished and relinquish the pcie bus
			 */
			kernel.setFinished(true);
			if (kernel.getOccupancy() == 0) {
				pcie_transfer = false;
			}
/*			
			if(kernel.getQuery_type() == 0)
				System.out.println(kernel.getExecution_order()+" : start: "+kernel.getStart_time()+"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~, duration: "+kernel.getReal_duration());
			else 
				System.out.println(kernel.getExecution_order()+" : start: "+kernel.getStart_time()+", duration:"+kernel.getReal_duration()+", client: "+kernel.getQuery_type());
*/
//			System.out.println(kernel.getQuery_type()+" : "+kernel.getExecution_order());
			/*
			 * 3. mark the query as not sequential constrained to issue kernel
			 */
			issuingQueries.get(kernel.getQuery_type()).setSeqconstraint(false);
			/*
			 * 4. relinquish the computing slots back to the pool
			 */
//			float util_percent = (MPSSim.COMPUTE_SLOTS - available_slots)
//					/ (MPSSim.COMPUTE_SLOTS * 1.0f);
//			utilization.add(new SimpleEntry<Float, Float>(kernel.getEnd_time(),
//					util_percent));
			available_slots += kernel.getOccupancy();
			
			//set the ready time of the next kernel in issuing Queries.
			issuingQueries.get(kernel.getQuery_type()).setReady_time(kernel.getEnd_time()+kernel.getSlack_time());
			
			/*
			 * 5. add the finished kernel back to the query's finished kernel
			 * queue
			 */
			issuingQueries.get(kernel.getQuery_type()).getFinishedKernelQueue()
					.offer(kernel);
			/*
			 * 6. if all the kernels from the query have been finished
			 */
			if (issuingQueries.get(kernel.getQuery_type()).getKernelQueue()
					.isEmpty()) {
				/*
				 * 7. set the finish time (global time) of the query
				 */
				issuingQueries.get(kernel.getQuery_type()).setEnd_time(kernel.getEnd_time());
				Query query = issuingQueries.get(kernel.getQuery_type());
				// remove the finished query from the issue list
				/*
				 * 8. if the target query, save the finished query to a list
				 */
//				if (query.getQuery_type() < targetQueries.size()) {
					finishedQueries.get(query.getQuery_type()).add(query);
//				}
				/*
				 * 9. instead of removing the finished query from the issue list
				 * mark the indicator of the corresponding issuing slot as
				 * invalid
				 */
				// issuingQueries.remove(kernel.getQuery_type());
				issueIndicator.set(kernel.getQuery_type(), 0);
				/*
				 * 10. add the same type of query to the issue list unless the
				 * query queue is empty for that type of query
				 */
				float duration = kernel.getEnd_time() - issuingQueries.get(kernel.getQuery_type()).getStart_time();
//				System.out.println("Duration is: "+ duration+", end is: "+kernel.getEnd_time()+", start is: "+issuingQueries.get(kernel.getQuery_type()).getStart_time());
				
				if (kernel.getQuery_type() < targetQueries.size()) {
					if (!targetQueries.get(kernel.getQuery_type()).isEmpty()) {
						Query comingQuery = targetQueries.get(
								kernel.getQuery_type()).poll();
						comingQuery.setStart_time(kernel.getEnd_time());
						issuingQueries.set(kernel.getQuery_type(), comingQuery);
						issueIndicator.set(kernel.getQuery_type(), 1);
						
						comingQuery.setQuery_id(query_id.get(kernel.getQuery_type()));
						query_id.set(kernel.getQuery_type(), comingQuery.getQuery_id()+1);
//						issuingQueries.get(kernel.getQuery_type()).setReady_time(kernel.getEnd_time()+slacks.get(kernel.getQuery_type()));
//						issuingQueries.get(kernel.getQuery_type()).setStart_time(kernel.getEnd_time()+slacks.get(kernel.getQuery_type()));
						issuingQueries.get(kernel.getQuery_type()).setReady_time(kernel.getEnd_time()+target_load.get(comingQuery.getQuery_id())-duration);
						issuingQueries.get(kernel.getQuery_type()).setStart_time(kernel.getEnd_time()+target_load.get(comingQuery.getQuery_id())-duration);
					//	System.out.println("end time: "+kernel.getEnd_time()+", duration: "+duration+", start: "+ issuingQueries.get(kernel.getQuery_type()).getStart_time()+", load: "+target_load.get(comingQuery.getQuery_id()));
					} else {
						COMPLETE_TIME = kernel.getEnd_time();
						System.out.println("target queries stop at: "+COMPLETE_TIME);
					}
				} else {
					if (!backgroundQueries.get(
							kernel.getQuery_type() - targetQueries.size())
							.isEmpty()) {						
						Query comingQuery = backgroundQueries.get(
								kernel.getQuery_type() - targetQueries.size())
								.poll();
//						comingQuery.setStart_time(kernel.getEnd_time());
						
						issuingQueries.set(kernel.getQuery_type(), comingQuery);
						issueIndicator.set(kernel.getQuery_type(), 1);
						
						comingQuery.setQuery_id(query_id.get(kernel.getQuery_type()));
						query_id.set(kernel.getQuery_type(), comingQuery.getQuery_id()+1);
						
//						issuingQueries.get(kernel.getQuery_type()).setReady_time(kernel.getEnd_time()+slacks.get(kernel.getQuery_type()));
//						issuingQueries.get(kernel.getQuery_type()).setStart_time(kernel.getEnd_time()+slacks.get(kernel.getQuery_type()));
						issuingQueries.get(kernel.getQuery_type()).setReady_time(kernel.getEnd_time()+bg_load.get(comingQuery.getQuery_id())-duration+slacks.get(kernel.getQuery_type()));
						issuingQueries.get(kernel.getQuery_type()).setStart_time(kernel.getEnd_time()+bg_load.get(comingQuery.getQuery_id())-duration+slacks.get(kernel.getQuery_type()));
					//	System.out.println("**********end time: "+kernel.getEnd_time()+", duration: "+duration+", start: "+ issuingQueries.get(kernel.getQuery_type()).getStart_time()+", load: "+bg_load.get(comingQuery.getQuery_id()));
					}
				}
//				if (kernel.getQuery_type() >= targetQueries.size())
//					System.out.println("background kernel: "+kernel.getDuration());
			}
			/*
			 * 11. select the kernel to be issued to the queue
			 */
			if (Detail) {
				System.out.println("At time " + kernel.getEnd_time() + " (ms)"
					+ " finished executing kernel "
					+ kernel.getExecution_order() + " from query "
					+ kernel.getQuery_type());
				System.out.println("Enqueue new kernels from the issuing list...");
			}
			
			float overlapping_time=0.0f;
			int left_sm = 0;
			
			float start_time;
			if(kernel.getOccupancy()==0) {
				start_time = kernel.getStart_time();
				overlapping_time = 0.0f;
			}
			else { 
				start_time = kernel.getEnd_time();
				int batches = (int) Math.ceil(kernel.getWarps()/(float)(kernel.getWarps_per_batch()));
				if(batches !=0 ) overlapping_time = kernel.getDuration()/batches;
				left_sm = 15 - (int) Math.ceil( (kernel.getWarps()-(batches-1)*kernel.getWarps_per_batch())/(kernel.getWarps_per_batch()/15) );
//				System.out.println("left over is: "+left_sm);
//				if(kernel.getQuery_type()==0)
//					System.out.println("kernel duration: "+kernel.getDuration()+", kernel batches: "+batches+", the expected overlapping time is: "+overlapping_time+", kernel is: "+kernel.getQuery_type()+", kernel id: "+kernel.getExecution_order());
			}
			
			if(!MPS_enabled) {
				overlapping_time = 0.0f;
				left_sm = 0;
			}

			enqueueKernel_quan(start_time, overlapping_time, left_sm);			
//			enqueueKernel_quan(kernel.getEnd_time(), overlapping_time);
		}
		System.out
				.println("At time "
						+ kernel.getEnd_time()
						+ "(ms): All kernel have been executed, the simulation will stop");
	}
	
	/**
	 * Parse the kernel profile
	 * 
	 * @param filename
	 *            file contains kernel profile
	 * @param queryType
	 *            the type of query to which the kernels belong
	 * @return a list of kernel instances
	 */
	private static ArrayList<Kernel> readKernelFile(String filename,
			int queryType) {
		ArrayList<Kernel> kernelList = new ArrayList<Kernel>();
		BufferedReader fileReader = null;
		String line;
		try {

			fileReader = new BufferedReader(new FileReader(MPSSim.PROFILE_PATH
					+ filename));
			// skip the column name of the first line
			line = fileReader.readLine();
			int i = 0;
			while ((line = fileReader.readLine()) != null) {
				String[] profile = line.split(",");
				Kernel kernel = new Kernel();
				kernel.setDuration(new Float(profile[2]).floatValue());
				kernel.setReal_duration(new Float(profile[2]).floatValue());
				kernel.setOccupancy(new Integer(profile[5]).intValue());
				kernel.setWarps_per_batch((int)(new Float(profile[4]).floatValue()*64*15));
				kernel.setWarps(new Integer(profile[3]).intValue());
				kernel.setQuery_type(queryType);
				kernel.setExecution_order(i);
				kernel.setSole_start_time(new Float(profile[1]).floatValue());

				kernelList.add(kernel);
//				System.out.println("Slack time is: "+kernel.getSlack_time());
				i++;
			}
			
			for(i=0; i<kernelList.size()-1;i++) {
				kernelList.get(i).setSlack_time(kernelList.get(i+1).getSole_start_time() - (kernelList.get(i).getDuration()+kernelList.get(i).getSole_start_time()));
				if(kernelList.get(i).getSlack_time()<0)	kernelList.get(i).setSlack_time(0);
//					System.out.println("file name: "+filename+", the slack id is: "+kernelList.get(i).getExecution_order());
			}
			
			fileReader.close();
		} catch (Exception ex) {
			System.out.println("Failed to read the file!" + ex.getMessage());
		}
/*		
		float whole_duration = 0;
		for (Kernel k : kernelList) {
			whole_duration +=k.getDuration();
		}
		System.out.println("query is "+filename+", whole duration is: "+whole_duration); 
 */
		return kernelList;
	}

	public static void main(String[] args) {
		MPSSim mps_sim = new MPSSim();
		/*
		 * manipulate the input
		 */
//		preprocess("sim.conf");
		generateTestCase(args[0], args[1], new Integer(args[2]).intValue(), new Integer(args[3]).intValue(), new Integer(args[4]).intValue(), new Integer(args[5]).intValue());
		read_load(args[0], args[1]);
		
		Random random = new Random();
		Random random2 = new Random();
		int start_time=0;
		float flag=0.0f;
		
		int t_variation=getStartVariation(args[0]);
		start_time = random.nextInt(t_variation);
		flag = random2.nextFloat();
		t_variation=getStartVariation(args[1]);
/*		
//		if(flag < 0.5) 	start_time = 0-start_time;
		target_start_point = 9000.0f+start_time;
		
		System.out.println("TARGET: start time is: "+target_start_point);

		t_variation=getStartVariation(args[1]);
		for(int i=0;i<n_bg;i++) {
			start_time = random.nextInt(t_variation);
			flag = random2.nextFloat();
			if(flag < 0.5) 	start_time = 0-start_time;
			
			bg_start_points.add(i*1000.0f+start_time);
			System.out.println("BG: start time is: "+bg_start_points.get(i));
		}
*/
		target_start_point = 10000.0f + getInitTime(args[0]) + getWarmupTime(args[1])+ random.nextInt(5);		
//		target_start_point = 8000.0f + random.nextInt(5);
		System.out.println("TARGET: start time is: "+target_start_point);
		
		for(int i=0;i<n_bg;i++) {
			start_time = random.nextInt(t_variation);
			flag = random2.nextFloat();
			bg_start_points.add(i*1000.0f + getInitTime(args[1]) + getWarmupTime(args[1]) + start_time);
/*			
			if(args[1].equals("dig"))
				bg_start_points.add(dig_delay[i]+start_time);
			else if(args[1].equals("imc"))
				bg_start_points.add(imc_delay[i]+start_time);
			else if(args[1].equals("ner"))
				bg_start_points.add(ner_delay[i]+start_time);
			else if(args[1].equals("pos"))
				bg_start_points.add(pos_delay[i]+start_time);
*/			
			System.out.println("BG: start time is: "+bg_start_points.get(i));
		}
/*		
		bg_start_points.add(0.0f);
		bg_start_points.add(1037.0f+start_time);
		bg_start_points.add(1995.0f+start_time);
		bg_start_points.add(3020.0f+start_time);
		bg_start_points.add(4023.0f+start_time);
		bg_start_points.add(5043.0f+start_time);
		bg_start_points.add(6017.0f+start_time);
		bg_start_points.add(7106.0f+start_time);
*/		
		/*
		 * start to simulate
		 */
//		mps_sim.mps_simulate();
		mps_sim.mps_simulate_quan();

		/*
		 * print out statistics about the MPS simulation
		 */
		calculate_latency(mps_sim);
//		calculate_utilization(mps_sim);
	}

	private static void read_load(String tg, String bg){
		List target_load_obj=null;
		List bg_load_obj=null;
		String TARGET_LOAD=LOAD_PATH+tg+"_target_load.csv";
		String BG_LOAD=LOAD_PATH+bg+"_bg_load.csv";
		
		try {
			CSVReader reader = new CSVReader(new FileReader(TARGET_LOAD), ',');
			try {
				target_load_obj = reader.readAll();
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		
		try {
			CSVReader reader = new CSVReader(new FileReader(BG_LOAD), ',');
			try {
				bg_load_obj = reader.readAll();
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		for(Object lat: target_load_obj) {
			String[] target = (String [])lat;
			for(int i=0;i<target.length; i++) {
				target_load.add(new Integer(target[i]));
			}
		}
		for(Object lat: bg_load_obj) {
			String[] target = (String [])lat;
			for(int i=0;i<target.length; i++) {
				bg_load.add(new Integer(target[i]));
			}
		}
		
//		for(int i=0;i<target_load.size();i++)
//			System.out.println("i: "+i+", value: "+target_load.get(i).intValue());
//		for(int i=0;i<target_load.size();i++)
//			System.out.println("BG i: "+i+", value: "+bg_load.get(i).intValue());
	}
	
	/**
	 * TODO calculate the latency distribution for mutiple target queries
	 * 
	 * @param mps_sim
	 */
	private static void calculate_latency(MPSSim mps_sim) {
		/*
		 * print out the statistics from the finished query queue, save the
		 * target query latency distribution into a file
		 */		
		float accumulative_latency = 0.0f;
		float target_endtime = 0.0f;

//		for (int i = 0; i < finishedQueries.size(); i++) {
		for (int i = 0; i < targetQueries.size(); i++) {
			ArrayList<Float> all_latency = new ArrayList<Float>();
			try {
				BufferedWriter bw = new BufferedWriter(new FileWriter(
						MPSSim.RESULT_PATH
								+ "sim-"+finishedQueries.get(i).peek().getQuery_name()
								+ "-" + n_bg + "-" + bg_name + ".csv", true));
//								+ "+" + n_bg + "+" + bg_name + "+" + schedulingType + ".csv"));				
//								+ "+" + n_bg + "+" + bg_name + "-" + schedulingType + "-"
//								+ "latency.csv"));
//				bw.write("end_to_end\n");
				for (Query finishedQuery : finishedQueries.get(i)) {
					accumulative_latency += finishedQuery.getEnd_time()
							- finishedQuery.getStart_time();
					
					if(finishedQueries.get(i).peek().getQuery_name().equals("imc")) {
						all_latency.add(finishedQuery.getEnd_time()-finishedQuery.getStart_time()+1.2f);
						System.out.println("tg is imc ");
					}
					else if(finishedQueries.get(i).peek().getQuery_name().equals("dig")) {
						all_latency.add(finishedQuery.getEnd_time()-finishedQuery.getStart_time()+1.0f);
						System.out.println("tg is dig ");
					}
					else {
						all_latency.add(finishedQuery.getEnd_time()-finishedQuery.getStart_time());
					}
					
//					System.out.println("start: "+finishedQuery.getStart_time()+", end: "+finishedQuery.getEnd_time());
//					bw.append(finishedQuery.getEnd_time()
//							- finishedQuery.getStart_time() + "\n");
					bw.write(finishedQuery.getEnd_time()
							- finishedQuery.getStart_time() + "\n");
					if (finishedQuery.getEnd_time() > target_endtime) {
						target_endtime = finishedQuery.getEnd_time();
					}
				}
				bw.close();
			} catch (Exception ex) {
				System.out
						.println("Failed to write to the latency.txt, the reason is: "
								+ ex.getMessage());
			}
			
			System.out.println("n_bg is: "+n_bg);

			Collections.sort(all_latency);
			System.out.println(i+", "+all_latency.size()+": 50%-ile latency is: "+all_latency.get(all_latency.size()/2).floatValue()+", 95%-ile latency is: "+all_latency.get(all_latency.size()*95/100).floatValue());
//			System.out.println("50%-ile latency is: "+all_latency.get(all_latency.size()/2).floatValue()+", 99%-ile latency is: "+all_latency.get(all_latency.size()*99/100).floatValue());
			/*
			 * print out the average latency for target queries
			 */
			System.out.println("The average latency for the target query: "
					+ String.format("%.2f", accumulative_latency
							/ finishedQueries.get(i).size()) + "(ms)");
		}
//		if(Detail) {
		if(true) {
//calculate latency of background queries		
		for (int i = targetQueries.size(); i < finishedQueries.size(); i++) {
			ArrayList<Float> all_latency = new ArrayList<Float>();
			try {
//				BufferedWriter bw = new BufferedWriter(new FileWriter(
//						MPSSim.RESULT_PATH
//								+ "sim-"+finishedQueries.get(i).peek().getQuery_name()
//								+ "-" + n_bg + "-" + i+"-"+bg_name + ".csv"));
//				bw.write("end_to_end\n");
				for (Query finishedQuery : finishedQueries.get(i)) {
					if(finishedQuery.getEnd_time()<=COMPLETE_TIME) {
						accumulative_latency += finishedQuery.getEnd_time()
								- finishedQuery.getStart_time();
						all_latency.add(finishedQuery.getEnd_time()-finishedQuery.getStart_time());
//						bw.write(finishedQuery.getEnd_time()
//								- finishedQuery.getStart_time() + "\n");
						if (finishedQuery.getEnd_time() > target_endtime) {
							target_endtime = finishedQuery.getEnd_time();
						}
					}
//					System.out.println("start: "+finishedQuery.getStart_time()+", end: "+finishedQuery.getEnd_time()+", complete: "+COMPLETE_TIME);
				}
//				bw.close();
			} catch (Exception ex) {
				System.out
						.println("Failed to write to the latency.txt, the reason is: "
								+ ex.getMessage());
			}

			Collections.sort(all_latency);
			System.out.println(i+", "+all_latency.size()+": 50%-ile latency is: "+all_latency.get(all_latency.size()/2).floatValue()+", 95%-ile latency is: "+all_latency.get(all_latency.size()*95/100).floatValue());
			/*
			 * print out the average latency for target queries
			 */
//			System.out.println("The average latency for the target query: "
//					+ String.format("%.2f", accumulative_latency
//							/ finishedQueries.get(i).size()) + "(ms)");
		}
		}
	}

	/**
	 * TODO another way to calculate the utilization; TODO calculate the
	 * utilization with multiple target queries
	 * 
	 * @param mps_sim
	 */
	private static void calculate_utilization(MPSSim mps_sim) {
		float accumulative_utilization = 0.0f;
		float end_time = 0.0f;
		float previous_time = 0.0f;
		for (LinkedList<Query> queries : finishedQueries) {
			if (queries.get(queries.size() - 1).getEnd_time() > end_time) {
				end_time = queries.get(queries.size() - 1).getEnd_time();
			}
		}
		for (Map.Entry<Float, Float> utils : utilization) {
			if (utils.getKey() <= end_time) {
				accumulative_utilization += (utils.getKey() - previous_time)
						/ end_time * utils.getValue();
				previous_time = utils.getKey();
			}
		}
		System.out.println("The average utilization: "
				+ String.format("%.2f", accumulative_utilization));
	}

	private static float getSlack(String query_name) {
		if(Detail)	System.out.println(query_name);
/*		
		if(query_name.equals("dig")) {
			return 1.0f;
		} else if(query_name.equals("imc")) {
			return 0.56f;
		} else if(query_name.equals("face")) {
			return 0.32f;
		} else if(query_name.equals("pos")) {
			return 0.0f;
		} else if(query_name.equals("ner")) {
			return 0.0f;
		}		
*/
///*		
		if(query_name.equals("dig")) {
			return 0.3f;
		} else if(query_name.equals("imc")) {
			return 1.0f;
		} else if(query_name.equals("face")) {
			return 0.32f;
		} else if(query_name.equals("pos")) {
			return 0.05f;
		} else if(query_name.equals("ner")) {
			return 0.05f;
		}
//*/		
		return 0;
	}
	
	private static float getInitTime(String query_name) {
		if(Detail)	System.out.println(query_name);
		
		if(query_name.equals("dig")) {
			return 780.0f;
		} else if(query_name.equals("imc")) {
			return 1800.0f;
		} else if(query_name.equals("face")) {
			return 1.0f;
//			return 1.46f;
		} else if(query_name.equals("pos")) {
			return 85.0f;
		} else if(query_name.equals("ner")) {
			return 85.0f;
		}
		
		return 1.0f;
	}
	
	private static float getWarmupTime(String query_name) {
		if(Detail)	System.out.println(query_name);
		
		if(query_name.equals("dig")) {
			return 44.0f;
		} else if(query_name.equals("imc")) {
			return 180.0f;
		} else if(query_name.equals("face")) {
			return 1.0f;
		} else if(query_name.equals("pos")) {
			return 5.0f;
		} else if(query_name.equals("ner")) {
			return 5.0f;
		}
		
		return 1.0f;
	}
	
	private static float getMicroDelay(String query_name) {
		if(Detail)	System.out.println(query_name);
		
		if(query_name.equals("dig")) {
			return 1.0f;
		} else if(query_name.equals("imc")) {
			return 1.0f;
//			return 1.1f;
		} else if(query_name.equals("face")) {
			return 1.0f;
//			return 1.46f;
		} else if(query_name.equals("pos")) {
			return 1.0f;
		} else if(query_name.equals("ner")) {
			return 1.0f;
		}
		
		return 1.0f;
	}
	
	private static int getStartVariation(String query_name) {
		if(Detail)	System.out.println(query_name);
		
		if(query_name.equals("dig")) {
			return 20;
		} else if(query_name.equals("imc")) {
			return 200;
		} else if(query_name.equals("face")) {
			return 20;
		} else if(query_name.equals("pos")) {
			return 5;
		} else if(query_name.equals("ner")) {
			return 5;
		}
		
		return 20;
	}
	
	private static void generateTestCase(String target, String bg, int tg_num, int bg_num, int tg_query_num, int bg_query_num) {
		ArrayList<SimConfiguration> targetConfs = new ArrayList<SimConfiguration>();
		ArrayList<SimConfiguration> backgroundConfs = new ArrayList<SimConfiguration>();
		
		SimConfiguration config = new SimConfiguration();
		config.setQueryName(target);
		config.setClientNum(tg_num);
		n_tg = config.getClientNum();
		config.setQueryNum(tg_query_num);
		targetConfs.add(config);
		System.out.println("Target QueryName: "+config.getQueryName()+", Client Num: "+config.getClientNum() + ", Query num: "+config.getQueryNum());
		
		SimConfiguration bg_config = new SimConfiguration();
		bg_config.setQueryName(bg);
		bg_config.setClientNum(bg_num);
		bg_config.setQueryNum(bg_query_num);
		n_bg = bg_config.getClientNum();
		bg_name=bg_config.getQueryName();
		backgroundConfs.add(bg_config);
		System.out.println("Background QueryName: "+bg_config.getQueryName()+", Client Num: "+bg_config.getClientNum() + ", Query num: "+bg_config.getQueryNum());

		schedulingType="fifo";
		
		/*
		 * populate the target queries
		 */
		for (SimConfiguration conf : targetConfs) {
			for (int i = 0; i < conf.getClientNum(); i++) {
				LinkedList<Query> targetQueryList = new LinkedList<Query>();
				LinkedList<Query> finishedQueryList = new LinkedList<Query>();
				for (int j = 0; j < conf.getQueryNum(); j++) {
					Query targetQuery = new Query();
					targetQuery.setQuery_type(i);
					targetQuery.setQuery_name(conf.getQueryName());
					for (Kernel kernel : readKernelFile(conf.getQueryName()
							+ ".csv", targetQuery.getQuery_type())) {
						targetQuery.getKernelQueue().offer(kernel);
					}
					targetQueryList.offer(targetQuery);
				}
				query_id.add(new Integer(0));
				targetQueries.add(targetQueryList);
				finishedQueries.add(finishedQueryList);
				slacks.add(new Float(getSlack(conf.getQueryName())));	
				microDelays.add(new Float(getMicroDelay(conf.getQueryName())));
			}
		}
		/*
		 * populate the background queries, which are always numbered after
		 * target queries
		 */
		for (SimConfiguration conf : backgroundConfs) {
			for (int i = 0; i < conf.getClientNum(); i++) {
				LinkedList<Query> backgroundQueryList = new LinkedList<Query>();
				LinkedList<Query> finishedBGQueryList = new LinkedList<Query>();
				
				for (int j = 0; j < conf.getQueryNum(); j++) {
					Query backgroundQuery = new Query();
					backgroundQuery.setQuery_type(i + targetQueries.size());
					backgroundQuery.setQuery_name(conf.getQueryName());
					for (Kernel kernel : readKernelFile(conf.getQueryName()
							+ ".csv", backgroundQuery.getQuery_type())) {
						backgroundQuery.getKernelQueue().offer(kernel);
					}
					backgroundQueryList.offer(backgroundQuery);
				}
				query_id.add(new Integer(0));
				backgroundQueries.add(backgroundQueryList);
				finishedQueries.add(finishedBGQueryList);
				slacks.add(new Float(getSlack(conf.getQueryName())));	
				microDelays.add(new Float(getMicroDelay(conf.getQueryName())));	
			}
		}
		
		for(int i=0;i<slacks.size();i++)
			System.out.println(slacks.get(i));
	}
	
	/**
	 * Read the simulation configuration and generate queries of different types
	 * 
	 * @param simConf
	 *            the simulation configuration file
	 */
	private static void preprocess(String simConf) {
		ArrayList<SimConfiguration> targetConfs = null;
		ArrayList<SimConfiguration> backgroundConfs = null;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					MPSSim.CONFIG_PATH + simConf));
			String line;
			targetConfs = new ArrayList<SimConfiguration>();
			backgroundConfs = new ArrayList<SimConfiguration>();
			/*
			 * read the configuration for target queries, the configuration
			 * format is (query name, client number, query number)
			 */
			if ((line = reader.readLine()) != null) {
				String[] confs = line.split(",");
				System.out.println("confs length: "+confs.length+" confs: "+confs[0]);
				
				for (int i = 0; i < confs.length; i += 3) {
					SimConfiguration config = new SimConfiguration();
					config.setQueryName(confs[i]);
					config.setClientNum(new Integer(confs[i + 1]));
					config.setQueryNum(new Integer(confs[i + 2]));
					targetConfs.add(config);
					System.out.println("Target QueryName: "+config.getQueryName()+", Client NUm: "+config.getClientNum() + ", Query num: "+config.getQueryNum());
				}
			}
			/*
			 * read the configuration for background queries
			 */
			if ((line = reader.readLine()) != null) {
				String[] confs = line.split(",");
				for (int i = 0; i < confs.length; i += 3) {
					SimConfiguration config = new SimConfiguration();
					config.setQueryName(confs[i]);
					config.setClientNum(new Integer(confs[i + 1]));
					config.setQueryNum(new Integer(confs[i + 2]));
					n_bg = config.getClientNum();
					bg_name=config.getQueryName();
					backgroundConfs.add(config);
					System.out.println("Background QueryName: "+config.getQueryName()+", Client NUm: "+config.getClientNum() + ", Query num: "+config.getQueryNum());

				}
			}
			/*
			 * read the scheduling policy
			 */
			if ((line = reader.readLine()) != null) {
				schedulingType = line;
			}
			reader.close();
		} catch (Exception ex) {
			System.out
					.println("Failed to read the configuration file, the reason is: "
							+ ex.getMessage());
		}

		/*
		 * populate the target queries
		 */
		for (SimConfiguration conf : targetConfs) {
			for (int i = 0; i < conf.getClientNum(); i++) {
				LinkedList<Query> targetQueryList = new LinkedList<Query>();
				LinkedList<Query> finishedQueryList = new LinkedList<Query>();
				for (int j = 0; j < conf.getQueryNum(); j++) {
					Query targetQuery = new Query();
					targetQuery.setQuery_type(i);
					targetQuery.setQuery_name(conf.getQueryName());
					for (Kernel kernel : readKernelFile(conf.getQueryName()
							+ ".csv", targetQuery.getQuery_type())) {
						targetQuery.getKernelQueue().offer(kernel);
					}
					targetQueryList.offer(targetQuery);
				}
				query_id.add(new Integer(0));
				targetQueries.add(targetQueryList);
				finishedQueries.add(finishedQueryList);
			}
		}
		/*
		 * populate the background queries, which are always numbered after
		 * target queries
		 */
		for (SimConfiguration conf : backgroundConfs) {
			for (int i = 0; i < conf.getClientNum(); i++) {
				LinkedList<Query> backgroundQueryList = new LinkedList<Query>();
				LinkedList<Query> finishedBGQueryList = new LinkedList<Query>();
				
				for (int j = 0; j < conf.getQueryNum(); j++) {
					Query backgroundQuery = new Query();
					backgroundQuery.setQuery_type(i + targetQueries.size());
					backgroundQuery.setQuery_name(conf.getQueryName());
					for (Kernel kernel : readKernelFile(conf.getQueryName()
							+ ".csv", backgroundQuery.getQuery_type())) {
						backgroundQuery.getKernelQueue().offer(kernel);
					}
					backgroundQueryList.offer(backgroundQuery);
				}
				query_id.add(new Integer(0));
				backgroundQueries.add(backgroundQueryList);
				finishedQueries.add(finishedBGQueryList);
			}
		}
	}
}
