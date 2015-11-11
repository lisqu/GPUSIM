package edu.umich.clarity;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

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
	public static AtomicInteger AVAIL_SLOTS = new AtomicInteger(15);

	public static final float KERNEL_SLACK = 0.0f;
	// the location of kernel profiles
	public static final String PROFILE_PATH = "input/formated/";
	// the location of simulation configuration
	public static final String CONFIG_PATH = "input/";
	public static final String RESULT_PATH = "simulated/";

	public static final String LOAD_PATH = "input/load/";
	// public static final String BG_LOAD="input/load/bg_load.csv";

//	public static float target_start_point;
//	public static ArrayList<Float> bg_start_points = new ArrayList<Float>();
	public static ArrayList<Float> start_points = new ArrayList<Float>();

	public static ArrayList<LinkedList<Query>> targetQueries;
	public static ArrayList<LinkedList<Query>> backgroundQueries;
	public static ArrayList<Float> slacks = new ArrayList<Float>(); // slacks
																	// between
																	// two
																	// queries
																	// in a
																	// client

	private static Random randQuery = new Random(); // random delay between
													// queries
	private static ArrayList<LinkedList<Query>> finishedQueries; // finished
																	// query
																	// lists

	private static ArrayList<Map.Entry<Float, Float>> utilization;

	private static ArrayList<Float> microDelays = new ArrayList<Float>(); // Slow down due to co-location
	
	private static ArrayList<ArrayList<Integer>> query_load = new ArrayList<ArrayList<Integer>>();
//	private static ArrayList<ArrayList<Integer>> target_load = new ArrayList<ArrayList<Integer>>();
//	private static ArrayList<Integer> target_load_1 = new ArrayList<Integer>();
//	private static ArrayList<Integer> target_load_2 = new ArrayList<Integer>();
//	private static ArrayList<Integer> bg_load = new ArrayList<Integer>();
//	private static ArrayList<ArrayList<Integer>> bg_load = new ArrayList<ArrayList<Integer>>();
	
	private static ArrayList<String> all_query_type = new ArrayList<String>();	//the type of queries running on the same accelerator
	private static ArrayList<Integer> all_query_num = new ArrayList<Integer>();	//the number of each type of queries running on the same accelerator

	private static ArrayList<Integer> query_id = new ArrayList<Integer>();
	private ArrayList<Query> issuingQueries;
	private ArrayList<Integer> issueIndicator;
	private Queue<Kernel> kernelQueue;

	private ArrayList<Kernel> memCpies_HTD = new ArrayList<Kernel>();
	private ArrayList<Kernel> memCpies_DTH = new ArrayList<Kernel>();

	private ArrayList<MemCpy> active_memcpy_htd = new ArrayList<MemCpy>();
	private ArrayList<MemCpy> active_memcpy_dth = new ArrayList<MemCpy>();
	private ArrayList<MemCpy> active_memcpy = new ArrayList<MemCpy>();

	private ArrayList<Kernel> cudaMallocs = new ArrayList<Kernel>();
	private ArrayList<Kernel> cudaFrees = new ArrayList<Kernel>();
	private ArrayList<Kernel> activeKernel = new ArrayList<Kernel>();

	public static float COMPLETE_TIME; // The complete time of target queries
	public static int COMPLETE = 0;
	public static int n_bg;
	public static int n_tg;
	public static String bg_name;
	public static String tg_name;
	private final static boolean Detail = false;
	private final static boolean MPS_enabled = true;
	private final static boolean CONSIDER_DIRECTION = true;

	private static float kernel_elapse_time=0.0f;
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

			issuingQueries.get(issuingQueries.size() - 1).setReady_time(start_points.get(queries.get(0).getLoad_id()));
			issuingQueries.get(issuingQueries.size() - 1).setStart_time(start_points.get(queries.get(0).getLoad_id()));
			System.out.println("User-facing---"+queries.get(0).getQuery_name()+ " start at: "+start_points.get(queries.get(0).getLoad_id()));
		}
		for (LinkedList<Query> queries : backgroundQueries) {
			issuingQueries.add(queries.poll());

			issuingQueries.get(issuingQueries.size() - 1).setReady_time(start_points.get(queries.get(0).getLoad_id()));
			issuingQueries.get(issuingQueries.size() - 1).setStart_time(start_points.get(queries.get(0).getLoad_id()));
			System.out.println("Background---"+queries.get(0).getQuery_name()+" start at: "+start_points.get(queries.get(0).getLoad_id()));
		}
/*
		for (LinkedList<Query> queries : targetQueries) {
			issuingQueries.add(queries.poll());

			issuingQueries.get(issuingQueries.size() - 1).setReady_time(
					target_start_point);
			issuingQueries.get(issuingQueries.size() - 1).setStart_time(
					target_start_point);
			// i = i+1.0f;
		}

		int i = 0;
		for (LinkedList<Query> queries : backgroundQueries) {
			issuingQueries.add(queries.poll());
			// issuingQueries.get(issuingQueries.size()-1).setReady_time(start_time);
			// issuingQueries.get(issuingQueries.size()-1).setStart_time(start_time);
			issuingQueries.get(issuingQueries.size() - 1).setReady_time(
					bg_start_points.get(i));
			issuingQueries.get(issuingQueries.size() - 1).setStart_time(
					bg_start_points.get(i));
			i++;

		}
*/
		for (int m = 0; m < (issuingQueries.size()); m++) {
			issueIndicator.add(1);
		}
		// enqueueKernel(0.0f);
		enqueueKernel_quan(0.0f, 0.0f, 0);
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

		float earliest = 1000000000.0f;
		for (Integer id : select_range) {
			if (issuingQueries.get(id).getReady_time() < earliest) {
				chosen_query = id;
				earliest = issuingQueries.get(id).getReady_time();
			}
//			 System.out.println(issuingQueries.get(id).getQuery_type()+": "+issuingQueries.get(id).getReady_time());
		}
		// issuingQueries.get(chosen_query).setReady_time(100000000.0f);
		// System.out.println("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
		return chosen_query;
	}

	/*
	 * private int FIFOSchedule(ArrayList<Integer> select_range) { int
	 * chosen_query;
	 * 
	 * Random random = new Random(); int chosen_index =
	 * random.nextInt(select_range.size()); chosen_query =
	 * select_range.get(chosen_index); return chosen_query; }
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
									+ kernel.getExecution_order()
									+ " from query " + kernel.getQuery_type()
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
				System.out
						.println("Enqueue new kernels from the issuing list...");
			}
			enqueueKernel(kernel.getEnd_time());
		}
		System.out
				.println("At time "
						+ kernel.getEnd_time()
						+ "(ms): All kernel have been executed, the simulation will stop");
	}

	/**
	 * How many SMs are not occupied by other programs
	 * 
	 * @param st
	 * @return
	 */
	private int calLeftSM(float st) {
		int avail = 15;
		for (Kernel k : activeKernel) {
			if (Float.compare(k.getNonfull_time(), st) < 0
					&& Float.compare(k.getEnd_time(), st) > 0) {
				avail -= k.getSms();
				// System.out.println("kernel sms is: "+k.getSms()+", from "+
				// k.getNonfull_time()+" to "+ k.getEnd_time());
			}
		}

		for (int i = 0; i < activeKernel.size(); i++) {
			if (Float.compare(activeKernel.get(i).getEnd_time(), st) < 0) {
				activeKernel.remove(i);
			}
		}

		return avail;
	}

	/**
	 * update the start time of the current kernel
	 * 
	 * @param st
	 * @return
	 */
	private float updateStartTime(float st) {
		int avail = 15;
		float ret = st;
		for (Kernel k : activeKernel) {
			if (Float.compare(k.getNonfull_time(), st) < 0
					&& Float.compare(k.getEnd_time(), st) > 0) {
				avail -= k.getSms();
			}
		}

		if (avail >= 1)
			ret = st;
		else {
			ret = 100000000.0f;
			for (Kernel k : activeKernel) {
				if (Float.compare(k.getNonfull_time(), st) < 0
						&& Float.compare(k.getEnd_time(), st) > 0
						&& Float.compare(k.getEnd_time(), ret) < 0) {
					ret = k.getEnd_time();
				}
			}
			/*
			 * do { avail = 15; ret +=1.0f; for(Kernel k: activeKernel) {
			 * if(k.getNonfull_time() <= ret && k.getEnd_time() >= ret) { avail
			 * -= k.getSms(); } } } while (avail < 1);
			 */
		}
//		kernel_elapse_time = ret;
		return ret;
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
	private void enqueueKernel_quan(float elapse_time, float overlapping_time,
			int left_sm) {
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
			// while (available_slots >= 0 && select_range.size() != 0) {
			// System.out.println("select_range size " +
			// select_range.size());
			/*
			 * 4. random select one query candidate to mimic FIFO within MPS
			 */
			int chosen_query = 0;
			if (MPSSim.schedulingType.equalsIgnoreCase(MPSSim.FIFO_SCHEDULE)) {
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
			Kernel kernel = issuingQueries.get(chosen_query).getKernelQueue()
					.peek();
			boolean isConstrained = issuingQueries.get(chosen_query)
					.isSeqconstraint();
			// if (!isConstrained
			// && available_slots - kernel.getOccupancy() >= 0) {
			if (!isConstrained) {
				if ((kernel.getOccupancy() != 0)
						|| (kernel.getOccupancy() == 0 && !pcie_transfer)) {
					/*
					 * 6. if all good, get the kernel from selected query type
					 */
					kernel = issuingQueries.get(chosen_query).getKernelQueue()
							.poll();
					/*
					 * 7. set the kernel's start time and end time
					 */
					issuingQueries.get(chosen_query).setSeqconstraint(true);
					float org_st;

/*					
					if(kernel.getWarps()!=0)
						org_st = Math.max(kernel_elapse_time - overlapping_time,
								issuingQueries.get(kernel.getQuery_type())
								.getReady_time());						
					else org_st = Math.max(elapse_time - overlapping_time,
							issuingQueries.get(kernel.getQuery_type())
									.getReady_time());
*/
					if(kernel.getWarps()!=0)
						org_st = Math.max(kernel_elapse_time,
								issuingQueries.get(kernel.getQuery_type())
								.getReady_time());						
					else org_st = Math.max(elapse_time,
							issuingQueries.get(kernel.getQuery_type())
									.getReady_time());
					
//					 if(Float.compare(kernel.getDuration(), 20) > 0 &&
//					 Float.compare(elapse_time, 50000.0f) < 0 &&
//					 Float.compare(elapse_time, 25000.0f) > 0)
//					 System.out.println("*********ready: "+issuingQueries.get(kernel.getQuery_type()).getReady_time()
//					 +"; elapse: "+elapse_time+", kernel elapse: "+kernel_elapse_time+", overlapping: "+overlapping_time+", st: "+org_st);

					float st = updateStartTime(org_st);

//					 System.out.println("-----ready: "+issuingQueries.get(kernel.getQuery_type()).getReady_time()
//					 +"; elapse: "+elapse_time+", overlapping: "+overlapping_time+", org_st: "+org_st+", st: "+st);

//					if(kernel.getQuery_type()==2)
//						System.out.println("ready: "+issuingQueries.get(kernel.getQuery_type()).getReady_time()+", start at: "+st);

					kernel.setStart_time(st);
					// /*
					if (kernel.getOccupancy() == 0) {
						kernel.setEnd_time(kernel.getStart_time()
								+ kernel.getDuration());
						kernel.setReal_duration(kernel.getDuration()); // Initialize
																		// the
																		// duration

						if (kernel.getDirection() == 1)
							memCpies_HTD.add(kernel); // Add to the memcpy host
														// to device queue
						else if (kernel.getDirection() == 2)
							memCpies_DTH.add(kernel); // Add to the memcpy
														// device to host queue
							// active_memcpy.add(new MemCpy(kernel)); //record
							// the active_memcpy

						kernel.setReal_duration(calMemcpyDuration(kernel, st, kernel.getDirection())); // Update the duration
															// of the kernel
//						 System.out.println(kernel.getExecution_order()+" : "+kernel.getStart_time()+", duration:"+kernel.getDuration()+", client: "+kernel.getQuery_type());
					}
					// */

					/***** Todo: manage cudaMalloc contention *******/
/*					
					if (kernel.getCuda_malloc() == 1) {
						// kernel.setDuration(1.0f); //Initialize the duration
						kernel.setReal_duration(1.0f); // Initialize the
														// duration
						cudaMallocs.add(kernel); // Add to the memcpy queue
						kernel.setReal_duration(calMallocDuration(kernel, st)); // Update
																				// the
																				// duration
																				// of
																				// the
																				// kernel
					}
*/
					/***** Todo: manage cudaFree contention *******/
/*
					if (kernel.getCuda_free() == 1) {
						kernel.setReal_duration(1.0f); // Initialize the
														// duration
						cudaFrees.add(kernel); // Add to the memcpy queue
						kernel.setReal_duration(calFreeDuration(kernel, st)); // Update
																				// the
																				// duration
																				// of
																				// the
																				// kernel
					}
*/
					// kernel.setDuration(kernel.getDuration()*microDelays.get(kernel.getQuery_type()));
					// kernel.setReal_duration(kernel.getDuration()*microDelays.get(kernel.getQuery_type()));
					if (MPS_enabled) {
						int overlapped = 0;
						int batches = 1;
						int org_batches = 1;

						if (kernel.getOccupancy() != 0) {
							int left = calLeftSM(st);
							// int left = 3;
							overlapped = kernel.getWarps_per_batch() / 15 * left;
							// overlapped = kernel.getWarps_per_batch()/15 *
							// left_sm;
							org_batches = (int) Math.ceil(kernel.getWarps()
									/ (float) (kernel.getWarps_per_batch()));
							kernel.setOverlapped_warps(overlapped);
							// System.out.println("left_sm, cal left_sm: "+left_sm+" , "+calLeftSM(st));

							// batches = 1+(int) Math.ceil(
							// (kernel.getWarps()-overlapped)/(float)(kernel.getWarps_per_batch()));
							if (overlapped > 0)
								batches = 1 + (int) Math
										.ceil((kernel.getWarps() - overlapped)
												/ (float) (kernel
														.getWarps_per_batch()));
							else
								batches = (int) Math
										.ceil(kernel.getWarps()
												/ (float) (kernel
														.getWarps_per_batch()));
///*							
///////////////////////////To be considered////////////////////////////////////////							
							if(org_batches == 1 && batches == 2)								
								kernel.setReal_duration(kernel.getDuration()
									* (1.95f+randQuery.nextFloat()*0.05f)
									* microDelays.get(kernel.getQuery_type()));
///////////////////////////////////////////////////////////////////////////////////
							else 
//*/
							// batches = (int)
							// Math.ceil(kernel.getWarps()/(float)(kernel.getWarps_per_batch()));
							kernel.setReal_duration(kernel.getDuration()
									* batches / org_batches
									* microDelays.get(kernel.getQuery_type()));
	
							// Mark the time range where the current kernel does
							// not occupy all the SMs, calculates the number of
							// SMs used in that range
							if (overlapped > kernel.getWarps())
								kernel.setNonfull_time(kernel.getStart_time());
							else
								kernel.setNonfull_time(kernel.getStart_time()
										+ kernel.getDuration() * (batches - 1)
										/ batches);

							int sms;
							if (overlapped > kernel.getWarps()) {
								sms = kernel.getWarps()
										/ (kernel.getWarps_per_batch() / 15);
							} else {
								int bat = (kernel.getWarps() - overlapped)
										/ kernel.getWarps_per_batch();
								sms = (int) Math.ceil((kernel.getWarps()
										- overlapped - bat
										* kernel.getWarps_per_batch())
										/ (kernel.getWarps_per_batch() / 15));
							}
							// int sms = (int) Math.ceil(
							// (kernel.getWarps()-(batches-1)*kernel.getWarps_per_batch())/(kernel.getWarps_per_batch()/15)
							// );
							if (sms < 0) sms = 0;
							kernel.setSms(sms);

							activeKernel.add(kernel);
						}
						// else kernel.setReal_duration(kernel.getDuration());
					}

					// if(kernel.getQuery_type() >=1)
					// kernel.setReal_duration(kernel.getReal_duration()*1.0f);

					// if(kernel.getQuery_type() == 0)
					// System.out.println(kernel.getExecution_order()+" : "+kernel.getDuration()+"->"+kernel.getReal_duration());

					if(kernel.getWarps() !=0) {
						kernel.setEnd_time(kernel.getReal_duration() + st
							+ MPSSim.KERNEL_SLACK);
//						kernel_elapse_time = kernel.getEnd_time();
					}
					else {
						kernel.setEnd_time(kernel.getReal_duration() + kernel.getStart_time()
							+ MPSSim.KERNEL_SLACK);
					}
//					System.out.println("-------start: "+kernel.getStart_time()+" , end: "+kernel.getEnd_time()+" ,st: --------"+st);

					// kernel.setEnd_time(kernel.getDuration() + elapse_time +
					// MPSSim.KERNEL_SLACK);
					kernelQueue.offer(kernel);
					// if(kernel.getQuery_type() >= targetQueries.size() &&
					// elapse_time <1000)
					// System.out.println("Duration: "+kernel.getDuration()+", start: "+kernel.getStart_time()+", end: "+kernel.getEnd_time());

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

					// if (kernel.getOccupancy() == 0) {
					// pcie_transfer = true;
					// }
				}
			}
			/*
			 * 9. this type of query should not be considered during next round
			 */
			select_range.remove(select_range.indexOf(chosen_query));
			// }
		} else {
			System.out
					.println("At time "
							+ elapse_time
							+ "(ms): all queries have been submitted, wait for the kernel queue to become empty");
		}
	}

	public float calMemcpyDuration_old(Kernel kernel, float current_time,
			int direction) {
		float ret = 0.0f;

		LinkedList<Integer> active_memcpies = new LinkedList<Integer>();
		if (direction == 1) {
			for (Kernel k : memCpies_HTD) {
				// if(k.getStart_time()+k.getReal_duration() >= current_time +
				// kernel.getDuration()) {
				if (k.getStart_time() + k.getReal_duration() >= current_time) {
					active_memcpies.add(new Integer(memCpies_HTD.indexOf(k)));
					ret += k.getDuration() - (current_time - k.getStart_time());
				}
			}
		} else if (direction == 2) {
			for (Kernel k : memCpies_DTH) {
				// if(k.getStart_time()+k.getReal_duration() >= current_time +
				// kernel.getDuration()) {
				if (k.getStart_time() + k.getReal_duration() >= current_time) {
					active_memcpies.add(new Integer(memCpies_DTH.indexOf(k)));
					ret += k.getDuration() - (current_time - k.getStart_time());
				}
			}
		}

		float slow_down;
		if (active_memcpies.size() == 1) {
			ret = kernel.getDuration();
			slow_down = 1.0f;
		} else if (active_memcpies.size() >= 2 && active_memcpies.size() < 4) {
			// ret = kernel.getDuration() * (1.0f + 0.05f *
			// active_memcpies.size());
			// slow_down = 1.0f + 0.05f *active_memcpies.size();
			ret = kernel.getDuration()
					* (1.0f + 0.03f * active_memcpies.size());
			slow_down = 1.0f + 0.03f * active_memcpies.size();
		} else {
			slow_down = 1.0f + ((active_memcpies.size()) / 4) * 0.3f;
			// slow_down = (float)(active_memcpies.size()) / 5.0f;
			// System.out.println("the memcpy is delayed~~~~~~~~~~~~~~~~~~");
		}
		// slow_down = (float) (1.0f +
		// Math.ceil(((active_memcpies.size()-1)/3))*0.4f);

		ret = kernel.getDuration() * slow_down; // calculate new duration

		if (kernel.getQuery_type() == 0)
			System.out.println("the memcpy is delayed******************"
					+ active_memcpies.size() + ", length: " + ret + ", type: "
					+ kernel.getDirection());

		if (slow_down > 1.0f) {
			for (int i = 0; i < active_memcpies.size(); i++) {
				if (direction == 1) {
					// ret = ret/3.0f + kernel.getDuration();
					float new_time = memCpies_HTD.get(active_memcpies.get(i))
							.getDuration() * slow_down; // calculate new
														// duration
					float delta_time = new_time
							- memCpies_HTD.get(active_memcpies.get(i))
									.getReal_duration();
					// System.out.println("delta_time is: "+delta_time);
					if (delta_time < 0)
						delta_time = 0;

					memCpies_HTD.get(active_memcpies.get(i)).setReal_duration(
							new_time); // update new duration
					// if(memCpies_HTD.get(active_memcpies.get(i)).getQuery_type()
					// == 0)
					// System.out.println("Duration is updated to--------------------------"+new_time+", "+memCpies_HTD.get(active_memcpies.get(i)).getQuery_type());

					issuingQueries.get(
							memCpies_HTD.get(active_memcpies.get(i))
									.getQuery_type()).setReady_time(
							issuingQueries.get(
									memCpies_HTD.get(active_memcpies.get(i))
											.getQuery_type()).getReady_time()
									+ delta_time);
				} else if (direction == 2) {
					float new_time = memCpies_DTH.get(active_memcpies.get(i))
							.getDuration() * slow_down; // calculate new
														// duration
					float delta_time = new_time
							- memCpies_DTH.get(active_memcpies.get(i))
									.getReal_duration();
					if (delta_time < 0)
						delta_time = 0;

					memCpies_DTH.get(active_memcpies.get(i)).setReal_duration(
							new_time); // update new duration

					// if(memCpies_DTH.get(active_memcpies.get(i)).getQuery_type()
					// == 0)
					// System.out.println("Duration is: "+ new_time);
					issuingQueries.get(
							memCpies_DTH.get(active_memcpies.get(i))
									.getQuery_type()).setReady_time(
							issuingQueries.get(
									memCpies_DTH.get(active_memcpies.get(i))
											.getQuery_type()).getReady_time()
									+ delta_time);
				}
			}
			// System.out.println(ret/3 + kernel.getDuration());
			// ret = ret/3.0f + kernel.getDuration();
			// ret = kernel.getDuration()* (n_tg + n_bg)/3.86f;
		}

		if (direction == 1) {
			for (int i = 0; i < memCpies_HTD.size(); i++) {
				if (memCpies_HTD.get(i).getStart_time()
						+ memCpies_HTD.get(i).getReal_duration() + 100 < current_time) {
					memCpies_HTD.remove(i);
				}
			}
		} else if (direction == 2) {
			for (int i = 0; i < memCpies_DTH.size(); i++) {
				if (memCpies_DTH.get(i).getStart_time()
						+ memCpies_DTH.get(i).getReal_duration() < current_time) {
					memCpies_DTH.remove(i);
				}
			}
		}

		/*
		 * if(kernel.getQuery_type() < 1)
		 * System.out.println("client: "+kernel.getQuery_type
		 * ()+", duration updates from: "
		 * +kernel.getDuration()+" to "+ret+", active is: "
		 * +active_memcpies+", current time: "+current_time
		 * +", size: "+active_memcpies
		 * .size()+", ret is: "+ret+", start: "+kernel.getStart_time());
		 */

		return ret;
	}

	public float calMemcpyDuration(Kernel kernel, float current_time,
			int direction) {
		float ret = 0.0f;
		float slow = 0.0f;
		float raw_slow = 0.0f;
		float raw_time = 0.0f;
//		active_memcpy.add(new MemCpy(kernel));
		float start_t = kernel.getStart_time();
		float end_t = kernel.getStart_time() + kernel.getDuration();
		int pre_parallel = 0, parallel = 0;

		if (CONSIDER_DIRECTION) {
			if (direction == 1) {
				/**
				 * 1. Sort the active memory copies in the order of completion
				 */
				Collections.sort(active_memcpy_htd);

				/**
				 * 2. Add the current memcpy into the queue
				 */
				MemCpy current = new MemCpy(kernel);

				/**
				 * 3. Remove the completed memory copies
				 */
				for (Iterator<MemCpy> iterator = active_memcpy_htd.iterator(); iterator.hasNext();) {
					MemCpy mc = iterator.next();
					if (Float.compare(mc.getK().getEnd_time(), start_t) < 0) {
					// Remove the current element from the iterator and the list.
						iterator.remove();
					}
				}
				
				/**
				 * 4. Check whether there are any active pinned memory copies in
				 * the same direction. if there is any active pinned memory
				 * copies, the current memcpy cannot start before the completion
				 * of the pinned memory copy.
				 */
				for (Iterator<MemCpy> iterator = active_memcpy_htd.iterator(); iterator.hasNext();) {
					MemCpy mc = iterator.next();
					if (mc.getPinned() == 1) {
						start_t = mc.getK().getEnd_time();
						end_t = mc.getK().getEnd_time() + kernel.getDuration();
						current.getK().setStart_time(start_t);
						current.getK().setEnd_time(end_t); // update the end_time first, for sorting in the next step
						if(current.getK().getEnd_time() > issuingQueries.get(current.getK().getQuery_type()).getReady_time())
							issuingQueries.get(current.getK().getQuery_type())
									.setReady_time(current.getK().getEnd_time());
						
//						System.out.println(current.getPinned()+"---Found active pinned memory copies------------"+mc.getK().getStart_time()
//								+ " --> " + mc.getK().getEnd_time() + ", current: " + current_time);
					}
				}
//				System.out.println("UPDATED::::::start: "+start_t+", end: "+end_t+", client, order: "+kernel.getQuery_type()+":"+kernel.getExecution_order());
//				System.out.println("Active Memcpies ==============================================================================="+active_memcpy_htd);

				if (current.getPinned() != 1) {
					/**
					 * the current memory copy is Pageable memory copy
					 */
					active_memcpy_htd.add(current); // add the current memory copy into the queue
					Collections.sort(active_memcpy_htd);

					ArrayList<Float> interval = new ArrayList<Float>();
					interval.add(start_t);
					for (int i = 0; i < active_memcpy_htd.size(); i++) {
						interval.add(active_memcpy_htd.get(i).getK().getEnd_time());
						active_memcpy_htd.get(i).setExpected_left_time(0.0f);
					}
					for (int i = 1; i < interval.size(); i++) {
						// System.out.println("~~~~~~~~~~~~~~~~update time from "+interval.get(i-1)+" to "+ interval.get(i));
						if (interval.get(i) <= end_t) pre_parallel = active_memcpy_htd.size() - i;
						else pre_parallel = active_memcpy_htd.size() - i + 1;

						parallel = active_memcpy_htd.size() - i + 1;

						for (int j = i - 1; j < active_memcpy_htd.size(); j++) {
							float ll = 2.5f + randQuery.nextFloat() * 0.2f;
							// float ll = 2.0f;
							slow = Math.max(parallel / ll, 1.0f);
							if (active_memcpy_htd.get(j).getNew_inst() == 1)
								raw_slow = 1.0f;
							else
								raw_slow = Math.max(pre_parallel / ll, 1.0f);

							raw_time = (interval.get(i) - interval.get(i - 1))
									/ raw_slow;
							// if(active_memcpy_htd.get(j).getK().getQuery_type() == 0)
							// 		System.out.println("memcpy: "+j+" of "+active_memcpy_htd.size()+"--------append addition: "+raw_time*slow);
							
							active_memcpy_htd.get(j).setExpected_left_time(
									active_memcpy_htd.get(j)
											.getExpected_left_time()
											+ raw_time
											* slow + 0.02f);
						}
					}

					for (int i = 0; i < active_memcpy_htd.size(); i++) {
						active_memcpy_htd.get(i).getK()
								.setEnd_time(start_t+ active_memcpy_htd.get(i).getExpected_left_time());
/*						
						if (active_memcpy_htd.get(i).getK().getQuery_type() == 0)
							System.out.println("add new-------------duration updated to: "
									+ (active_memcpy_htd.get(i).getK()
											.getEnd_time() - active_memcpy_htd
											.get(i).getK().getStart_time()));
*/
						if (active_memcpy_htd.get(i).getK().getEnd_time() > issuingQueries
								.get(active_memcpy_htd.get(i).getK()
										.getQuery_type()).getReady_time()) {
							
							issuingQueries.get(active_memcpy_htd.get(i).getK()
										.getQuery_type()).setReady_time(
											active_memcpy_htd.get(i).getK()
													.getEnd_time());
						}
					}

					for (int i = 0; i < active_memcpy_htd.size(); i++) {
						if (active_memcpy_htd.get(i).getNew_inst() == 1) {
							ret = active_memcpy_htd.get(i).getK().getEnd_time()	- start_t;
							active_memcpy_htd.get(i).setNew_inst(0);
							break;
						}
					}
				} else if (current.getPinned() == 1) {
					Collections.sort(active_memcpy_htd);
//					System.out.println("Add a pinned memory copy!!!!!"+active_memcpy_htd.size()+", duration is: "+current.getK().getDuration());
					/**
					 * the current added memcpy is a pinned memory copy, only
					 * need to update the existing pageable memcpies
					 */
					for (int i = 0; i < active_memcpy_htd.size(); i++) {						
						if(active_memcpy_htd.get(i).getPinned() != 1) {
							active_memcpy_htd.get(i).getK().setEnd_time(active_memcpy_htd.get(i).getK().getEnd_time() + kernel.getDuration());
							
							/*						
							if (active_memcpy_htd.get(i).getK().getQuery_type() == 0)
								System.out.println(i+"----:duration updated from "+active_memcpy_htd.get(i).getK().getReal_duration() + " ---> "
										+ (active_memcpy_htd.get(i).getK()
												.getEnd_time() - active_memcpy_htd
												.get(i).getK().getStart_time()));
							*/
							
							active_memcpy_htd.get(i).getK().setReal_duration(active_memcpy_htd.get(i).getK().getEnd_time() 
										- active_memcpy_htd.get(i).getK().getStart_time());
						}

						if (active_memcpy_htd.get(i).getK().getEnd_time() > 
								issuingQueries.get(active_memcpy_htd.get(i).getK().getQuery_type()).getReady_time()) {
							issuingQueries.get(active_memcpy_htd.get(i).getK().getQuery_type()).
									setReady_time(active_memcpy_htd.get(i).getK().getEnd_time());
							if(active_memcpy_htd.get(i).getK().getQuery_type()==2)
							System.out.println(active_memcpy_htd.get(i).getK().getExecution_order()+": ready at: "+ active_memcpy_htd.get(i).getK().getEnd_time());
						}
					}

					ret = current.getK().getDuration();
//					ret = start_t - current_time + current.getK().getDuration();
					current.setNew_inst(0);
					active_memcpy_htd.add(current); // add the current memory copy into the queue
				}
			} else if (direction == 2) {
				/**
				 * 1. Sort the active memory copies in the order of completion
				 */
				Collections.sort(active_memcpy_dth);

				/**
				 * 2. Add the current memcpy into the queue
				 */
				MemCpy current = new MemCpy(kernel);

				/**
				 * 3. Remove the completed memory copies
				 */
				for (Iterator<MemCpy> iterator = active_memcpy_dth.iterator(); iterator.hasNext();) {
					MemCpy mc = iterator.next();
					if (Float.compare(mc.getK().getEnd_time(), start_t) <= 0) {
					// Remove the current element from the iterator and the list.
						iterator.remove();
					}
				}
				
				/**
				 * 4. Check whether there are any active pinned memory copies in
				 * the same direction. if there is any active pinned memory
				 * copies, the current memcpy cannot start before the completion
				 * of the pinned memory copy.
				 */
				for (Iterator<MemCpy> iterator = active_memcpy_dth.iterator(); iterator.hasNext();) {
					MemCpy mc = iterator.next();
					if (mc.getPinned() == 1) {
						start_t = mc.getK().getEnd_time();
						end_t = mc.getK().getEnd_time() + kernel.getDuration();
						current.getK().setStart_time(start_t);
						current.getK().setEnd_time(end_t); // update the end_time first, for sorting in the next step
						if(current.getK().getEnd_time() > issuingQueries.get(current.getK().getQuery_type()).getReady_time())
							issuingQueries.get(current.getK().getQuery_type())
									.setReady_time(current.getK().getEnd_time());
						
//						System.out.println(current.getPinned()+"---Found active pinned memory copies------------"+mc.getK().getStart_time()
//								+ " --> " + mc.getK().getEnd_time() + ", current: " + current_time);
					}
				}
//				System.out.println("Active Memcpies ==============================================================================="+active_memcpy_htd);

				if (current.getPinned() != 1) {
					/**
					 * the current memory copy is Pageable memory copy
					 */
					active_memcpy_dth.add(current); // add the current memory copy into the queue
					Collections.sort(active_memcpy_dth);

					ArrayList<Float> interval = new ArrayList<Float>();
					interval.add(start_t);
					for (int i = 0; i < active_memcpy_dth.size(); i++) {
						interval.add(active_memcpy_dth.get(i).getK().getEnd_time());
						active_memcpy_dth.get(i).setExpected_left_time(0.0f);
					}
					for (int i = 1; i < interval.size(); i++) {
						if (interval.get(i) <= end_t) pre_parallel = active_memcpy_dth.size() - i;
						else pre_parallel = active_memcpy_dth.size() - i + 1;

						parallel = active_memcpy_dth.size() - i + 1;

						for (int j = i - 1; j < active_memcpy_dth.size(); j++) {
							float ll = 2.5f + randQuery.nextFloat() * 0.2f;
							// float ll = 2.0f;
							slow = Math.max(parallel / ll, 1.0f);
							if (active_memcpy_dth.get(j).getNew_inst() == 1)
								raw_slow = 1.0f;
							else
								raw_slow = Math.max(pre_parallel / ll, 1.0f);

							raw_time = (interval.get(i) - interval.get(i - 1))
									/ raw_slow;
							
							active_memcpy_dth.get(j).setExpected_left_time(
									active_memcpy_dth.get(j)
											.getExpected_left_time()
											+ raw_time
											* slow + 0.02f);
						}
					}

					for (int i = 0; i < active_memcpy_dth.size(); i++) {
						active_memcpy_dth.get(i).getK()
								.setEnd_time(start_t+ active_memcpy_dth.get(i).getExpected_left_time());
						
						if (active_memcpy_dth.get(i).getK().getEnd_time() > issuingQueries
								.get(active_memcpy_dth.get(i).getK()
										.getQuery_type()).getReady_time()) {
							
							issuingQueries.get(active_memcpy_dth.get(i).getK()
										.getQuery_type()).setReady_time(
											active_memcpy_dth.get(i).getK()
													.getEnd_time());
						}
					}

					for (int i = 0; i < active_memcpy_dth.size(); i++) {
						if (active_memcpy_dth.get(i).getNew_inst() == 1) {
							ret = active_memcpy_dth.get(i).getK().getEnd_time()	- start_t;
							active_memcpy_dth.get(i).setNew_inst(0);
							break;
						}
					}
				} else if (current.getPinned() == 1) {
					Collections.sort(active_memcpy_dth);
//					System.out.println("Add a pinned memory copy!!!!!"+active_memcpy_htd.size()+", duration is: "+current.getK().getDuration());
					/**
					 * the current added memcpy is a pinned memory copy, only
					 * need to update the existing pageable memcpies
					 */
					for (int i = 0; i < active_memcpy_dth.size(); i++) {						
						if(active_memcpy_dth.get(i).getPinned() != 1) {
							active_memcpy_dth.get(i).getK().setEnd_time(active_memcpy_dth.get(i).getK().getEnd_time() + kernel.getDuration());
							
							///*						
							if (active_memcpy_dth.get(i).getK().getQuery_type() == 0)
								System.out.println(i+"----:duration updated from "+active_memcpy_dth.get(i).getK().getReal_duration() + " ---> "
										+ (active_memcpy_dth.get(i).getK()
												.getEnd_time() - active_memcpy_dth
												.get(i).getK().getStart_time()));
							//*/
							
							active_memcpy_dth.get(i).getK().setReal_duration(active_memcpy_dth.get(i).getK().getEnd_time() 
										- active_memcpy_dth.get(i).getK().getStart_time());
						}

						if (active_memcpy_dth.get(i).getK().getEnd_time() > 
								issuingQueries.get(active_memcpy_dth.get(i).getK().getQuery_type()).getReady_time()) {
							issuingQueries.get(active_memcpy_dth.get(i).getK().getQuery_type()).
									setReady_time(active_memcpy_dth.get(i).getK().getEnd_time());
						}
					}

					ret =  current.getK().getDuration();
					current.setNew_inst(0);
					active_memcpy_dth.add(current); // add the current memory copy into the queue
				}
			} else {
				ret = kernel.getDuration();
			}
		} else {
			/**
			 * 1. Sort the active memory copies in the order of completion
			 */
			Collections.sort(active_memcpy);

			/**
			 * 2. Add the current memcpy into the queue
			 */
			MemCpy current = new MemCpy(kernel);

			/**
			 * 3. Remove the completed memory copies
			 */
			for (Iterator<MemCpy> iterator = active_memcpy.iterator(); iterator.hasNext();) {
				MemCpy mc = iterator.next();
				if (Float.compare(mc.getK().getEnd_time(), start_t) <= 0) {
				// Remove the current element from the iterator and the list.
					iterator.remove();
				}
			}
			
			/**
			 * 4. Check whether there are any active pinned memory copies in
			 * the same direction. if there is any active pinned memory
			 * copies, the current memcpy cannot start before the completion
			 * of the pinned memory copy.
			 */
			for (Iterator<MemCpy> iterator = active_memcpy.iterator(); iterator.hasNext();) {
				MemCpy mc = iterator.next();
				if (mc.getPinned() == 1) {
					start_t = mc.getK().getEnd_time();
					end_t = mc.getK().getEnd_time() + kernel.getDuration();
					current.getK().setStart_time(start_t);
					current.getK().setEnd_time(end_t); // update the end_time first, for sorting in the next step
					if(current.getK().getEnd_time() > issuingQueries.get(current.getK().getQuery_type()).getReady_time())
						issuingQueries.get(current.getK().getQuery_type())
								.setReady_time(current.getK().getEnd_time());
					
//					System.out.println(current.getPinned()+"---Found active pinned memory copies------------"+mc.getK().getStart_time()
//							+ " --> " + mc.getK().getEnd_time() + ", current: " + current_time);
				}
			}
//			System.out.println("Active Memcpies ==============================================================================="+active_memcpy_htd);

			if (current.getPinned() != 1) {
				/**
				 * the current memory copy is Pageable memory copy
				 */
				active_memcpy.add(current); // add the current memory copy into the queue
				Collections.sort(active_memcpy);

				ArrayList<Float> interval = new ArrayList<Float>();
				interval.add(start_t);
				for (int i = 0; i < active_memcpy.size(); i++) {
					interval.add(active_memcpy.get(i).getK().getEnd_time());
					active_memcpy.get(i).setExpected_left_time(0.0f);
				}
				for (int i = 1; i < interval.size(); i++) {
					// System.out.println("~~~~~~~~~~~~~~~~update time from "+interval.get(i-1)+" to "+ interval.get(i));
					if (interval.get(i) <= end_t) pre_parallel = active_memcpy.size() - i;
					else pre_parallel = active_memcpy.size() - i + 1;

					parallel = active_memcpy.size() - i + 1;

					for (int j = i - 1; j < active_memcpy.size(); j++) {
						float ll = 2.5f + randQuery.nextFloat() * 0.2f;
						// float ll = 2.0f;
						slow = Math.max(parallel / ll, 1.0f);
						if (active_memcpy.get(j).getNew_inst() == 1)
							raw_slow = 1.0f;
						else
							raw_slow = Math.max(pre_parallel / ll, 1.0f);

						raw_time = (interval.get(i) - interval.get(i - 1))
								/ raw_slow;
						// if(active_memcpy_htd.get(j).getK().getQuery_type() == 0)
						// 		System.out.println("memcpy: "+j+" of "+active_memcpy_htd.size()+"--------append addition: "+raw_time*slow);
						
						active_memcpy.get(j).setExpected_left_time(
								active_memcpy.get(j)
										.getExpected_left_time()
										+ raw_time
										* slow + 0.02f);
					}
				}

				for (int i = 0; i < active_memcpy.size(); i++) {
					active_memcpy.get(i).getK()
							.setEnd_time(start_t+ active_memcpy.get(i).getExpected_left_time());

					if (active_memcpy.get(i).getK().getEnd_time() > issuingQueries
							.get(active_memcpy.get(i).getK()
									.getQuery_type()).getReady_time()) {
						
						issuingQueries.get(active_memcpy.get(i).getK()
									.getQuery_type()).setReady_time(
										active_memcpy.get(i).getK()
												.getEnd_time());
					}
				}

				for (int i = 0; i < active_memcpy.size(); i++) {
					if (active_memcpy.get(i).getNew_inst() == 1) {
						ret = active_memcpy.get(i).getK().getEnd_time()	- start_t;
						active_memcpy.get(i).setNew_inst(0);
						break;
					}
				}
			} else if (current.getPinned() == 1) {
				Collections.sort(active_memcpy);
//				System.out.println("Add a pinned memory copy!!!!!"+active_memcpy_htd.size()+", duration is: "+current.getK().getDuration());
				/**
				 * the current added memcpy is a pinned memory copy, only
				 * need to update the existing pageable memcpies
				 */
				for (int i = 0; i < active_memcpy.size(); i++) {						
					if(active_memcpy.get(i).getPinned() != 1) {
						active_memcpy.get(i).getK().setEnd_time(active_memcpy.get(i).getK().getEnd_time() + kernel.getDuration());
						
						///*						
						if (active_memcpy.get(i).getK().getQuery_type() == 0)
							System.out.println(i+"----:duration updated from "+active_memcpy.get(i).getK().getReal_duration() + " ---> "
									+ (active_memcpy.get(i).getK()
											.getEnd_time() - active_memcpy
											.get(i).getK().getStart_time()));
						//*/
						
						active_memcpy.get(i).getK().setReal_duration(active_memcpy.get(i).getK().getEnd_time() 
									- active_memcpy.get(i).getK().getStart_time());
					}

					if (active_memcpy.get(i).getK().getEnd_time() > 
							issuingQueries.get(active_memcpy.get(i).getK().getQuery_type()).getReady_time()) {
						issuingQueries.get(active_memcpy.get(i).getK().getQuery_type()).
								setReady_time(active_memcpy.get(i).getK().getEnd_time());
					}
				}

				ret = current.getK().getDuration();
				current.setNew_inst(0);
				active_memcpy.add(current); // add the current memory copy into the queue
			}
		}

//		 System.out.println("After*************************************************************************"+ret);

		return ret;
	}

	public float calMemcpyDuration_NoPin(Kernel kernel, float current_time,
			int direction) {
		float ret = 0.0f;
		float slow = 0.0f;
		float raw_slow = 0.0f;
		float raw_time = 0.0f;
		active_memcpy.add(new MemCpy(kernel));

		if (CONSIDER_DIRECTION) {
			if (direction == 1) {
				active_memcpy_htd.add(new MemCpy(kernel));
				float start_t = kernel.getStart_time();
				float end_t = kernel.getStart_time() + kernel.getDuration();
				// System.out.println("Add new kernel:~~~~~~~~~~~~~~~~~~~~ "+start_t+"-->"+end_t);
				// System.out.println("Before==============================================================================="+start_t+"-->"+end_t+", "+active_memcpy_htd);

				for (Iterator<MemCpy> iterator = active_memcpy_htd.iterator(); iterator
						.hasNext();) {
					MemCpy mc = iterator.next();
					if (Float.compare(mc.getK().getEnd_time(), start_t) <= 0) {
						// Remove the current element from the iterator and the
						// list.
						iterator.remove();
					}
				}
				// System.out.println("After ==============================================================================="+active_memcpy_htd);

				Collections.sort(active_memcpy_htd);
				// System.out.println("Active Memcpies ==============================================================================="+active_memcpy_htd);

				// Create time intervals that should be comsidered to update
				// data transfer time, to be fixed
				ArrayList<Float> interval = new ArrayList<Float>();
				interval.add(start_t);
				for (int i = 0; i < active_memcpy_htd.size(); i++) {
					interval.add(active_memcpy_htd.get(i).getK().getEnd_time());
					active_memcpy_htd.get(i).setExpected_left_time(0.0f);
				}
				/*
				 * for(int i = 1; i< interval.size(); i++) {
				 * System.out.println("~~~~~~~~~~~~~~~~update time from "
				 * +interval.get(i-1)+" to "+ interval.get(i));
				 * 
				 * for(int j = i-1; j< active_memcpy.size(); j++) { float slow =
				 * Math.max((active_memcpy.size()-j)/3.0f, 1.0f);
				 * System.out.println
				 * (j+"append addition: "+(interval.get(i)-interval
				 * .get(i-1))*slow);
				 * active_memcpy.get(j).setExpected_left_time(active_memcpy
				 * .get(j).getExpected_left_time() +
				 * (interval.get(i)-interval.get(i-1))*slow); } }
				 */
				for (int i = 1; i < interval.size(); i++) {
					// System.out.println("~~~~~~~~~~~~~~~~update time from "+interval.get(i-1)+" to "+
					// interval.get(i));
					int pre_parallel, parallel;
					if (interval.get(i) <= end_t)
						pre_parallel = active_memcpy_htd.size() - i;
					else
						pre_parallel = active_memcpy_htd.size() - i + 1;

					parallel = active_memcpy_htd.size() - i + 1;

					for (int j = i - 1; j < active_memcpy_htd.size(); j++) {
						float ll = 2.5f + randQuery.nextFloat() * 0.2f;
						// float ll = 2.0f;
						slow = Math.max(parallel / ll, 1.0f);
						if (active_memcpy_htd.get(j).getNew_inst() == 1)
							raw_slow = 1.0f;
						else
							raw_slow = Math.max(pre_parallel / ll, 1.0f);

						raw_time = (interval.get(i) - interval.get(i - 1))
								/ raw_slow;
						// if(active_memcpy_htd.get(j).getK().getQuery_type() ==
						// 0)
						// System.out.println("memcpy: "+j+" of "+active_memcpy_htd.size()+"--------append addition: "+raw_time*slow);
						active_memcpy_htd.get(j).setExpected_left_time(
								active_memcpy_htd.get(j)
										.getExpected_left_time()
										+ raw_time
										* slow + 0.02f);
					}
				}

				for (int i = 0; i < active_memcpy_htd.size(); i++) {
					active_memcpy_htd
							.get(i)
							.getK()
							.setEnd_time(
									start_t
											+ active_memcpy_htd.get(i)
													.getExpected_left_time());
					if (active_memcpy_htd.get(i).getK().getQuery_type() == 0)
						System.out.println("duration updated to: "
								+ (active_memcpy_htd.get(i).getK()
										.getEnd_time() - active_memcpy_htd
										.get(i).getK().getStart_time()));

					if (active_memcpy_htd.get(i).getK().getEnd_time() > issuingQueries
							.get(active_memcpy_htd.get(i).getK()
									.getQuery_type()).getReady_time())
						issuingQueries
								.get(active_memcpy_htd.get(i).getK()
										.getQuery_type()).setReady_time(
										active_memcpy_htd.get(i).getK()
												.getEnd_time());
				}

				for (int i = 0; i < active_memcpy_htd.size(); i++) {
					if (active_memcpy_htd.get(i).getNew_inst() == 1) {
						ret = active_memcpy_htd.get(i).getK().getEnd_time()
								- start_t;
						active_memcpy_htd.get(i).setNew_inst(0);
						break;
					}
				}
			} else if (direction == 2) {
				active_memcpy_dth.add(new MemCpy(kernel));
				float start_t = kernel.getStart_time();
				float end_t = kernel.getStart_time() + kernel.getDuration();
				// System.out.println("Add new kernel:~~~~~~~~~~~~~~~~~~~~ "+start_t+"-->"+end_t);
				// System.out.println("Before==============================================================================="+start_t+", "+active_memcpy);

				for (Iterator<MemCpy> iterator = active_memcpy_dth.iterator(); iterator
						.hasNext();) {
					MemCpy mc = iterator.next();
					if (Float.compare(mc.getK().getEnd_time(), start_t) <= 0) {
						// Remove the current element from the iterator and the
						// list.
						iterator.remove();
					}
				}
				// System.out.println("After ==============================================================================="+active_memcpy);

				Collections.sort(active_memcpy_dth);
				// System.out.println("Active Memcpies ==============================================================================="+active_memcpy);

				ArrayList<Float> interval = new ArrayList<Float>();
				interval.add(start_t);
				for (int i = 0; i < active_memcpy_dth.size(); i++) {
					interval.add(active_memcpy_dth.get(i).getK().getEnd_time());
					active_memcpy_dth.get(i).setExpected_left_time(0.0f);
				}

				for (int i = 1; i < interval.size(); i++) {
					// System.out.println("~~~~~~~~~~~~~~~~update time from "+interval.get(i-1)+" to "+
					// interval.get(i));
					int pre_parallel, parallel;
					if (interval.get(i) <= end_t)
						pre_parallel = active_memcpy_dth.size() - i;
					else
						pre_parallel = active_memcpy_dth.size() - i + 1;

					parallel = active_memcpy_dth.size() - i + 1;

					for (int j = i - 1; j < active_memcpy_dth.size(); j++) {
						float ll = 2.5f + randQuery.nextFloat() * 0.2f;
						// float ll = 2.0f;
						slow = Math.max(parallel / ll, 1.0f);
						if (active_memcpy_dth.get(j).getNew_inst() == 1)
							raw_slow = 1.0f;
						else
							raw_slow = Math.max(pre_parallel / ll, 1.0f);

						raw_time = (interval.get(i) - interval.get(i - 1))
								/ raw_slow;
						active_memcpy_dth.get(j).setExpected_left_time(
								active_memcpy_dth.get(j)
										.getExpected_left_time()
										+ raw_time
										* slow + 0.02f);
					}
				}

				for (int i = 0; i < active_memcpy_dth.size(); i++) {
					active_memcpy_dth
							.get(i)
							.getK()
							.setEnd_time(
									start_t
											+ active_memcpy_dth.get(i)
													.getExpected_left_time());
					if (active_memcpy_dth.get(i).getK().getEnd_time() > issuingQueries
							.get(active_memcpy_dth.get(i).getK()
									.getQuery_type()).getReady_time())
						issuingQueries
								.get(active_memcpy_dth.get(i).getK()
										.getQuery_type()).setReady_time(
										active_memcpy_dth.get(i).getK()
												.getEnd_time());
				}

				for (int i = 0; i < active_memcpy_dth.size(); i++) {
					if (active_memcpy_dth.get(i).getNew_inst() == 1) {
						ret = active_memcpy_dth.get(i).getK().getEnd_time()
								- start_t;
						active_memcpy_dth.get(i).setNew_inst(0);
						break;
					}
				}
			} else {
				ret = kernel.getDuration();
			}
		} else {
			float start_t = kernel.getStart_time();
			float end_t = kernel.getStart_time() + kernel.getDuration();
			// System.out.println("Add new kernel:~~~~~~~~~~~~~~~~~~~~ "+start_t+"-->"+end_t);
			// System.out.println("Before==============================================================================="+start_t+", "+active_memcpy);

			for (Iterator<MemCpy> iterator = active_memcpy.iterator(); iterator
					.hasNext();) {
				MemCpy mc = iterator.next();
				if (Float.compare(mc.getK().getEnd_time(), start_t) <= 0) {
					// Remove the current element from the iterator and the
					// list.
					iterator.remove();
				}
			}
			// System.out.println("After ==============================================================================="+active_memcpy);

			Collections.sort(active_memcpy);
			// System.out.println("Active Memcpies ==============================================================================="+active_memcpy);

			ArrayList<Float> interval = new ArrayList<Float>();
			interval.add(start_t);
			for (int i = 0; i < active_memcpy.size(); i++) {
				interval.add(active_memcpy.get(i).getK().getEnd_time());
				active_memcpy.get(i).setExpected_left_time(0.0f);
			}

			for (int i = 1; i < interval.size(); i++) {
				// System.out.println("~~~~~~~~~~~~~~~~update time from "+interval.get(i-1)+" to "+
				// interval.get(i));
				int pre_parallel, parallel;
				if (interval.get(i) <= end_t)
					pre_parallel = active_memcpy.size() - i;
				else
					pre_parallel = active_memcpy.size() - i + 1;

				parallel = active_memcpy.size() - i + 1;

				for (int j = i - 1; j < active_memcpy.size(); j++) {
					float ll = 2.5f + randQuery.nextFloat() * 0.2f;
					// float ll = 2.0f;
					slow = Math.max(parallel / ll, 1.0f);
					if (active_memcpy.get(j).getNew_inst() == 1)
						raw_slow = 1.0f;
					else
						raw_slow = Math.max(pre_parallel / ll, 1.0f);

					raw_time = (interval.get(i) - interval.get(i - 1))
							/ raw_slow;
					active_memcpy.get(j).setExpected_left_time(
							active_memcpy.get(j).getExpected_left_time()
									+ raw_time * slow + 0.02f);
				}
			}

			for (int i = 0; i < active_memcpy.size(); i++) {
				active_memcpy
						.get(i)
						.getK()
						.setEnd_time(
								start_t
										+ active_memcpy.get(i)
												.getExpected_left_time());
				if (active_memcpy.get(i).getK().getEnd_time() > issuingQueries
						.get(active_memcpy.get(i).getK().getQuery_type())
						.getReady_time())
					issuingQueries.get(
							active_memcpy.get(i).getK().getQuery_type())
							.setReady_time(
									active_memcpy.get(i).getK().getEnd_time());
			}

			for (int i = 0; i < active_memcpy.size(); i++) {
				if (active_memcpy.get(i).getNew_inst() == 1) {
					ret = active_memcpy.get(i).getK().getEnd_time() - start_t;
					active_memcpy.get(i).setNew_inst(0);
					break;
				}
			}
		}

		// System.out.println("After*************************************************************************"+active_memcpy);

		return ret;
	}

	// Todo: Calculate the impact of cudaMalloc on the latency, not a correct
	// method
	public float calMallocDuration(Kernel kernel, float current_time) {
		float ret = 0.0f;

		LinkedList<Integer> active_allocs = new LinkedList<Integer>();

		for (Kernel k : cudaMallocs) {
			float d = Math.max(1.0f, k.getReal_duration());
			if (k.getStart_time() + d >= current_time) {
				// if(k.getStart_time()+d >= current_time +
				// kernel.getDuration()) {
				active_allocs.add(new Integer(cudaMallocs.indexOf(k)));
				// System.out.println("added~~~~~~ start at: "+k.getStart_time()+", curent time: "+current_time);
			}
		}

		if (active_allocs.size() <= 1) {
			ret = kernel.getDuration() * 1.0f;
		} else {
			for (int i = 0; i < active_allocs.size(); i++) {
				// ret = (4.5f) * active_allocs.size(); //calculate new duration
				ret = randQuery.nextFloat() * (8.0f) * active_allocs.size(); // calculate
																				// new
																				// duration

				float delta_time = ret
						- cudaMallocs.get(active_allocs.get(i))
								.getReal_duration();

				if (delta_time < 0)
					delta_time = 0;

				cudaMallocs.get(active_allocs.get(i)).setReal_duration(ret); // update
																				// new
																				// duration

				issuingQueries.get(
						cudaMallocs.get(active_allocs.get(i)).getQuery_type())
						.setReady_time(
								issuingQueries.get(
										cudaMallocs.get(active_allocs.get(i))
												.getQuery_type())
										.getReady_time()
										+ delta_time);
			}
		}

		for (int i = 0; i < cudaMallocs.size(); i++) {
			if (cudaMallocs.get(i).getStart_time() + 200 < current_time) {
				cudaMallocs.remove(i);
			}
		}
		/*
		 * if(kernel.getQuery_type() < 1)
		 * System.out.println("client: "+kernel.getQuery_type
		 * ()+", duration updates from: "
		 * +kernel.getDuration()+" to "+ret+", active is: "
		 * +active_allocs+", current time: "+current_time
		 * +", size: "+active_allocs
		 * .size()+", ret is: "+ret+", start: "+kernel.getStart_time());
		 */
		return ret;
	}

	// Todo: Calculate the impact of cudaFree on the latency
	public float calFreeDuration(Kernel kernel, float current_time) {
		return 1.0f;
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

			// System.out.println(kernel.getQuery_type()+" : "+kernel.getExecution_order());
			/*
			 * 3. mark the query as not sequential constrained to issue kernel
			 */
			issuingQueries.get(kernel.getQuery_type()).setSeqconstraint(false);
			/*
			 * 4. relinquish the computing slots back to the pool
			 */
			// float util_percent = (MPSSim.COMPUTE_SLOTS - available_slots)
			// / (MPSSim.COMPUTE_SLOTS * 1.0f);
			// utilization.add(new SimpleEntry<Float,
			// Float>(kernel.getEnd_time(),
			// util_percent));
			available_slots += kernel.getOccupancy();

			// set the ready time of the next kernel in issuing Queries.
			issuingQueries.get(kernel.getQuery_type()).setReady_time(
					kernel.getEnd_time() + kernel.getSlack_time());
			/*
			  if(kernel.getQuery_type() == 0)
			  System.out.println(kernel.getExecution_order()+" : start: "+kernel.getStart_time()+", end: "
					  +kernel.getEnd_time()+"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~, duration: " +kernel.getReal_duration()
					  +", ready: "+issuingQueries.get(kernel.getQuery_type()).getReady_time()); 
			  else //
			  System.out.println(kernel.getQuery_type()+"----"+kernel.getExecution_order()+
					  " : start: "+kernel.getStart_time()+", end: "
							  +kernel.getEnd_time()+", duration:"+kernel.getReal_duration()
					  +", ready: "+issuingQueries.get(kernel.getQuery_type()).getReady_time()
					  +"--- type: "+kernel.getWarps());
			 */
			  
			/*
			 * 5. add the finished kernel back to the query's finished kernel
			 * queue
			 */
			issuingQueries.get(kernel.getQuery_type()).getFinishedKernelQueue()
					.offer(kernel);

			// Launch the next memcpy tasks
			Kernel k = null;
			if (kernel.getWarps() != 0
					&& issuingQueries.get(kernel.getQuery_type())
							.getKernelQueue().peek() != null
					&& issuingQueries.get(kernel.getQuery_type())
							.getKernelQueue().peek().getWarps() == 0) {

				k = issuingQueries.get(kernel.getQuery_type()).getKernelQueue()
						.poll();
				k.setReal_duration(k.getDuration()); // Initialize the duration
				if (k.getDirection() == 1)
					memCpies_HTD.add(k); // Add to the memcpy host to device
											// queue
				else if (k.getDirection() == 2)
					memCpies_DTH.add(k); // Add to the memcpy device to host
											// queue
					// active_memcpy.add(new MemCpy(kernel)); //record the
					// active_memcpy

				k.setStart_time(issuingQueries.get(kernel.getQuery_type())
						.getReady_time());
				k.setEnd_time(k.getStart_time() + k.getDuration());
				k.setReal_duration(calMemcpyDuration(k,
						issuingQueries.get(kernel.getQuery_type())
								.getReady_time(), k.getDirection())); // Update
																		// the
																		// duration
																		// of
																		// the
																		// kernel

				k.setEnd_time(k.getStart_time() + k.getReal_duration());

				if (k.getDirection() == 2
						&& issuingQueries.get(kernel.getQuery_type())
								.getKernelQueue().isEmpty()) {
					issuingQueries.get(k.getQuery_type()).setEnd_time(
							k.getEnd_time());
					/*
					 * if(k.getQuery_type() == 0)
					 * System.out.println(k.getExecution_order
					 * ()+"------------: start: "+k.getStart_time()
					 * +", end: "+k.getEnd_time()+
					 * "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~, duration: "
					 * +k.getReal_duration()
					 * +", Run: "+issuingQueries.get(k.getQuery_type
					 * ()).getStart_time()+ "-->"
					 * +issuingQueries.get(k.getQuery_type()).getEnd_time()
					 * +", duration is: "
					 * +(issuingQueries.get(k.getQuery_type()).
					 * getEnd_time()-issuingQueries
					 * .get(k.getQuery_type()).getStart_time())); // else //
					 * System.out.println(k.getExecution_order()+" : start: "+k.
					 * getStart_time
					 * ()+", duration:"+k.getReal_duration()+", client: "
					 * +k.getQuery_type());
					 */
				}
			}

			
			// /*
			// 6. if all the kernels from the query have been finished

			if (issuingQueries.get(kernel.getQuery_type()).getKernelQueue()
					.isEmpty()) {

				// 7. set the finish time (global time) of the query
				if (Float.compare(kernel.getEnd_time(),
						issuingQueries.get(kernel.getQuery_type())
								.getEnd_time()) > 0) {
					issuingQueries.get(kernel.getQuery_type()).setEnd_time(
							kernel.getEnd_time());
				}
				// add the duration of the last memcpy from GPU to CPU
				// issuingQueries.get(kernel.getQuery_type()).setEnd_time(kernel.getEnd_time()+15.4f);

				Query query = issuingQueries.get(kernel.getQuery_type());
				// remove the finished query from the issue list

				// 8. if the target query, save the finished query to a list

				// if (query.getQuery_type() < targetQueries.size()) {
				finishedQueries.get(query.getQuery_type()).add(query);
				// }

				// 9. instead of removing the finished query from the issue list
				// mark the indicator of the corresponding issuing slot as
				// invalid
				//
				// issuingQueries.remove(kernel.getQuery_type());
				issueIndicator.set(kernel.getQuery_type(), 0);

				// 10. add the same type of query to the issue list unless the
				// query queue is empty for that type of query

				float duration = kernel.getEnd_time()
						- issuingQueries.get(kernel.getQuery_type())
								.getStart_time();
				
				// System.out.println("Duration is: "+
				// duration+", end is: "+kernel.getEnd_time()+", start is: "+issuingQueries.get(kernel.getQuery_type()).getStart_time());

				if (kernel.getQuery_type() < targetQueries.size()) {
					if (!targetQueries.get(kernel.getQuery_type()).isEmpty()) {
						Query comingQuery = targetQueries.get(
								kernel.getQuery_type()).poll();
						comingQuery.setStart_time(kernel.getEnd_time());
						issuingQueries.set(kernel.getQuery_type(), comingQuery);
						issueIndicator.set(kernel.getQuery_type(), 1);

						comingQuery.setQuery_id(query_id.get(kernel
								.getQuery_type()));
						query_id.set(kernel.getQuery_type(),
								comingQuery.getQuery_id() + 1);
						if (query_load.get(comingQuery.getLoad_id()).get(comingQuery.getQuery_id()) > duration) {
							issuingQueries
									.get(kernel.getQuery_type())
									.setReady_time(kernel.getEnd_time()
													+ query_load.get(comingQuery.getLoad_id()).get(comingQuery.getQuery_id())
													- duration + slacks.get(kernel
															.getQuery_type()));
							issuingQueries
									.get(kernel.getQuery_type())
									.setStart_time(kernel.getEnd_time()
													+ query_load.get(comingQuery.getLoad_id()).get(comingQuery.getQuery_id())
													- duration + slacks.get(kernel
															.getQuery_type()));
							// System.out.println("end time: "+kernel.getEnd_time()+", duration: "+duration+", start: "+
							// issuingQueries.get(kernel.getQuery_type()).getStart_time()+", load: "+target_load.get(comingQuery.getQuery_id()));
						} else {
							issuingQueries.get(kernel.getQuery_type())
									.setReady_time(
											kernel.getEnd_time()
													+ slacks.get(kernel
															.getQuery_type()));
							issuingQueries.get(kernel.getQuery_type())
									.setStart_time(
											kernel.getEnd_time()
													+ slacks.get(kernel
															.getQuery_type()));
						}

					} else {
						int nonempty=0;
						for(int j=0;j<targetQueries.size();j++) {
							if(!targetQueries.get(j).isEmpty()) {
								nonempty ++;
								break;
							}
						}
						
						if(nonempty == 0) {
							COMPLETE_TIME = kernel.getEnd_time();
							System.out.println("target queries stop at: "
									+ COMPLETE_TIME);
							COMPLETE = 1;
						}
					}
				} else {
					if (!backgroundQueries.get(
							kernel.getQuery_type() - targetQueries.size())
							.isEmpty()
					// && COMPLETE == 0
					) {
						Query comingQuery = backgroundQueries.get(
								kernel.getQuery_type() - targetQueries.size())
								.poll();
						// comingQuery.setStart_time(kernel.getEnd_time());

						issuingQueries.set(kernel.getQuery_type(), comingQuery);
						issueIndicator.set(kernel.getQuery_type(), 1);

						comingQuery.setQuery_id(query_id.get(kernel
								.getQuery_type()));
						query_id.set(kernel.getQuery_type(),
								comingQuery.getQuery_id() + 1);

						if (query_load.get(comingQuery.getLoad_id()).get(comingQuery.getQuery_id()) > duration) {
							issuingQueries.get(kernel.getQuery_type())
									.setReady_time(
											kernel.getEnd_time()
													+ query_load.get(comingQuery.getLoad_id()).get(comingQuery
															.getQuery_id())- duration
													+ slacks.get(kernel.getQuery_type()));
							issuingQueries.get(kernel.getQuery_type())
									.setStart_time(
											kernel.getEnd_time()
													+ query_load.get(comingQuery.getLoad_id()).get(comingQuery
															.getQuery_id())
													- duration
													+ slacks.get(kernel
															.getQuery_type()));
							// System.out.println("**********end time: "+kernel.getEnd_time()+", duration: "+duration+", start: "+
							// issuingQueries.get(kernel.getQuery_type()).getStart_time()+", load: "+bg_load.get(comingQuery.getQuery_id()));
						} else {
							issuingQueries.get(kernel.getQuery_type())
									.setReady_time(
											kernel.getEnd_time()
													+ slacks.get(kernel
															.getQuery_type()));
							issuingQueries.get(kernel.getQuery_type())
									.setStart_time(
											kernel.getEnd_time()
													+ slacks.get(kernel
															.getQuery_type()));
						}
					}
				}
				// if (kernel.getQuery_type() >= targetQueries.size())
				// System.out.println("background kernel: "+kernel.getDuration());
			}
			// */
			/*
			 * 11. select the kernel to be issued to the queue
			 */
			if (Detail) {
				System.out.println("At time " + kernel.getEnd_time() + " (ms)"
						+ " finished executing kernel "
						+ kernel.getExecution_order() + " from query "
						+ kernel.getQuery_type());
				System.out
						.println("Enqueue new kernels from the issuing list...");
			}

			float overlapping_time = 0.0f;
			int left_sm = 0;

			float start_time;
			if (kernel.getWarps() == 0) {
				start_time = kernel.getStart_time();
				overlapping_time = 0.0f;
			} else {
				start_time = kernel.getEnd_time();
				int batches = (int) Math.ceil(kernel.getWarps()
						/ (float) (kernel.getWarps_per_batch()));
				if (batches != 0)
					overlapping_time = kernel.getDuration() / batches;

				left_sm = 15 - (int) Math
						.ceil((kernel.getWarps() - (batches - 1)
								* kernel.getWarps_per_batch())
								/ (kernel.getWarps_per_batch() / 15));
				// System.out.println("left over is: "+left_sm+", start time: "+start_time+", overlappting time is: "+overlapping_time+", warps: "+kernel.getWarps()+" , per batch: "+kernel.getWarps_per_batch()+" , batch: "+batches);
				// if(kernel.getQuery_type()==0)
				// System.out.println("kernel duration: "+kernel.getDuration()+", kernel batches: "+batches+", the expected overlapping time is: "+overlapping_time+", kernel is: "+kernel.getQuery_type()+", kernel id: "+kernel.getExecution_order());
			}

			if (!MPS_enabled) {
				overlapping_time = 0.0f;
				left_sm = 0;
			}


			if(kernel.getWarps() !=0)	kernel_elapse_time = kernel.getEnd_time()-overlapping_time;

//			else {
//				kernel_elapse_time = start_time + 0.01f;
//				System.out.println("end time is: "+ kernel.getEnd_time()+"==========elapse time: "+kernel_elapse_time);
//			}
			
			// if(Float.compare(start_time, 50000.0f) < 0 &&
			// Float.compare(start_time, 25000.0f) > 0)
			// System.out.println("start time, duration, overlapping_time: "+start_time+"----"+kernel.getDuration()
			// +"---"+overlapping_time+" !! completed is: "+kernel.getQuery_type());

			enqueueKernel_quan(start_time, overlapping_time, left_sm);
			// enqueueKernel_quan(kernel.getEnd_time(), overlapping_time);
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
				kernel.setWarps_per_batch((int) (new Float(profile[4])
						.floatValue() * 64 * 15));
				kernel.setWarps(new Integer(profile[3]).intValue());
				kernel.setQuery_type(queryType);
				kernel.setExecution_order(i);
				kernel.setSole_start_time(new Float(profile[1]).floatValue());
				kernel.setPinned(new Integer(profile[7]).intValue());
				kernel.setCuda_malloc(new Integer(profile[8]).intValue());
				kernel.setCuda_free(new Integer(profile[9]).intValue());
				kernel.setDirection(new Integer(profile[10]).intValue()); // record
																			// the
																			// data
																			// transfer
																			// direction

				kernelList.add(kernel);
				// System.out.println("Slack time is: "+kernel.getSlack_time());
				i++;
			}

			for (i = 0; i < kernelList.size() - 1; i++) {
				kernelList.get(i).setSlack_time(
						kernelList.get(i + 1).getSole_start_time()
								- (kernelList.get(i).getDuration() + kernelList
										.get(i).getSole_start_time()));
				if (kernelList.get(i).getSlack_time() < 0)
					kernelList.get(i).setSlack_time(0);
				// System.out.println("file name: "+filename+", the slack id is: "+kernelList.get(i).getExecution_order());
				// if(kernelList.get(i).getCuda_malloc() > 0)
				// System.out.println("cuda malloc is: "+kernelList.get(i).getCuda_malloc());
			}

			fileReader.close();
		} catch (Exception ex) {
			System.out.println("Failed to read the file!" + ex.getMessage());
		}
		/*
		 * float whole_duration = 0; for (Kernel k : kernelList) {
		 * whole_duration +=k.getDuration(); }
		 * System.out.println("query is "+filename
		 * +", whole duration is: "+whole_duration);
		 */
		return kernelList;
	}

	static float tg_st = 22386.0f;
	static float bg_st[] = { 150.0f, 1143.0f, 2153.0f, 3155.0f, 4148.0f,
			5154.0f, 6155.0f, 7154.0f };

	public static void main(String[] args) {
		MPSSim mps_sim = new MPSSim();
		/*
		 * manipulate the input
		 */
		preprocess("sim.conf");
/*		
		generateTestCase(args[0], args[1], args[2], new Integer(args[3]).intValue(),
				new Integer(args[4]).intValue(),
				new Integer(args[5]).intValue(),
				new Integer(args[6]).intValue(),
				new Integer(args[7]).intValue(),
				new Integer(args[8]).intValue());
		read_load(args[0], args[1], args[2]);

*/		
/*
		Random random = new Random();
		int start_time = 0;

		int t_variation = getStartVariation(args[0]);
		start_time = random.nextInt(t_variation);

		target_start_point = 20000.0f + getInitTime(args[0])
				+ getWarmupTime(args[0]) + start_time;
		// target_start_point = tg_st;

		System.out.println("TARGET: start time is " + target_start_point);

		// flag = random2.nextFloat();
		t_variation = getStartVariation(args[2]);

		for (int i = 0; i < n_bg; i++) {
			start_time = random.nextInt(t_variation);
			bg_start_points.add(i * 1000.0f + getInitTime(args[1])
					+ getWarmupTime(args[1]) + start_time);
			System.out.println("BG: start time is " + bg_start_points.get(i));
		}
*/
		/*
		 * start to simulate
		 */
		// mps_sim.mps_simulate();
		mps_sim.mps_simulate_quan();

		/*
		 * print out statistics about the MPS simulation
		 */
		calculate_latency(mps_sim);
		// calculate_utilization(mps_sim);
	}

/*	
	private static void read_load_old(String tg_1, String tg_2, String bg) {
		List target_load_obj_1 = null;
		List target_load_obj_2 = null;
		List bg_load_obj = null;
		String TARGET_LOAD_1 = LOAD_PATH + tg_1 + "_target_load.csv";
		String TARGET_LOAD_2 = LOAD_PATH + tg_2 + "_target_load.csv";
		// String BG_LOAD=LOAD_PATH+bg+"_bg_load.csv";
		String BG_LOAD = LOAD_PATH + bg + "_load.csv";

		try {
			CSVReader reader = new CSVReader(new FileReader(TARGET_LOAD_1), ',');
			try {
				target_load_obj_1 = reader.readAll();
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		try {
			CSVReader reader = new CSVReader(new FileReader(TARGET_LOAD_2), ',');
			try {
				target_load_obj_2 = reader.readAll();
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

		target_load.add(new ArrayList<Integer>());
		for (Object lat : target_load_obj_1) {
			String[] target = (String[]) lat;
			for (int i = 0; i < target.length; i++) {
				target_load.get(0).add(new Integer(target[i]));
//				target_load_1.add(new Integer(target[i]));
			}
		}
		
		target_load.add(new ArrayList<Integer>());
		for (Object lat : target_load_obj_2) {
			String[] target = (String[]) lat;
			for (int i = 0; i < target.length; i++) {
				target_load.get(1).add(new Integer(target[i]));
//				target_load_2.add(new Integer(target[i]));
			}
		}
		
		bg_load.add(new ArrayList<Integer>());
		for (Object lat : bg_load_obj) {
			String[] target = (String[]) lat;
			for (int i = 0; i < target.length; i++) {
				bg_load.get(0).add(new Integer(target[i]));
			}
		}

		for(int i=0;i<target_load.size();i++)
			System.out.println("i: "+i+", value: "+target_load.get(i).get(0).intValue());
		for(int i=0;i<bg_load.size();i++)
			System.out.println("BG i: "+i+", value: "+bg_load.get(i).get(0).intValue());
	}
*/

private static void read_load(String name, int type) {
		List target_load_obj_1 = null;
		List target_load_obj_2 = null;
		List bg_load_obj = null;
//		String TARGET_LOAD_1 = LOAD_PATH + tg_1 + "_target_load.csv";
//		String TARGET_LOAD_2 = LOAD_PATH + tg_2 + "_target_load.csv";
		// String BG_LOAD=LOAD_PATH+bg+"_bg_load.csv";
//		String BG_LOAD = LOAD_PATH + bg + "_load.csv";

		String LOAD;

		if(type == 0) LOAD = LOAD_PATH + name + "_target_load.csv";
		else LOAD = LOAD_PATH + name + "_load.csv";
		
		try {
			CSVReader reader = new CSVReader(new FileReader(LOAD), ',');
			try {
				target_load_obj_1 = reader.readAll();
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		query_load.add(new ArrayList<Integer>());
//		target_load.add(new ArrayList<Integer>());
		for (Object lat : target_load_obj_1) {
			String[] target = (String[]) lat;
			for (int i = 0; i < target.length; i++) {
				query_load.get(query_load.size()-1).add(new Integer(target[i]));				
//				target_load.get(0).add(new Integer(target[i]));
			}
		}

//		for(int i=0;i<target_load.size();i++)
//			System.out.println("i: "+i+", value: "+target_load.get(i).get(0).intValue());
//		for(int i=0;i<bg_load.size();i++)
//			System.out.println("BG i: "+i+", value: "+bg_load.get(i).get(0).intValue());
}
	
	/**
	 * TODO calculate the latency distribution for mutiple target queries
	 * 
	 * @param mps_sim
	 */
	private static void calculate_latency_old(MPSSim mps_sim) {
		/*
		 * print out the statistics from the finished query queue, save the
		 * target query latency distribution into a file
		 */
		float accumulative_latency = 0.0f;
		float target_endtime = 0.0f;

		// for (int i = 0; i < finishedQueries.size(); i++) {
		for (int i = 0; i < targetQueries.size(); i++) {
			ArrayList<Float> all_latency = new ArrayList<Float>();
			try {
				BufferedWriter bw = new BufferedWriter(
						new FileWriter(MPSSim.RESULT_PATH
								+ finishedQueries.get(i).peek().getQuery_name()
								+ "-" + n_bg + "-" + bg_name + "-tg-sim.csv", true));

				for (Query finishedQuery : finishedQueries.get(i)) {
					float real_latency;

					if (finishedQueries.get(i).peek().getQuery_name()
							.equals("imc")) {
						real_latency = finishedQuery.getEnd_time()
								- finishedQuery.getStart_time() + 1.2f;
					} else if (finishedQueries.get(i).peek().getQuery_name()
							.equals("dig")) {
						real_latency = finishedQuery.getEnd_time()
								- finishedQuery.getStart_time() + 1.2f;
					} else {
						real_latency = finishedQuery.getEnd_time()
								- finishedQuery.getStart_time() + 0.5f;
						// real_latency =
						// finishedQuery.getEnd_time()-finishedQuery.getStart_time();
					}

					// System.out.println("Query start at: "+finishedQuery.getStart_time()+", end at: "+finishedQuery.getEnd_time());
					// real_latency =
					// finishedQuery.getEnd_time()-finishedQuery.getStart_time();

					accumulative_latency += real_latency;

					all_latency.add(real_latency);

					bw.write(real_latency + "\n");
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

			System.out.println("n_bg is: " + n_bg);

			Collections.sort(all_latency);
			System.out.println(i
					+ ", "
					+ all_latency.size()
					+ ": 50%-ile latency is: "
					+ all_latency.get(all_latency.size() / 2).floatValue()
					+ ", 95%-ile latency is: "
					+ all_latency.get(all_latency.size() * 95 / 100)
							.floatValue());
			// System.out.println("50%-ile latency is: "+all_latency.get(all_latency.size()/2).floatValue()+", 99%-ile latency is: "+all_latency.get(all_latency.size()*99/100).floatValue());

			System.out.println("The average latency for the target query: "
					+ String.format("%.2f", accumulative_latency
							/ finishedQueries.get(i).size()) + "(ms)");
		}
		// if(Detail) {
		if (true) {			
			// calculate latency of background queries
			for (int i = targetQueries.size(); i < finishedQueries.size(); i++) {
				ArrayList<Float> all_latency = new ArrayList<Float>();
				try {
					BufferedWriter bg_bw = new BufferedWriter(
							new FileWriter(MPSSim.RESULT_PATH
									+ finishedQueries.get(0).peek().getQuery_name()
									+ "-" + n_bg + "-" + bg_name + "-bg-sim.csv", true));
					
					for (Query finishedQuery : finishedQueries.get(i)) {
						if (finishedQuery.getEnd_time() <= COMPLETE_TIME) {
							accumulative_latency += finishedQuery.getEnd_time()
									- finishedQuery.getStart_time();
							all_latency.add(finishedQuery.getEnd_time()
									- finishedQuery.getStart_time());
							// bw.write(finishedQuery.getEnd_time()
							// - finishedQuery.getStart_time() + "\n");
							if (finishedQuery.getEnd_time() > target_endtime) {
								target_endtime = finishedQuery.getEnd_time();
							}
							bg_bw.write(all_latency.get(all_latency.size()-1) + "\n");
//							 System.out.println("start: "+finishedQuery.getStart_time()+", end: "+finishedQuery.getEnd_time()
//					 			+", duration: "+(finishedQuery.getEnd_time()-finishedQuery.getStart_time())+", complete: "+COMPLETE_TIME);
						}
					}
					 bg_bw.close();
				} catch (Exception ex) {
					System.out
							.println("Failed to write to the latency.txt, the reason is: "
									+ ex.getMessage());
				}

				Collections.sort(all_latency);
				System.out.println(i
						+ ", "
						+ all_latency.size()
						+ ":  50%-ile latency is: "
						+ all_latency.get(all_latency.size() / 2).floatValue()
						+ ", 95%-ile latency is: "
						+ all_latency.get(all_latency.size() * 95 / 100)
								.floatValue());

				// System.out.println("The average latency for the target query: "
				// + String.format("%.2f", accumulative_latency
				// / finishedQueries.get(i).size()) + "(ms)");
			}
		}
	}
	
	/**
	 * TODO calculate the latency distribution for mutiple target queries
	 * 
	 * @param mps_sim
	 */
	private static void calculate_latency(MPSSim mps_sim) {
		System.out.println("all query type is: "+all_query_type+", all query num is: "+ all_query_num);
		
		String detail_conf_name="";
		for(int i=0; i<all_query_type.size();i++) {
			detail_conf_name += all_query_num.get(i)+"-"+all_query_type.get(i);
			if(i<all_query_type.size()-1) detail_conf_name +="+";
		}
		System.out.println(detail_conf_name);
		/*
		 * print out the statistics from the finished query queue, save the
		 * target query latency distribution into a file
		 */
		float accumulative_latency = 0.0f;
		float target_endtime = 0.0f;

		// for (int i = 0; i < finishedQueries.size(); i++) {
		for (int i = 0; i < targetQueries.size(); i++) {
//			System.out.println(MPSSim.RESULT_PATH
//					+detail_conf_name +"~"+finishedQueries.get(i).get(0).getQuery_name()+"-sim.csv");
			ArrayList<Float> all_latency = new ArrayList<Float>();
			try {
				BufferedWriter bw = new BufferedWriter(
						new FileWriter(MPSSim.RESULT_PATH
								+ detail_conf_name +"~"+finishedQueries.get(i).get(0).getQuery_name()+"-sim.csv", true));

//				BufferedWriter bw = new BufferedWriter(
//						new FileWriter(MPSSim.RESULT_PATH
//								+ finishedQueries.get(i).peek().getQuery_name()
//								+ "-" + n_bg + "-" + bg_name + "-tg-sim.csv", true));

				for (Query finishedQuery : finishedQueries.get(i)) {
					float real_latency;
					// /*
					if (finishedQueries.get(i).peek().getQuery_name()
							.equals("imc")) {
						real_latency = finishedQuery.getEnd_time()
								- finishedQuery.getStart_time() + 1.2f;
					} else if (finishedQueries.get(i).peek().getQuery_name()
							.equals("dig")) {
						real_latency = finishedQuery.getEnd_time()
								- finishedQuery.getStart_time() + 5.0f;
					} else {
						real_latency = finishedQuery.getEnd_time()
								- finishedQuery.getStart_time() + randQuery.nextFloat()*5.0f;
						// real_latency =
						// finishedQuery.getEnd_time()-finishedQuery.getStart_time();
					}
					// */
					// System.out.println("Query start at: "+finishedQuery.getStart_time()+", end at: "+finishedQuery.getEnd_time());
					// real_latency =
					// finishedQuery.getEnd_time()-finishedQuery.getStart_time();

					accumulative_latency += real_latency;

					all_latency.add(real_latency);

					bw.write(real_latency + "\n");
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

			Collections.sort(all_latency);
			System.out.println(finishedQueries.get(i).get(0).getQuery_name()+"-"+i+ ", "+ all_latency.size()
					+ ": 50%-ile latency is: " + all_latency.get(all_latency.size() / 2).floatValue()
					+ ", 95%-ile latency is: " + all_latency.get(all_latency.size() * 95 / 100).floatValue());
			// System.out.println("50%-ile latency is: "+all_latency.get(all_latency.size()/2).floatValue()+", 99%-ile latency is: "+all_latency.get(all_latency.size()*99/100).floatValue());
			/*
			 * print out the average latency for target queries
			 */
//			System.out.println("The average latency for the target query-----: "+finishedQueries.get(i).get(0).getQuery_name()+"-----"
//					+ String.format("%.2f", accumulative_latency
//							/ finishedQueries.get(i).size()) + "(ms)");
		}
		// if(Detail) {
		if (true) {			
			// calculate latency of background queries
			for (int i = targetQueries.size(); i < finishedQueries.size(); i++) {
				ArrayList<Float> all_latency = new ArrayList<Float>();
				try {
					BufferedWriter bg_bw = new BufferedWriter(
							new FileWriter(MPSSim.RESULT_PATH
									+ detail_conf_name +"~"+finishedQueries.get(i).get(0).getQuery_name()+"-sim.csv", true));
/*					
					BufferedWriter bg_bw = new BufferedWriter(
							new FileWriter(MPSSim.RESULT_PATH
									+ finishedQueries.get(0).peek().getQuery_name()
									+ "-" + n_bg + "-" + bg_name + "-bg-sim.csv", true));
*/					
					for (Query finishedQuery : finishedQueries.get(i)) {
						if (finishedQuery.getStart_time()>= 20000.0f && finishedQuery.getEnd_time() <= COMPLETE_TIME) {
							accumulative_latency += finishedQuery.getEnd_time()
									- finishedQuery.getStart_time();
							all_latency.add(finishedQuery.getEnd_time()
									- finishedQuery.getStart_time());
							// bw.write(finishedQuery.getEnd_time()
							// - finishedQuery.getStart_time() + "\n");
							if (finishedQuery.getEnd_time() > target_endtime) {
								target_endtime = finishedQuery.getEnd_time();
							}
							bg_bw.write(all_latency.get(all_latency.size()-1) + "\n");
//							 System.out.println("start: "+finishedQuery.getStart_time()+", end: "+finishedQuery.getEnd_time()
//					 			+", duration: "+(finishedQuery.getEnd_time()-finishedQuery.getStart_time())+", complete: "+COMPLETE_TIME);
						}
					}
					 bg_bw.close();
				} catch (Exception ex) {
					System.out.println("Failed to write to the latency.txt, the reason is: "
									+ ex.getMessage());
				}

				Collections.sort(all_latency);
				System.out.println(finishedQueries.get(i).get(0).getQuery_name()+"-"+i+ ", "+ all_latency.size()
						+ ":  50%-ile latency is: "+ all_latency.get(all_latency.size() / 2).floatValue()
						+ ", 95%-ile latency is: "+ all_latency.get(all_latency.size() * 95 / 100).floatValue());
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
		if (Detail)
			System.out.println(query_name);
		switch (query_name) {
		// DjiNN and Tonic~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "dig":
			return 0.1f;
		case "imc":
			return 0.15f + randQuery.nextInt(2);
		case "face":
			return 0.15f;
		case "pos":
			return 0.15f;
		case "ner":
			return randQuery.nextFloat() * 2;
			// Sirius Suite~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "asr":
			return 0.05f + randQuery.nextFloat();
		case "gmm":
			return 0.05f;
		case "stemmer":
			return 0.15f + randQuery.nextInt(1);
			// Rodinia~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "backprop":
			return randQuery.nextFloat();
		case "bfs":
			return randQuery.nextFloat();
		case "cfd":
			return 0.05f;
		case "dwt2d":
			return randQuery.nextFloat();
		case "gaussian":
			return randQuery.nextFloat();
		case "heartwall":
			return randQuery.nextFloat();
		case "hotspot":
			return randQuery.nextFloat();
		case "hybridsort":
			return 0;
		case "kmeans":
			return randQuery.nextFloat();
		case "lavaMD":
			return randQuery.nextFloat();
		case "leukocyte":
			return 0;
		case "lud":
			return randQuery.nextFloat();
		case "mummergpu":
			return randQuery.nextFloat();
		case "myocyte":
			return 0;
		case "nn":
			return randQuery.nextFloat();
		case "nw":
			return randQuery.nextFloat();
		case "particlefilter":
			return randQuery.nextFloat();
		case "pathfinder":
			return randQuery.nextFloat();
		case "pathfinderPIN":
			return randQuery.nextFloat();
		case "srad":
			return randQuery.nextFloat()*0.5f;
		case "streamcluster":
			return randQuery.nextFloat();
		default:
			System.out
					.println("Slack Time: not recorded------------------------");
		}

		return 0;
	}

	private static float getInitTime(String query_name) {
		if (Detail)
			System.out.println(query_name);
		switch (query_name) {
		// DjiNN and Tonic~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "dig":
			return 800.0f;
		case "imc":
			return 1700.0f;
		case "face":
			return 60.0f + randQuery.nextInt(1);
		case "pos":
			return 20.0f + randQuery.nextInt(1);
		case "ner":
			return 8.0f + randQuery.nextFloat() * 2;
			// Sirius Suite~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "asr":
			return 1420.0f;
		case "gmm":
			return 475.0f;
		case "stemmer":
			return 500.0f + randQuery.nextInt(20);
			// Rodinia~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "backprop":
			return 256.0f;
		case "bfs":
			return 238.0f;
		case "cfd":
			return 1740.0f;
		case "dwt2d":
			return 256.0f;
		case "gaussian":
			return 230.0f;
		case "heartwall":
			return 30.0f;
		case "hotspot":
			return 60.0f;
		case "hybridsort":
			return 270.0f;
		case "kmeans":
			return 1650.0f;
		case "lavaMD":
			return 245.0f;
		case "leukocyte":
			return 240.0f;
		case "lud":
			return 240.0f;
		case "mummergpu":
			return 2650.0f;
		case "myocyte":
			return 245.0f;
		case "nn":
			return 235.0f;
		case "nw":
			return 1150.0f;
		case "particlefilter":
			return 245.0f;
		case "pathfinder":
			return 2900.0f;
		case "pathfinderPIN":
			return 2900.0f;
		case "srad":
			return 290.0f;
		case "streamcluster":
			return 240.0f;
		default:
			System.out
					.println("Init Time: not recorded------------------------");
		}

		return 1.0f;
	}

	private static float getWarmupTime(String query_name) {
		if (Detail)
			System.out.println(query_name);
		switch (query_name) {
		// DjiNN and Tonic~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "dig":
			return 45.0f;
		case "imc":
			return 180.0f;
		case "face":
			return 340.0f;
		case "pos":
			return 7.5f;
		case "ner":
			return 5.0f;
			// Sirius Suite~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "asr":
			return 90.0f;
		case "gmm":
			return 230.0f;
		case "stemmer":
			return 46.0f;
			// Rodinia~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "backprop":
			return 15.0f;
		case "bfs":
			return 60.0f;
		case "cfd":
			return 20.0f;
		case "dwt2d":
			return 100.0f;
		case "gaussian":
			return 56.0f;
		case "heartwall":
			return 10.0f;
		case "hotspot":
			return 5.0f;
		case "hybridsort":
			return 90.0f;
		case "kmeans":
			return 160.0f;
		case "lavaMD":
			return 0.0f;
		case "leukocyte":
			return 32.0f;
		case "lud":
			return 5.0f;
		case "mummergpu":
			return 10.0f;
		case "myocyte":
			return 20.0f;
		case "nn":
			return 3.0f;
		case "nw":
			return 10.0f;
		case "particlefilter":
			return 2.0f;
		case "pathfinder":
			return 5.0f;
		case "pathfinderPIN":
			return 5.0f;
		case "srad":
			return 25.0f;
		case "streamcluster":
			return 18.0f;
		default:
			System.out.println("Warmup Time: not recorded--------------------");
		}

		return 1.0f;
	}

	private static float getMicroDelay(String query_name) {
		if (Detail)
			System.out.println(query_name);
		switch (query_name) {
		// DjiNN and Tonic~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "dig":
//			return 1.1f + randQuery.nextFloat() * 0.1f;
			return 1.0f;
		case "imc":
//			return 1.1f + randQuery.nextFloat() * 0.1f;
			return 1.0f;
		case "face":
			return 1.0f;
		case "pos":
			return 1.0f;
		case "ner":
			return 1.0f;
			// Sirius Suite~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "asr":
			return 1.0f;
		case "gmm":
			return 1.0f;
		case "stemmer":
			return 1.0f;
			// Rodinia~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "backprop":
			return 1.0f;
		case "bfs":
			return 1.0f;
		case "cfd":
			return 1.0f;
		case "dwt2d":
			return 1.0f;
		case "gaussian":
			return 1.0f;
		case "heartwall":
			return 1.0f;
		case "hotspot":
			return 1.0f;
		case "hybridsort":
			return 1.0f;
		case "kmeans":
			return 1.1f+randQuery.nextFloat()*0.1f;
		case "lavaMD":
			return 1.0f;
		case "leukocyte":
			return 1.0f;
		case "lud":
			return 1.0f;
		case "mummergpu":
			return 1.0f;
		case "myocyte":
			return 1.0f;
		case "nn":
			return 1.0f;
		case "nw":
			return 1.0f;
		case "particlefilter":
			return 1.0f;
		case "pathfinder":
			return 1.0f;
		case "pathfinderPIN":
			return 1.0f;
		case "srad":
			return 1.05f+randQuery.nextFloat()*0.1f;
		case "streamcluster":
			return 1.0f;
		default:
			System.out
					.println("Micro Delay: not recorded---------------------------");
		}

		return 1.0f;
	}

	private static int getStartVariation(String query_name) {
		if (Detail)
			System.out.println(query_name);
		switch (query_name) {
		// DjiNN and Tonic~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "dig":
			return 20;
		case "imc":
			return 30;
		case "face":
			return 30;
		case "pos":
			return 20;
		case "ner":
			return 20;
			// Sirius Suite~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "asr":
			return 20;
		case "gmm":
			return 20;
		case "stemmer":
			return 20;
			// Rodinia~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		case "backprop":
			return 20;
		case "bfs":
			return 20;
		case "cfd":
			return 1;
		case "dwt2d":
			return 20;
		case "gaussian":
			return 20;
		case "heartwall":
			return 20;
		case "hotspot":
			return 20;
		case "hybridsort":
			return 10;
		case "kmeans":
			return 1;
		case "lavaMD":
			return 5;
		case "leukocyte":
			return 20;
		case "lud":
			return 20;
		case "mummergpu":
			return 20;
		case "myocyte":
			return 20;
		case "nn":
			return 20;
		case "nw":
			return 20;
		case "particlefilter":
			return 20;
		case "pathfinder":
			return 20;
		case "pathfinderPIN":
			return 50;
		case "srad":
			return 20;
		case "streamcluster":
			return 20;
		default:
			System.out
					.println("Start Variation: not recorded-----------------");
		}

		return 20;
	}

	private static void generateTestCase(String target_1, String target_2, String bg, 
			int tg_num_1, int tg_num_2, int bg_num, int tg_query_num_1, int tg_query_num_2, int bg_query_num) {
		ArrayList<SimConfiguration> targetConfs = new ArrayList<SimConfiguration>();
		ArrayList<SimConfiguration> targetConfs_2 = new ArrayList<SimConfiguration>();
		ArrayList<SimConfiguration> backgroundConfs = new ArrayList<SimConfiguration>();

		SimConfiguration config = new SimConfiguration();
		config.setQueryName(target_1);
		config.setClientNum(tg_num_1);
		n_tg = config.getClientNum();
		tg_name=config.getQueryName();
		config.setQueryNum(tg_query_num_1);
		targetConfs.add(config);
		System.out.println("Target QueryName: " + config.getQueryName()
				+ ", Client Num: " + config.getClientNum() + ", Query num: "
				+ config.getQueryNum());

		SimConfiguration config_2 = new SimConfiguration();
		config_2.setQueryName(target_2);
		config_2.setClientNum(tg_num_2);
		n_tg += config_2.getClientNum();
		config_2.setQueryNum(tg_query_num_2);
		targetConfs_2.add(config_2);

		System.out.println("Target 2 QueryName: " + config_2.getQueryName()
		+ ", Client Num: " + config_2.getClientNum() + ", Query num: "
		+ config_2.getQueryNum());

		SimConfiguration bg_config = new SimConfiguration();
		bg_config.setQueryName(bg);
		bg_config.setClientNum(bg_num);
		bg_config.setQueryNum(bg_query_num);
		n_bg = bg_config.getClientNum();
		bg_name = bg_config.getQueryName();
		backgroundConfs.add(bg_config);
		System.out.println("Background QueryName: " + bg_config.getQueryName()
				+ ", Client Num: " + bg_config.getClientNum() + ", Query num: "
				+ bg_config.getQueryNum());

		schedulingType = "fifo";

		int client_id = 0;
		/*
		 * populate the target queries 1
		 */
		for (SimConfiguration conf : targetConfs) {
			for (int i = 0; i < conf.getClientNum(); i++) {
				LinkedList<Query> targetQueryList = new LinkedList<Query>();
				LinkedList<Query> finishedQueryList = new LinkedList<Query>();
				for (int j = 0; j < conf.getQueryNum(); j++) {
					Query targetQuery = new Query();
					targetQuery.setQuery_type(client_id);
					targetQuery.setQuery_name(conf.getQueryName());
					targetQuery.setLoad_id(0); 			//The first load file
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
				client_id ++;
			}
		}
		
		for (SimConfiguration conf : targetConfs_2) {
			for (int i = 0; i < conf.getClientNum(); i++) {
				LinkedList<Query> targetQueryList = new LinkedList<Query>();
				LinkedList<Query> finishedQueryList = new LinkedList<Query>();
				for (int j = 0; j < conf.getQueryNum(); j++) {
					Query targetQuery = new Query();
					targetQuery.setQuery_type(client_id);
					targetQuery.setQuery_name(conf.getQueryName());
					targetQuery.setLoad_id(1);
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
				client_id ++;
			}
		}
		

		for (SimConfiguration conf : backgroundConfs) {
			for (int i = 0; i < conf.getClientNum(); i++) {
				LinkedList<Query> backgroundQueryList = new LinkedList<Query>();
				LinkedList<Query> finishedBGQueryList = new LinkedList<Query>();

				for (int j = 0; j < conf.getQueryNum(); j++) {
					Query backgroundQuery = new Query();
					backgroundQuery.setQuery_type(client_id);
					backgroundQuery.setQuery_name(conf.getQueryName());
					backgroundQuery.setLoad_id(0);
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
				client_id ++;
			}
		}

		for (int i = 0; i < slacks.size(); i++)
			System.out.println(slacks.get(i));
	}

	/**
	 * Read the simulation configuration and generate queries of different types
	 * 
	 * @param simConf
	 *            the simulation configuration file
	 */
	private static void preprocess(String simConf) {
		int start_time = 0;
		int t_variation = 0;
		Random random = new Random();

		ArrayList<SimConfiguration> targetConfs = null;
		ArrayList<SimConfiguration> backgroundConfs = null;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					MPSSim.CONFIG_PATH + simConf));
			
			System.out.println("simulator confiuratin directory is: "+(MPSSim.CONFIG_PATH + simConf));
			String line;
			targetConfs = new ArrayList<SimConfiguration>();
			backgroundConfs = new ArrayList<SimConfiguration>();
						
			/*
			 * read the configuration for target queries, the configuration
			 * format is (query name, client number, query number)
			 */
			if ((line = reader.readLine()) != null) {
				String[] confs = line.split(",");
				System.out.println("confs length: " + confs.length + " confs: "
						+ confs[0]);

				for (int i = 0; i < confs.length; i += 3) {
					SimConfiguration config = new SimConfiguration();
					config.setQueryName(confs[i]);
					config.setClientNum(new Integer(confs[i + 1]));
					config.setQueryNum(new Integer(confs[i + 2]));
					targetConfs.add(config);
					System.out.println("Target QueryName: "
							+ config.getQueryName() + ", Client Num: "
							+ config.getClientNum() + ", Query num: "
							+ config.getQueryNum());
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
					bg_name = config.getQueryName();
					backgroundConfs.add(config);
					System.out.println("Background QueryName: "
							+ config.getQueryName() + ", Client NUm: "
							+ config.getClientNum() + ", Query num: "
							+ config.getQueryNum());
				}
			}
			schedulingType = "fifo";
			/*
			 * read the scheduling policy
			 */
//			if ((line = reader.readLine()) != null) {
//				schedulingType = line;
//			}
			reader.close();
		} catch (Exception ex) {
			System.out
					.println("Failed to read the configuration file, the reason is: "
							+ ex.getMessage());
		}
		
		/*
		 * populate the user-facing queries
		 */
		int client_id = 0;
		int type=1;
		for (SimConfiguration conf : targetConfs) {
			for (int i = 0; i < conf.getClientNum(); i++) {
				LinkedList<Query> targetQueryList = new LinkedList<Query>();
				LinkedList<Query> finishedQueryList = new LinkedList<Query>();
				for (int j = 0; j < conf.getQueryNum(); j++) {
					Query targetQuery = new Query();
					targetQuery.setQuery_type(client_id);
					targetQuery.setQuery_name(conf.getQueryName());
					targetQuery.setLoad_id(client_id); 			//The first load file
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
				client_id ++;
				read_load(conf.getQueryName(), 0);	//read user-facing loads
				
				t_variation = getStartVariation(conf.getQueryName());
				start_time = random.nextInt(t_variation);
				start_points.add(type*20000.0f + i * 1000.0f + getInitTime(conf.getQueryName())
					+ getWarmupTime(conf.getQueryName()) + start_time);
			}
			type++;
			if(conf.getClientNum() !=0) {
				all_query_type.add(conf.getQueryName());
				all_query_num.add(conf.getClientNum());
			}
		}
		
		/*
		 * populate the throughput-oriented queries
		 */
		for (SimConfiguration conf : backgroundConfs) {
			for (int i = 0; i < conf.getClientNum(); i++) {
				LinkedList<Query> backgroundQueryList = new LinkedList<Query>();
				LinkedList<Query> finishedBGQueryList = new LinkedList<Query>();

				for (int j = 0; j < conf.getQueryNum(); j++) {
					Query backgroundQuery = new Query();
					backgroundQuery.setQuery_type(client_id);
					backgroundQuery.setQuery_name(conf.getQueryName());
					backgroundQuery.setLoad_id(client_id);
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
				client_id ++;
				read_load(conf.getQueryName(), 1);			//read throuhput-oriented loads
				
				t_variation = getStartVariation(conf.getQueryName());
				start_time = random.nextInt(t_variation);
//				start_points.add(getInitTime(conf.getQueryName())
				start_points.add(i * 1000.0f + getInitTime(conf.getQueryName())
						+ getWarmupTime(conf.getQueryName()) + start_time);				

			}
			if(conf.getClientNum() !=0) {
				all_query_type.add(conf.getQueryName());
				all_query_num.add(conf.getClientNum());
			}
		}	
/*		
		for(LinkedList<Query> ql: backgroundQueries) {
			t_variation = getStartVariation(ql.get(0).getQuery_name());
			start_time = random.nextInt(t_variation);
			start_points.add(i * 1000.0f + getInitTime(conf.getQueryName())
				+ getWarmupTime(conf.getQueryName()) + start_time);
		System.out.println("BG: start time is " + start_points.get(i));
		}
*/		
/*
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
*/		
	}

	
	/**
	 * Read the simulation configuration and generate queries of different types
	 * 
	 * @param simConf
	 *            the simulation configuration file
	 */
	private static void preprocess_old(String simConf) {
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
				System.out.println("confs length: " + confs.length + " confs: "
						+ confs[0]);

				for (int i = 0; i < confs.length; i += 3) {
					SimConfiguration config = new SimConfiguration();
					config.setQueryName(confs[i]);
					config.setClientNum(new Integer(confs[i + 1]));
					config.setQueryNum(new Integer(confs[i + 2]));
					targetConfs.add(config);
					System.out.println("Target QueryName: "
							+ config.getQueryName() + ", Client NUm: "
							+ config.getClientNum() + ", Query num: "
							+ config.getQueryNum());
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
					bg_name = config.getQueryName();
					backgroundConfs.add(config);
					System.out.println("Background QueryName: "
							+ config.getQueryName() + ", Client NUm: "
							+ config.getClientNum() + ", Query num: "
							+ config.getQueryNum());
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
