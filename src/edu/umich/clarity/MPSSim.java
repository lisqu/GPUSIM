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

	public static final String TARGET_LOAD="input/load/target_load.csv";
	public static final String BG_LOAD="input/load/bg_load.csv";
	
	public static ArrayList<LinkedList<Query>> targetQueries;
	public static ArrayList<LinkedList<Query>> backgroundQueries;

	private static ArrayList<LinkedList<Query>> finishedQueries;
	private static ArrayList<Map.Entry<Float, Float>> utilization;

	private static List target_load = null;
	private static List bg_load=null;
	
	private ArrayList<Query> issuingQueries;
	private ArrayList<Integer> issueIndicator;
	private Queue<Kernel> kernelQueue;

	public static int n_bg;
	public static int n_tg;
	public static String bg_name;
	private final static boolean Detail = false;
	
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
		}
		for (LinkedList<Query> queries : backgroundQueries) {
			issuingQueries.add(queries.poll());
		}
		for (int i = 0; i < (issuingQueries.size()); i++) {
			issueIndicator.add(1);
		}
//		enqueueKernel(0.0f);
		enqueueKernel_quan(0.0f,0.0f);
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
		issuingQueries.get(chosen_query).setReady_time(10000000.0f);
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
	private void enqueueKernel_quan(float elapse_time, float overlapping_time) {
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
						kernel.setStart_time(elapse_time-overlapping_time);

						kernel.setEnd_time(kernel.getDuration() + elapse_time + MPSSim.KERNEL_SLACK);
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
						issuingQueries.get(kernel.getQuery_type()).setReady_time(kernel.getEnd_time()+1.0f);
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
			int batches = (int) Math.ceil(kernel.getWarps()/(float)(kernel.getWarps_per_batch()));
			if(batches != 0) overlapping_time = kernel.getDuration()/batches;

			enqueueKernel_quan(kernel.getEnd_time(), overlapping_time);
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
		preprocess("sim.conf");
		
		read_load();
		/*
		 * start to simulate
		 */
//		mps_sim.mps_simulate();
		mps_sim.mps_simulate_quan();

		/*
		 * print out statistics about the MPS simulation
		 */
		calculate_latency(mps_sim);
		calculate_utilization(mps_sim);
	}

	private static void read_load(){
		try {
			CSVReader reader = new CSVReader(new FileReader(TARGET_LOAD), ',');
			try {
				target_load = reader.readAll();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			CSVReader reader = new CSVReader(new FileReader(BG_LOAD), ',');
			try {
				bg_load = reader.readAll();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		ArrayList<Float> all_latency = new ArrayList<Float>();
		
		float accumulative_latency = 0.0f;
		float target_endtime = 0.0f;

		for (int i = 0; i < finishedQueries.size(); i++) {
			try {
				BufferedWriter bw = new BufferedWriter(new FileWriter(
						MPSSim.CONFIG_PATH
								+ "sim-"+finishedQueries.get(i).peek().getQuery_name()
								+ "-" + n_bg + "-" + bg_name + ".csv"));
//								+ "+" + n_bg + "+" + bg_name + "+" + schedulingType + ".csv"));				
//								+ "+" + n_bg + "+" + bg_name + "-" + schedulingType + "-"
//								+ "latency.csv"));
				bw.write("end_to_end\n");
				for (Query finishedQuery : finishedQueries.get(i)) {
					accumulative_latency += finishedQuery.getEnd_time()
							- finishedQuery.getStart_time();
					all_latency.add(finishedQuery.getEnd_time()-finishedQuery.getStart_time());
//					System.out.println("start: "+finishedQuery.getStart_time()+", end: "+finishedQuery.getEnd_time());
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
			System.out.println("50%-ile latency is: "+all_latency.get(all_latency.size()/2).floatValue()+", 99%-ile latency is: "+all_latency.get(all_latency.size()*99/100).floatValue());
//			System.out.println("99%-ile latency is: "+all_latency.get(all_latency.size()*99/100).floatValue());
			/*
			 * print out the average latency for target queries
			 */
			System.out.println("The average latency for the target query: "
					+ String.format("%.2f", accumulative_latency
							/ finishedQueries.get(i).size()) + "(ms)");
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
				backgroundQueries.add(backgroundQueryList);
			}
		}
	}
}
