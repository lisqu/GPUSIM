package edu.umich.clarity;

import java.util.Comparator;

public class KernelComparator<T> implements Comparator<T> {
	public int compare(Object o1, Object o2) {
		Kernel kernel1 = (Kernel) o1;
		Kernel kernel2 = (Kernel) o2;
		float result = kernel1.getEnd_time() - kernel2.getEnd_time();
		if (result > 0) {
			return 1;
		} else if (result < 0) {
			return -1;
		}
		return 0;
	}

}
