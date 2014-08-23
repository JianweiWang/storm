package wjw.storm.util;

import java.util.Collection;
import java.util.Iterator;

public class MyUtils {

	/**
	 * @param args
	 */
	public static double averageDouble(Collection<Double> c) {
		int size = c.size();
		double average ;
		double sum = 0;
		Iterator iter = c.iterator();
		while(iter.hasNext()) {
			sum = sum +((Double)iter.next());
		}
		average = sum / size;
		return average;
	}
	public static long averageInt(Collection<Long> c){
		int size = c.size();
		long average ;
		long sum = 0;
		Iterator iter = c.iterator();
		while(iter.hasNext()) {
			sum = sum + (Long)iter.next();
		}
		average = sum / size;
		return average;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
