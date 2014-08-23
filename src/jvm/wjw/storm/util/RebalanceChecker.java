package wjw.storm.util;

import java.util.HashMap;

public class RebalanceChecker {
	private HashMap<String,MyConcurrentQueue> throughputMap;
	private String tname;
	public RebalanceChecker(String tname , HashMap<String,MyConcurrentQueue> throughputMap) {
		this.throughputMap = throughputMap;
		this.tname = tname;
	}
	public boolean checkGood () {
		if(throughputMap.get(tname).getTend() == 1) {
			return true;
		} else {
			return false;
		}
	}
}
