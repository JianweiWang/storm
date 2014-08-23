package wjw.storm.util;

import backtype.storm.generated.TopologyInfo;

public class RebalanceInfo {

	private int lastParallism;
	private int currentParallism;
	private int lastThroughput;
	//private int currentThroughput;
	private String boltName;
	private String topologyName;
	private long rebalancingTime;
	private boolean isBolt = true;
	private int workLoad;
	public RebalanceInfo(String topologyName,String boltName,int lastParallism, int currentParallism ,int lastThroughput,boolean isBolt,int workLoad) {
		this.lastParallism = lastParallism;
		this.currentParallism = currentParallism;
		this.lastThroughput = lastThroughput;
		this.boltName = boltName;
		this.topologyName = topologyName;
		this.setRebalancingTime(System.currentTimeMillis());
		this.setBolt(isBolt);
		this.setWorkLoad(workLoad);
		
	}
	
	public RebalanceInfo(String topologyName,String boltName,int lastParallism, int currentParallism ,int lastThroughput,int workLoad) {
		this.lastParallism = lastParallism;
		this.currentParallism = currentParallism;
		this.lastThroughput = lastThroughput;
		this.boltName = boltName;
		this.topologyName = topologyName;
		this.setRebalancingTime(System.currentTimeMillis());
		//this.setBolt(isBolt);
		this.setWorkLoad(workLoad);
		
	}
	public RebalanceInfo setData(String topologyName ,String boltName,int lastParallism, int currentParallism ,int lastThroughput ,int workLoad) {
		setBoltName(boltName);
		setCurrentParallism(currentParallism);
		setLastParallism(lastParallism);
		setLastThroughput(lastThroughput);
		setTopologyName(topologyName);
		setRebalancingTime(System.currentTimeMillis());
		setWorkLoad(workLoad);
		return this;
	}
	public int getLastParallism() {
		return lastParallism;
	}
	public void setLastParallism(int lastParallism) {
		this.lastParallism = lastParallism;
	}
	public int getCurrentParallism() {
		return currentParallism;
	}
	public void setCurrentParallism(int currentParallism) {
		this.currentParallism = currentParallism;
	}
	public int getLastThroughput() {
		return lastThroughput;
	}
	public void setLastThroughput(int lastThroughput) {
		this.lastThroughput = lastThroughput;
	}
//	public int getCurrentThroughput() {
//		return currentThroughput;
//	}
//	public void setCurrentThroughput(int currentThroughput) {
//		this.currentThroughput = currentThroughput;
//	}
	public String getBoltName() {
		return boltName;
	}
	public void setBoltName(String boltName) {
		this.boltName = boltName;
	}
	public String toString() {
		return "[" + topologyName + "****" + boltName + "\t" + lastParallism + "\t" + currentParallism + "\t" + lastThroughput +"]";
	}
	public String getTopologyName() {
		return topologyName;
	}
	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}
	public long getRebalancingTime() {
		return rebalancingTime;
	}
	public void setRebalancingTime(long rebalancingTime) {
		this.rebalancingTime = rebalancingTime;
	}
	public boolean isBolt() {
		return isBolt;
	}
	public void setBolt(boolean isBolt) {
		this.isBolt = isBolt;
	}
	public int getWorkLoad() {
		return workLoad;
	}
	public void setWorkLoad(int workLoad) {
		this.workLoad = workLoad;
	}
}
