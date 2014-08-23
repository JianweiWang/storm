package wjw.storm.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.Utils;

public class SamplingThread implements Runnable{
	private HashMap<String,MyConcurrentQueue> processTimeMap ;
	private HashMap<String,MyConcurrentQueue> throughputMap;
	private TopologyBoltProcessTimeHashMap tbMap;
	private ConcurrentHashMap rebalancingFlagMap;
	public SamplingThread(HashMap<String,MyConcurrentQueue> pmap,HashMap<String,MyConcurrentQueue> tmap,TopologyBoltProcessTimeHashMap tbMap, ConcurrentHashMap rebalancingFlagMap) {
		this.processTimeMap = pmap;
		throughputMap = tmap;
		this.tbMap = tbMap;
		this.rebalancingFlagMap = rebalancingFlagMap;
	}
	public static void main(String[] args) {
		HashMap<String,MyConcurrentQueue> pmap = new HashMap<String,MyConcurrentQueue>();
		HashMap<String,MyConcurrentQueue> tmap = new HashMap<String,MyConcurrentQueue>();
		TopologyBoltProcessTimeHashMap tbMap = new TopologyBoltProcessTimeHashMap();
		ConcurrentHashMap rebalancingFlagMap = new ConcurrentHashMap();
		SamplingThread st = new SamplingThread(pmap,tmap,tbMap,rebalancingFlagMap);
		new Thread(st).start();
		while(true) {
			Utils.sleep(6000);
			System.out.println(tmap);
			System.out.println(pmap);
		}
	}
	@Override
	public void run() {
		System.out.println("starting sampling.....");
		while(true) {
			Utils.sleep(5000);
			StormMonitor sm = new StormMonitor();
			List<TopologyInfo> t_list = sm.getTopology();
			Iterator<TopologyInfo> t_iter = t_list.iterator();
			TopologyInfo topology;
			Iterator<ExecutorSummary> e_iter;
			ExecutorSummary executor;			
			String tname,bname;
			double bProcessTime = 0;
			long throughput = 0;
			
			while(t_iter.hasNext()) {
				topology = t_iter.next();
				tname = topology.get_name();
				if(rebalancingFlagMap.get(tname) != null)
				  {
						while((Boolean)rebalancingFlagMap.get(tname)) 
						;
				  }
					
				e_iter = topology.get_executors_iterator();
				MyConcurrentQueue queue = processTimeMap.get(tname);
				MyConcurrentQueue queue1 = throughputMap.get(tname);
				while(e_iter.hasNext()) {
					executor = e_iter.next();
					bname = executor.get_component_id();
					if(executor.get_stats().get_specific().is_set_spout()){
						
						bProcessTime = MyUtils.averageDouble(executor.get_stats().get_specific().get_spout().get_complete_ms_avg().get("600").values());
						long uptime = executor.get_uptime_secs();
						throughput = MyUtils.averageInt(executor.get_stats().get_specific().get_spout().get_acked().get("600").values());
						long averageThroughput = (uptime > 600? throughput / 600 : throughput / uptime);
						
						if(queue != null) {
							queue.add(bProcessTime);
						} else {
							queue = new MyConcurrentQueue();
							queue.add(bProcessTime);
							processTimeMap.put(tname, queue);
						}
						
						if(queue1 != null) {
							queue1.add(averageThroughput);
						} else {
							queue1 = new MyConcurrentQueue();
							queue1.add(averageThroughput);
							throughputMap.put(tname, queue1);
						}
					} else {
						bProcessTime = MyUtils.averageDouble(executor.get_stats().get_specific().get_bolt().get_process_ms_avg().get("600").values());
						tbMap.put(tname, new BoltProcessTime(bname,bProcessTime));
					}
				}			
			}		
		}
	}

}
