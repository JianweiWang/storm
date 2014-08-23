package wjw.storm.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.Utils;

public class MainMonitor {
	
	public HashMap<String,MyConcurrentQueue> processTimeMap ;
	public HashMap<String,MyConcurrentQueue> throughputMap;
	public TopologyBoltProcessTimeHashMap tbMap;
	public ConcurrentHashMap<String,Boolean> rebalancingFlagMap;
	public ExecutorService executor;
	public MainMonitor() {
		this.processTimeMap = new HashMap<String,MyConcurrentQueue>();
		this.throughputMap = new HashMap<String,MyConcurrentQueue>();
		this.rebalancingFlagMap = new ConcurrentHashMap();
		this.tbMap = new TopologyBoltProcessTimeHashMap();
		executor = Executors.newFixedThreadPool(8);
	}
	
	public void init() {
		System.out.println("initing......");
		new Thread(new SamplingThread(processTimeMap,throughputMap,tbMap,rebalancingFlagMap)).start();
		Utils.sleep(10000);
	}
	
	public void dynamicAdapt() {
		System.out.println("dynamicAdaptint....");
		String tname;
		String bname;
		int parallism;
		while(true) {
			Utils.sleep(10000);
			Iterator iter = processTimeMap.keySet().iterator();
			while(iter.hasNext()) {
				tname = (String)iter.next();
				System.out.println("tend: " + processTimeMap.get(tname).getTend());
				if (processTimeMap.get(tname).getTend() == 1) {
					System.out.println("rebalancing...");
					HashMap hm = tbMap.getBoltParallisms(tname);
					rebalancingFlagMap.put(tname,true);
					executor.submit(new DynamicAdaptThread(tname,hm));
					Utils.sleep(5000);
					rebalancingFlagMap.put(tname,false);
				}
			}
		}
	}
	public void getBoltInfo(TopologyBoltProcessTimeHashMap tbpth) {
		StormMonitor sm = new StormMonitor();
		List<TopologyInfo> t_list = sm.getTopology();
		Iterator<TopologyInfo> t_iter = t_list.iterator();
		TopologyInfo topology;
		Iterator<ExecutorSummary> e_iter;
		ExecutorSummary executor;
		
		String tname,bname;
		double bProcessTime = 0;
		
		while(t_iter.hasNext()) {
			topology = t_iter.next();
			tname = topology.get_name();
			e_iter = topology.get_executors_iterator();
			while(e_iter.hasNext()) {
				executor = e_iter.next();
				bname = executor.get_component_id();
				if(executor.get_stats().get_specific().is_set_bolt())
				{
					bProcessTime = MyUtils.averageDouble(executor.get_stats().get_specific().get_bolt().get_process_ms_avg().get("600").values());
					tbpth.put(tname, new BoltProcessTime(bname,bProcessTime));
				}
				
			}			
		}		
	}
	public static void main(String[] args) {
		String tname = "wordcount1000tps";
		//TopologyBoltProcessTimeHashMap tbpth = new TopologyBoltProcessTimeHashMap();
		MainMonitor mm = new MainMonitor();
		mm.init();
		Utils.sleep(100000);
		mm.dynamicAdapt();
		//mm.getBoltInfo(tbpth);
//		try {
//			System.out.println(tbpth.getMinTimeBolt(tname));
//			System.out.println();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		System.out.println(tbpth.getTBMap());
//		System.out.println(tbpth.getBoltParallisms("wordcount1000tps"));
//	}
	}
}
