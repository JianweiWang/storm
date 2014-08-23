package wjw.storm.util;

/*
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportException;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus.Client.Factory;
import backtype.storm.generated.Nimbus;


public class StormMonitor {
	public static TSocket tsocket = null;
	public static TFramedTransport tTransport = null;
	public static TBinaryProtocol tBinaryProtocol = null;
	public static Nimbus.Client client = null;
	public static final String SERVER_IP = "localhost";
	public static final int SERVER_PORT	= 6627;
	public static final int TIMEOUT = 30000;
	public static ClusterSummary cs = null;
	public void startClient() {
		tsocket = new TSocket(SERVER_IP, SERVER_PORT);
		tTransport = new TFramedTransport(tsocket);
		tBinaryProtocol = new TBinaryProtocol(tTransport);
		client = new Nimbus.Client(tBinaryProtocol);
		try {
			
			tTransport.open();
			
			
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		try {
				cs = client.getClusterInfo();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(cs);
	}
	public static void main(String[] args) {
		StormMonitor sm = new StormMonitor();
		sm.startClient();
	}

}*/


/*
 * 监控，统计storm集群中的作业运行信息
 * @author : wjw
 * @date : 2014-03-11
 * 
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.log4j.Logger;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransportException;




import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;



public class StormMonitor {

	public static Logger LOG = Logger.getLogger(StormMonitor.class);    
	
	public static TSocket tsocket = null;
	public static TFramedTransport tTransport = null;
	public static TBinaryProtocol tBinaryProtocol = null;
	public static Nimbus.Client client = null;
	
	//public String host = "202.117.249.19";
	public String host = "10.128.12.213";      //default nimbus host
	public int port = 6627;                    //default nimbus port
	
	public static CopyOnWriteArraySet<String> jobNames = null;    //job names to monit
	
//	public static TimerScheduler ts = null;
	
	
	public StormMonitor(){
		new StormMonitor(host, port);
	}
	
	public StormMonitor(String nimbusHost, int nimbusPort){
		tsocket = new TSocket(nimbusHost, nimbusPort);
		tTransport = new TFramedTransport(tsocket);
		tBinaryProtocol = new TBinaryProtocol(tTransport);
		client = new Nimbus.Client(tBinaryProtocol);
		try {
			tTransport.open();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
		
		jobNames = new CopyOnWriteArraySet<String>();
		
		//ts = new TimerScheduler();
		//if (ts == null)
		//	System.out.println("m.ts is null");
		
	}
	
	/*
	 * get the topology id
	 * 
	 * @param : name topology name
	 * @return : topology id
	 */
	public static String getTopologyId(String name) {
		if (name == null)
			return null;
        try {
        	ClusterSummary summary = client.getClusterInfo();
        	
            for(TopologySummary s : summary.get_topologies()) {
                if(s.get_name().equals(name)) {  
                	
                	String id = s.get_id();
                	LOG.info("Topology " + name + " exists ! " + "id : " + id);
                    return id;
                } 
            }  
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("Topology " + name + " not exists ! ");
        return null;
    }
	
	public  List<TopologyInfo> getTopology() {
		List<TopologyInfo> topologyInfos = new ArrayList<TopologyInfo>();
        try {
        	ClusterSummary summary = client.getClusterInfo();
            for(TopologySummary s : summary.get_topologies()) {
            	String id = s.get_id();
            	TopologyInfo ti = client.getTopologyInfo(id);
            	topologyInfos.add(ti);
                }   
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        
        	return topologyInfos;
        
    }
	public void close(){
		tTransport.close();
	}
	
	public static ArrayList<String> getTopologyIds(String[] names){
		ArrayList<String> rs = new ArrayList<String>();
		for (String name : names){
			rs.add(getTopologyId(name));
		}
		return rs;
	}
	
	/*
	 * stastic the topology info
	 */
	public static HashMap<String, ArrayList<String>> stasticTopologyInfo(String topologyId){
		if (topologyId == null){
			LOG.warn("Topology id is null !");
			return null;
		}
		
		//key : host_component
		//value : emit or transit value
		HashMap<String, ArrayList<String>> rs = new HashMap<String, ArrayList<String>>();
		
		try{
			TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
			
			List<ExecutorSummary> executorSummaries = topologyInfo.get_executors();

			for(ExecutorSummary executorSummary : executorSummaries) {
				String id = executorSummary.get_component_id();
				
				ExecutorStats executorStats = executorSummary.get_stats();
				
				String host = executorSummary.get_host();
				String component = id;
				String host_componet = String.format("%s\t%s", host,component);
				
				
				// 处理 transit 类型的数据 
				if (executorStats.get_transferred().get(":all-time").size() == 0){
					
					if (!rs.containsKey(host_componet)){
						ArrayList<String> tmpArray = new ArrayList<String>();
						tmpArray.add("transit\t" + 0);
						rs.put(host_componet, tmpArray);
					}
					else {
						rs.get(host_componet).add("transit\t" + 0);
					}
				} else {
					
					String tmp = executorStats.get_transferred().get(":all-time").get("default") + "";
					
					if (!rs.containsKey(host_componet)){
						ArrayList<String> tmpArray = new ArrayList<String>();
						tmpArray.add("transit\t" + (tmp.equals("null") ? "0" : tmp));
						rs.put(host_componet, tmpArray);
					} else {
						
						rs.get(host_componet).add("transit\t" + (tmp.equals("null") ? "0" : tmp));
					}
					
				}
				
				//处理 emitted 类型的数据
				if (executorStats.get_emitted().get(":all-time").size() == 0){
					if (!rs.containsKey(host_componet)){
						ArrayList<String> tmpArray = new ArrayList<String>();
						tmpArray.add("emmitted\t" + 0);
						rs.put(host_componet, tmpArray);
					}
					else {
						rs.get(host_componet).add("emitted\t" + 0);
					}
					
				} else {
					String tmp = executorStats.get_emitted().get(":all-time").get("default") + "";
					
					if (!rs.containsKey(host_componet)){
						ArrayList<String> tmpArray = new ArrayList<String>();
						tmpArray.add("emitted\t" + (tmp.equals("null") ? "0" : tmp));
						rs.put(host_componet, tmpArray);
					} else {
						rs.get(host_componet).add("emitted\t" + (tmp.equals("null") ? "0" : tmp));
					}
					
				}
				
			}
			
		}catch(TTransportException e){
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotAliveException e) {
			e.printStackTrace();
		}
		
		return rs;
	}
	
	public ExecutorSummary  getExecutor() throws InterruptedException{
		//StormMonitor m = new StormMonitor();
					List<TopologyInfo> ti = this.getTopology();
					//System.out.println(ti);
					int i = 0;
					while(i < ti.size())
					{
						//System.out.println(i);
						Object[] executor =  ti.get(i).get_executors().toArray();
						//System.out.println(executor.length);
						ExecutorSummary[] executor1 = new ExecutorSummary[executor.length];
						
						for(int j = 0 ; j < executor.length; j ++) {
							executor1[j] = (ExecutorSummary)executor[j];
							//System.out.println(executor1);
							//if(executor1[j].get_component_id().equals("split")) {
								String s = executor1[j].get_component_id().toString();
							//	System.out.println(s);
								if(s.equals("sentenceSplitBolt")) {
//									System.out.println("sentenceSplitBolt");
									//System.out.println(executor1[j].get_stats().get_specific());
									
									return executor1[j];
								}
								
								
							//}
							
						}
						i++;
						//return null;
						
//						int j = 0;
//						while(j < ti.get(i).get_executors_size()) {
//							System.out.println(ti.get(i).get_executors());
//							j++;
//						}
						
					}
					return null;
		
	}
	public int getBoltParallism(String tname, String bname) {
		List <TopologyInfo> tlist ;
		List <ExecutorSummary> elist ;
		TopologyInfo t ;
		tlist = this.getTopology();
		Iterator<TopologyInfo> t_iter = tlist.iterator();
		Iterator<ExecutorSummary> e_iter ;
		int parallism = 0;
		while(t_iter.hasNext()){
			t = t_iter.next();
			if (t.get_name().equals(tname)) {
				elist = t.get_executors();
				e_iter = elist.iterator();
				while(e_iter.hasNext()) {
					ExecutorSummary e = e_iter.next();
					if(e.get_component_id().equals(bname))
						parallism++;
				}
			}
		}
		return parallism;
	}
	public static void main(String[] args) throws InterruptedException{
		StormMonitor m = new StormMonitor();
		System.out.println(m.getExecutor());
		/*List<TopologyInfo> ti = m.getTopology();
					int i = 0;
					while(i < ti.size())
					{
						
						Object[] executor =  ti.get(i).get_executors().toArray();
						ExecutorSummary[] executor1 = new ExecutorSummary[executor.length];
						for(int j = 0 ; j < executor.length; j ++) {
							executor1[j] = (ExecutorSummary)executor[j];
							//if(executor1[j].get_component_id().equals("split")) {
								String s = executor1[j].get_component_id().toString();
								if(s.equals("sentenceSplitBolt")) {
									System.out.println("sentenceSplitBolt");
									System.out.println(executor1[j].get_stats().get_specific());
								}
								
							//}
							
						}
						
//						int j = 0;
//						while(j < ti.get(i).get_executors_size()) {
//							System.out.println(ti.get(i).get_executors());
//							j++;
//						}
						i++;
					}*/
		
	}
	
}
