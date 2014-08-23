package wjw.storm.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class TopologyBoltProcessTimeHashMap {
	private String bname;
	private double ptime;
	private ConcurrentHashMap<String, HashMap<String,BoltProcessTime>> bmap; 
	public TopologyBoltProcessTimeHashMap() {
		bmap = new ConcurrentHashMap<String,HashMap<String,BoltProcessTime>>();
	}
	public ConcurrentHashMap<String, HashMap<String,BoltProcessTime>> getTBMap() {
		return bmap;
	}
	public synchronized void put (String tname,BoltProcessTime bpt) {
		bname = bpt.getBname();
		if(bmap.get(tname) != null) {
			bmap.get(tname).put(bname, bpt);
		} else {
			HashMap<String,BoltProcessTime> hm = new HashMap<String,BoltProcessTime>();
			hm.put(bname, bpt);
			bmap.put(tname, hm);
		}
		
	}
	public BoltProcessTime getMinTimeBolt (String tname) throws Exception {
		Iterator<String> iter = bmap.keySet().iterator();
		BoltProcessTime bpt = null, tmp = null;
		HashMap<String,BoltProcessTime> tmpHM = null;
		while(iter.hasNext()) {
			String key = iter.next();
			if(key.equals(tname) ) {
				tmpHM = bmap.get(key);
				Iterator<String> b_iter = tmpHM.keySet().iterator();
				while(b_iter.hasNext() ) {
					String b_name = b_iter.next();
					if(!(b_name.equals("__acker"))) {
						bpt = tmpHM.get(b_name);
						break;
					}
					
				}
				while(b_iter.hasNext()) {
					tmp = tmpHM.get(b_iter.next());
					if(bpt.getPtime() > tmp.getPtime() && !tmp.getBname().equals("__acker")) {
						bpt = tmp;
					}
				}
				break;
			}
		}
		if(bpt == null)
			throw new Exception("There is no bolt!");
		else
			return bpt;
	}
	public String getTopologyName(BoltProcessTime bpt) {
		Iterator<String> iter = bmap.keySet().iterator();
		while(iter.hasNext()) {
			String key = iter.next();
			if(bmap.get(key).equals(bpt)){
				return key;
			}
		}
		return null;
	}
	public HashMap getBoltParallisms (String tname) {
		HashMap<String,Integer> pmap = new HashMap<String,Integer>();
		BoltProcessTime p = null;
		int baseParallism ;
		try {
			p = getMinTimeBolt(tname);
			double minTime = p.getPtime();
			baseParallism = new StormMonitor().getBoltParallism(tname, p.getBname());
			if (p != null){
				Iterator<String> iter = bmap.keySet().iterator();
				String key;
				while(iter.hasNext()){
					key = iter.next();
					if(key.equals(tname)){
						Iterator<String> b_iter = bmap.get(key).keySet().iterator();
						while(b_iter.hasNext()) {
							BoltProcessTime b = bmap.get(key).get(b_iter.next());
							pmap.put(b.getBname(), (int )((b.getPtime() / minTime) +1));	
						}
						break;
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return pmap;
	}
	@Override
	public String toString() {
		return bmap.toString();
	}
	public static void main(String args[]) {
		//BoltProcessTimeHashMap bpm = new BoltProcessTimeHashMap();
		System.out.println();
	}
}
