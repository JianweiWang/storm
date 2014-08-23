package wjw.storm.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class DynamicAdaptThread implements Runnable{
	private String tname;
	private HashMap hm;
	private String bname;
	private int parallism = 0;
	public DynamicAdaptThread(String tname, HashMap hm) {
		this.tname = tname;
		this.hm = hm;
	}
	public void findMaxParallism() {
		Iterator iter = hm.keySet().iterator();
		while(iter.hasNext()) {
			String key = (String) iter.next();
			int tmp = (Integer)hm.get(key);
			if (parallism < tmp) {
				bname = key;
				parallism = tmp;
			}
		}
	}
	@Override
	public void run() {
		String cmd = "storm rebalance " + tname + " -e " + bname + "=" + parallism;
		String[] cmds = {"/bin/sh","-c",cmd};
		try {
			Runtime.getRuntime().exec(cmds);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
