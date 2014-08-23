package wjw.storm.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import clojure.lang.RT;
import clojure.lang.Var;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.ui.test;
//import backtype.storm.ui.core;

class MyTask extends TimerTask {

	@Override
	public void run() {
		StormMonitor sm = new StormMonitor();
		List<TopologyInfo> topologies = new ArrayList<TopologyInfo>();
		try {
			ExecutorSummary executor = sm.getExecutor();
			//core c = new core();
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("hello " + System.currentTimeMillis());
		
	}
	
}
class ClojureCaller {
	void print() throws IOException {
		// Load the Clojure script
        RT.loadResourceScript("backtype.storm.ui.test.clj");

        // Get a reference to the hello function
        Var foo = RT.var("backtype.storm.ui.test", "hello");

        // Call it!
       Object result = foo.invoke("World!");
	}
}
public class Test {
	public  void run() {
		Timer timer = new Timer();
		timer.schedule(new MyTask(), 1000,2000);
		
	}
	public static void print() {
		System.out.println("hello world");
	}
	public static void main(String[] args) throws IOException {
		//test t = new test();
		new ClojureCaller().print();
		
	}
	
}
