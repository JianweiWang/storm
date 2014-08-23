package wjw.storm.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FilePrinter {
	String filePath = null;
	public FilePrinter(String filePaht) {
		this.filePath = filePath;
	}
	
//	public void test () {
//		backtype.storm.ui.core.getExecutorCapacity();
//	}
	
	public void print(Object obj) throws Exception {
		FileWriter fw = null;
		if(filePath != null) {
			File file = new File(filePath);
			try {
				fw = new FileWriter(file,true);
				fw.write(obj.toString() + "\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			} else {
				throw new Exception("File: " + filePath + " is null!");
			}
	}
}
