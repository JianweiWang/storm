package wjw.storm.util;

public class BoltProcessTime {

	private String bname;
	private double ptime;
	public BoltProcessTime(String bname, double ptime) {
		this.setBname(bname);
		this.setPtime(ptime);
	}
	public double getPtime() {
		return ptime;
	}
	public void setPtime(double ptime) {
		this.ptime = ptime;
	}
	public String getBname() {
		return bname;
	}
	public void setBname(String bname) {
		this.bname = bname;
	}
	@Override
	public String toString() {
		return "[ BoltName: " + bname + " ProcessTime: " + ptime + " ]"; 
	}
}
