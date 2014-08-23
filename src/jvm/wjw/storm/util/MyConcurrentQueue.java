package wjw.storm.util;

public class MyConcurrentQueue {
	private Double[] array;
	private int head,tail;
	private int size = 0;
	private boolean full = false;
	public MyConcurrentQueue () {
		array = new Double[10];
		head = 0;
		tail = 0;
	}
	public synchronized void add (double d) {
		if (!full) {
			array[size] = d;
			size ++;
			if (size > 9) {
				full = true;	
			}
		} else {
			for(int i = 1; i <= 9; i++) {
				array[i-1] = array[i];
			}
			array[9] = d;
		}
	}
	public synchronized double getAvg() {
		double sum = 0;
		double avg;
		if (full) {
			for(int i = 0; i < 10; i++) {
				sum += (double)array[i];
			}
			avg = sum / 10;
			return avg;
		} else {
			for(int i = 0; i < size; i++) {
				sum += (double)array[i];
			}
			avg = sum / size;
			return avg;
		}
		
	}
	public synchronized double getTend() {
		Double y[] = array;
		//Double y[] = {-2.0/3, -4.0/3, -6.0/3, -8.0/3, -10.0/3, -12.0/3, -14.0/3, -16.0/3, -18.0/3, -20.0/3};
		Double x[] = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
		double tend = new LineCoefficient().getXc(x, y);
		/*if(tend < -0.1)
			return -1;
		else if(tend > 0.1)
			return 1;
		else return 0;*/
		return tend;
	}
	class LineCoefficient {  
		  
	      
	    /** 
	     * 预测 
	     * @param x 
	     * @param y 
	     * @param i 
	     * @return 
	     */  
	     double estimate( Double[] x , Double[] y , int i ) {  
	        double a = getXc( x , y ) ;  
	        double b = getC( x , y , a ) ;  
	        return a * i + b ;  
	    }  
	      
	    /** 
	     * 计算 x 的系数 
	     * @param x 
	     * @param y 
	     * @return 
	     */  
	     double getXc( Double[] x , Double[] y ){  
	        int n = x.length ;  
	        return ( n * pSum( x , y ) - sum( x ) * sum( y ) )   
	                / ( n * sqSum( x ) - Math.pow(sum(x), 2) ) ;  
	    }  
	      
	    /** 
	     * 计算常量系数 
	     * @param x 
	     * @param y 
	     * @param a 
	     * @return 
	     */  
	     double getC( Double[] x , Double[] y , double a ){  
	        int n = x.length ;  
	        return sum( y ) / n - a * sum( x ) / n ;  
	    }  
	      
	    /** 
	     * 计算常量系数 
	     * @param x 
	     * @param y 
	     * @return 
	     */  
	     double getC( Double[] x , Double[] y ){  
	        int n = x.length ;  
	        double a = getXc( x , y ) ;  
	        return sum( y ) / n - a * sum( x ) / n ;  
	    }  
	      
	     double sum(Double[] ds) {  
	        double s = 0 ;  
	        for( double d : ds ) s = s + d ;  
	        return s ;  
	    }  
	      
	     double sqSum(Double[] ds) {  
	        double s = 0 ;  
	        for( double d : ds ) s = s + Math.pow(d, 2) ;  
	        return s ;  
	    }  
	      
	     double pSum( Double[] x , Double[] y ) {  
	        double s = 0 ;  
	        for( int i = 0 ; i < x.length ; i++ ) s = s + x[i] * y[i] ;  
	        return s ;  
	    }  
	}
	@Override
	public String toString() {
		String str ="" ;
		for (int i = 0; i < 10 ;i++) {
			str += (array[i] + "\t");
		}
		return "MyConcurrentQueue: " + str;
	}
	public static void main(String[] args) {
		MyConcurrentQueue queue = new MyConcurrentQueue();
//		for(int i = 0; i < 15; i++) {
//			queue.add((double) i);
//		}
		System.out.println(queue.getTend());
	}
}
