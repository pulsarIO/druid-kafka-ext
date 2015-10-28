package lowlevelapi.test;

import java.util.ArrayList;
import java.util.Collection;

public class FullGc {

    private static final Collection<Object> leak = new ArrayList<Object>();
    private static volatile Object sink = null;
    private static String FLAG = "flag";

    // Run with: -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCDetails
    // Notice that all the stop the world pauses coincide with GC pauses

    public static void main(String[] args) throws Exception {
    	long start = System.currentTimeMillis();
    	if("flag".equalsIgnoreCase(FLAG)){
	    	while(true) {
	    		long end = System.currentTimeMillis();
	    		if(end - start < 30*1000){
		            try {
		                leak.add(new byte[1024 * 1024]);
		                sink = new byte[1024 * 1024];
		            } catch(OutOfMemoryError e) {
		            	//System.out.println("clear leak --------------" + System.currentTimeMillis());
		                leak.clear();
		            }
	    		}
	        }
    	}
    }
}
