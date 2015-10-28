package io.druid.firehose.kafka.support;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 *@author qxing
 * 
 **/
public class SimplePartitioner implements Partitioner {
	 public SimplePartitioner (VerifiableProperties props) {
		 
	    }
	@Override
	public int partition(Object arg0, int arg1) {
		int partition = 0;
        String stringKey = (String) arg0;
        partition=stringKey.hashCode()%4;
       return partition;
	}

}
