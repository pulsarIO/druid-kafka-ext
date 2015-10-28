package io.druid.firehose.kafka.support;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 *@author qxing
 * 
 **/
public class SimpleTwoPartitioner implements Partitioner {
	 public SimpleTwoPartitioner (VerifiableProperties props) {
		 
	    }
	@Override
	public int partition(Object key, int numPartitions) {
		int partition = 0;
        String stringKey = (String) key;
        partition=stringKey.hashCode()%numPartitions;
       return partition;
	}

}
