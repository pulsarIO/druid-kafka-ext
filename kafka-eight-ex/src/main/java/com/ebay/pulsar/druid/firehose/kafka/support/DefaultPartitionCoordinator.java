/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ebay.pulsar.druid.firehose.kafka.support.data.RebalanceResult;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 *@author qxing
 * 
 **/
public class DefaultPartitionCoordinator extends BasePartitionCoordinator {
	@Override
	public RebalanceResult calculate(String consumerId, Set<Integer> allPartitions,List<String> allConsumers,Map<String,Set<Integer>> view){
		Set<Integer> allTakenPartitions=Sets.newHashSet(Iterables.concat(view.values()));
		Set<Integer> newPartitions=Sets.difference(allPartitions, allTakenPartitions);
		Collections.sort(allConsumers);
		int thisConsumerIndex=allConsumers.indexOf(consumerId);
		if(thisConsumerIndex<0) throw new RuntimeException("Consumer with id: "+consumerId +" not regist.");
		// calc rebalance count
		int partitionCount = allPartitions.size();
		int minTake = partitionCount / allConsumers.size();
		int left = partitionCount % allConsumers.size();
		int shouldTake = minTake;
		if (thisConsumerIndex < left)
			shouldTake++;
		RebalanceResult rr=new RebalanceResult();
		rr.setShouldTaken(shouldTake);
		
		if(shouldTake>getMyPartitions().size()){
			rr.setIdlePartitions(newPartitions);
		}
		if(shouldTake<getMyPartitions().size()){
			rr.setShouldRelease(getMyPartitions());
		}
		//LOG.log("Coordinating partitions: ["+topic+"] ["+consumerId+"]AllPartitions:"+Joiner.on(",").join(allPartitions));
		//LOG.log("Coordinating partitions: ["+topic+"] ["+consumerId+"]newPartitions:"+Joiner.on(",").join(newPartitions));
		//LOG.log("Coordinating partitions: ["+topic+"] ["+consumerId+"]myPartitions:"+Joiner.on(",").join(getMyPartitions()));
		//LOG.log("Coordinating partitions: ["+topic+"] ["+consumerId+"]shouldTake:"+shouldTake);
		return rr;
	}
}
