/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.api;

import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaConsumerConfig;

/**
 *@author qxing
 * 
 **/
public interface PartitionCoordinatorStrategy {
	/**
	 * Init PartitionCoordinatorStrategy implementor by providing config and AssignorContext.
	 * Usually implementor should keep the config and AssignorContext.
	 * 
	 * @param config
	 * @param ctx
	 */
	public void init(KafkaConsumerConfig config, String topic,String groupId,String consumerId) ;
	/**
	 * do coordinate.
	 * what coordinate will do are below:
	 * 
	 * 1. check whether there are over-taken partitions.
	 *     release them if exists.
	 * 2. check whether there are un-taken partitions
	 * 		take them according to the strategy.
	 * 
	 * @param ctx
	 */
	public void coordinate(AssignorContext ctx);
	/**
	 * after the coordinator takes a partition, do some extra process for the partition.
	 * e.g.: save this partition for extra use.
	 * 
	 * @param topic
	 * @param partition
	 */
	public void afterTakePartition(String topic, int partition);
	/**
	 * when the coordinator want to take a partition,
	 * do some extra process for this partition 
	 * once this return true, the coordinator will take the partition
	 * once this return false, the coordinator give up taking the partition.
	 * 
	 * @param topic
	 * @param partition
	 * @return
	 */
	public boolean beforeTakePartition(String topic,int partition);
}
