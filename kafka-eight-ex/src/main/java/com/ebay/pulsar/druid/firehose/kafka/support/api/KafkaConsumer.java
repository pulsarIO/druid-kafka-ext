/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.api;

import com.ebay.pulsar.druid.firehose.kafka.support.data.ConsumerId;
import com.ebay.pulsar.druid.firehose.kafka.support.exceptions.KafkaPartitionReaderException;


/**
 *@author qxing
 *
 * 1. Manage PartitionAndOffset as well as topic
 * 2. Delegate Read to PartitionReader
 * 3. Manage Offset read and commit, as well as revert.
 * 
 **/
public interface KafkaConsumer {
	/**
	 * get consumer id which register to zookeeper.
	 * 
	 * @return
	 */
	public ConsumerId getConsumerId() ;

	/**
	 * handle all the partition reader exception.
	 */
	void handlePartitionReaderException(PartitionReader reader,KafkaPartitionReaderException e);
	
	/**
	 * do actrual rebalance.
	 */
	void rebalance();
	/**
	 * start ConsumerTask
	 */
	void start();
	/**
	 * stop consumerTask
	 */
	void stop();
	/**
	 * 
	 * @return
	 */
	boolean isRunning();
	/**
	 * commit offset to zookeeper.
	 */
	void commitOffset() throws Exception;
	/**
	 * mark commit offset.
	 * 
	 */
	void markCommitOffset();
	
	public void resetOffset(String topic, String group, int partition,long offset);
	public boolean takePartition(String topic,int partition) throws Exception;
	public void releasePartition(String topic,int partition);
	public String getTopic();
}
