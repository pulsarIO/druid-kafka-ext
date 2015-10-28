/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.api;

import java.util.List;

import com.ebay.pulsar.druid.firehose.kafka.support.exceptions.KafkaPartitionReaderException;

import kafka.message.MessageAndMetadata;


/**
 *@author qxing
 * 1. Read event
 * 2. Manage read offset
 * 3. Manage commit offset
 * 4. Be aware of group, topic, partition information.
 **/
public interface PartitionReader extends Comparable<PartitionReader>{
	public PartitionReader startFrom(long offset);
	
	public void reinit();
	/**
	 * 	read events.
	 * 
	 * any errors occurred druing the read process are wrapped as KafkaPartitionReaderException which contains the error code
	 * the exception should be processed by consumer.
	 * 
	 * @return
	 * @throws KafkaPartitionReaderException
	 */
	public List<MessageAndMetadata<byte[],byte[]>> readEvents() throws KafkaPartitionReaderException ;
	public boolean readToTheEnd() ;
	public long getLargestOffset() ;
	public long fetchSmallestOffset();
	public long fetchResetOffset(String reset);
	public void commitOffset() throws Exception ;
	public void markCommitOffset();
	public long getBatchSize();
	public void initOffset() throws Exception; 
	public void resetOffset() throws Exception;
	public long getAndSetCommitOffset(long val);
	public boolean isTaken();
	public void release() ;
	public long calcWaitTime() ;
	public long getReadOffset();
	public void setReadOffset(long readOffset);
	public String getGroupId();
	public String getClientId();
	public String getTopic();
	public int getPartition();
	public void onIdle();
	public boolean isOnIdled() ;
	public void clearIdleStats() ;
	public void close() throws Exception ;
	public boolean isClosed();
	public void lost() ;
	public boolean isLost();
}
