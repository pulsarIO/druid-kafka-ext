/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.data;

import java.util.Set;

import com.google.common.collect.Sets;

/**
 * 
 * refer to <code>BasePartitionCoordinator</code>
 * 
 * 
 * 
 *@author qxing
 * 
 **/
public class RebalanceResult {
	/**
	 * indicate how many partitions should be taken totally after each round rebalance for one consumer
	 */
	private int shouldTaken=-1;
	/**
	 * all the partitions which can be taken by consumer.
	 * coordinator will take partitions from here until one of condition reached: 
	 * 1. <code>shouldTaken</code> == <code>getMyPartitions().size()</code>
	 * 2. all <code>idlePartitions</code> be taken.
	 */
	private Set<Integer> idlePartitions=Sets.newHashSet();
	/**
	 * all the partitions which can be released by consumer.
	 * coordinator will fetch partitions from here to release until one of codition reached.
	 * 1. <code>shouldTaken</code> == <code>getMyPartitions().size()</code>
	 * 2. all <code>shouldRelease</code> be taken. 
	 */
	private Set<Integer> shouldRelease=Sets.newHashSet();
	public int getShouldTaken() {
		return shouldTaken;
	}
	public void setShouldTaken(int shouldTaken) {
		this.shouldTaken = shouldTaken;
	}
	public Set<Integer> getIdlePartitions() {
		return idlePartitions;
	}
	public void setIdlePartitions(Set<Integer> idlePartitions) {
		this.idlePartitions = idlePartitions;
	}
	public Set<Integer> getShouldRelease() {
		return shouldRelease;
	}
	public void setShouldRelease(Set<Integer> shouldRelease) {
		this.shouldRelease = shouldRelease;
	}
	
	
}
