/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.data;

import java.util.Properties;

import com.ebay.pulsar.druid.firehose.kafka.support.util.PropertiesUtils;

import kafka.consumer.ConsumerConfig;

/**
 * This is the config bean for the kafka consumer.
 * 
 * Below is a configuration example.
 * 
 * 
 * @author qxing
 * 
 */
public class KafkaConsumerConfig extends ConsumerConfig{
	private int rebalanceableWaitInMs = 15000;
	private int retryTimes=3;
	private int sleepMsBetweenRetries=500;
	public KafkaConsumerConfig(Properties originalProps) {
		super(originalProps);
		rebalanceableWaitInMs=PropertiesUtils.getInt(originalProps, "rebalance.first.wait.ms", 15000);
		retryTimes=PropertiesUtils.getInt(originalProps, "zookeeper.retry.times", 3);
		sleepMsBetweenRetries=PropertiesUtils.getInt(originalProps, "zookeeper.retry.backoff.ms", 500);
	}
	public int getRebalanceableWaitInMs() {
		return rebalanceableWaitInMs;
	}

	public void setRebalanceableWaitInMs(int rebalanceableWaitInMs) {
		this.rebalanceableWaitInMs = rebalanceableWaitInMs;
	}
	public int retryTimes(){
		return retryTimes;
	}
	public int sleepMsBetweenRetries(){
		return sleepMsBetweenRetries;
	}
}
