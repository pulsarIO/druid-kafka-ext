/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.api;


/**
 *@author qxing
 * 1. Consumer regist/unregist
 * 2. Handle Zookeeper Event(NewSession, Session Timeout, Session Disconnect)
 * 3. Partition rebalance power station.
 * 5. 
 **/
public interface SimpleController {
	/**
	 * create a consumer for topic
	 * 
	 * @param topic
	 * @return
	 */
	public KafkaConsumer createConsumer(String topic);
	/**
	 * Get underlying zookeeper client.
	 * 
	 * @return
	 */
	public ZKConnector<?> getZkConnector() ;
	/**
	 * stop the controller.
	 * once it is stopped, it couldn't do anything unless started again.
	 */
	public void stop() ;
	/**
	 * start the controller. 
	 * once the controller started, it will get ready to do all things. for example, re-balance checking.
	 * 
	 * @throws InterruptedException
	 */
	public void start() throws InterruptedException;
	/**
	 * true after start() invoke successfully.
	 * 
	 * @return
	 */
	public boolean isStarted();
	/**
	 * register the consumer to zookeeper.
	 * 
	 * @param name
	 * @param consumer
	 */
	public void register(String name, KafkaConsumer consumer) ;
	/**
	 * un-register the consumer from zookeeper.
	 * @param name
	 */
	public void unregister(String name);
	/**
	 * 
	 * @return
	 */
	public boolean isRebalanceable() ;
	
}
