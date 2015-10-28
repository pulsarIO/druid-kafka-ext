/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.api;

/**
 * AssignorConext contains all the information you need when reassign partitions.
 * at least it should be aware of SimpleController and KafkaConsumer.
 * 
 *@author qxing
 * 
 **/
public interface AssignorContext {
	/**
	 * get ZKConnector.
	 * 
	 * @return
	 */
	public ZKConnector<?> getZkConnector();
	/**
	 * get SimpleController
	 * @return
	 */
	public SimpleController getController() ;
	/**
	 * get KafkaConsumer
	 * 
	 * @return
	 */
	public KafkaConsumer getConsumer();
}
