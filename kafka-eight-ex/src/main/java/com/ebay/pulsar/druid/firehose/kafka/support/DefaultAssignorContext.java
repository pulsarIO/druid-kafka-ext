/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.pulsar.druid.firehose.kafka.support.api.AssignorContext;
import com.ebay.pulsar.druid.firehose.kafka.support.api.KafkaConsumer;
import com.ebay.pulsar.druid.firehose.kafka.support.api.SimpleController;
import com.ebay.pulsar.druid.firehose.kafka.support.api.ZKConnector;

/**
 *@author qxing
 * 
 **/
public class DefaultAssignorContext implements AssignorContext {
	protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultAssignorContext.class
			.getName());
		private KafkaConsumer consumer;
		private ZKConnector<?> zkConnector;
		private SimpleController controller;
		
		public DefaultAssignorContext(SimpleController controller, KafkaConsumer consumer){
			this.consumer=consumer;
			this.controller=controller;
			this.zkConnector=controller.getZkConnector();
		}

		public ZKConnector<?> getZkConnector() {
			return zkConnector;
		}

		public SimpleController getController() {
			return controller;
		}
		public KafkaConsumer getConsumer() {
			return consumer;
		}
}
