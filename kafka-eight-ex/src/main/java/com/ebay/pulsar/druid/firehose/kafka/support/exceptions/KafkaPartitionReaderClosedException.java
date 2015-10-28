/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.exceptions;
/**
 *@author qxing
 * 
 **/
public class KafkaPartitionReaderClosedException extends RuntimeException {

	private static final long serialVersionUID = 4708382343629078751L;

	public KafkaPartitionReaderClosedException(String msg){
		super(msg);
	}
}
