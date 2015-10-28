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
public class KafkaZKException extends RuntimeException {
	private static final long serialVersionUID = -7507119817636549797L;
	public KafkaZKException(String msg){
		super(msg);
	}
	public KafkaZKException(Throwable t){
		super(t);
	}
	public KafkaZKException(String msg,Throwable t){
		super(msg,t);
	}
}
