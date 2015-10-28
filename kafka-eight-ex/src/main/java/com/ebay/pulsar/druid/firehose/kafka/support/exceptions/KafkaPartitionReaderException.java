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
public class KafkaPartitionReaderException extends RuntimeException {
	private static final long serialVersionUID = -5969989670100717318L;
	private short code;
	public KafkaPartitionReaderException(short code){
		super("KafkaPartition Reader Exception.code="+code);
		this.code=code;
	}
	public KafkaPartitionReaderException(short code,String msg){
		super(msg);
		this.code=code;
	}
	public short getCode(){
		return code;
	}
}
