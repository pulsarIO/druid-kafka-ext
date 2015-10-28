/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package io.druid.firehose.kafka.support;

public class TestKafkaMessageSerializer implements KafkaMessageSerializer {

	@Override
	public byte[] encodeKey(Object event) {
		
		return event.toString().getBytes();
	}

	@Override
	public byte[] encodeMessage(Object event) {
		return event.toString().getBytes();
	}

	@Override
	public Object decode(byte[] key, byte[] message) {
		return new Object();
	}
	
}
