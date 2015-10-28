/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package io.druid.firehose.kafka.support;


public interface KafkaMessageSerializer {
	byte[] encodeKey(Object event);
	
	byte[] encodeMessage(Object event);
	
	Object decode(byte[] key, byte[] message);
}
