/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package io.druid.firehose.kafka.support;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestKafkaProducer {

	private ProducerConfig config;
	//private Producer<byte[], byte[]> producer;
	private Producer<String,String> producer;

	private String topic;
	private KafkaMessageSerializer serializer;

	public TestKafkaProducer(String topic, String brokerList,
			KafkaMessageSerializer serializer,String partitioner) {
		this.topic = topic;
		this.serializer = serializer;
		Properties props = new Properties();
		props.put("metadata.broker.list", brokerList);
		props.put("zookeeper.session.timeout.ms", 5000);
		props.put("zookeeper.sync.time.ms", 2000);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
//		props.put("key.serializer.class", "kafka.serializer.DefaultEncoder");
//		props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner.class", partitioner);
        props.put("request.required.acks", "1");
		props.put("compression.codec", "snappy");
//		props.put("request.required.acks", "0");
		props.put("producer.type", "async");
		props.put("client.id", "testProducer");

		config = new ProducerConfig(props);
		//producer = new Producer<byte[], byte[]>(config);
		producer=new Producer<String,String>(config);
		
	}

	public void produce(int count) {
		for (int i = 0; i < count; i++) {
			//TD jsEvent = new TD();
			//jsEvent.setId(i);
			//jsEvent.setName(i+"");
			//byte[] key = serializer.encodeMessage(jsEvent);
			//byte[] message = serializer.encodeMessage(jsEvent);
			//producer.send(new KeyedMessage<byte[], byte[]>(topic, key, message));
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, i+"", "msg"+i);
			producer.send(data);
			
		}
	}

	
	public void close() {
		producer.close();
	}
	
	public class TD{
		int id;
		String name;
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		
	}
}
