package io.druid.firehose.kafka.support;

import java.util.Iterator;
import java.util.Properties;

import com.ebay.pulsar.druid.firehose.kafka.support.ConsumerPartitionReader;
import com.ebay.pulsar.druid.firehose.kafka.support.SimpleConsumerController;

import kafka.message.MessageAndMetadata;

/**
 *@author qxing
 * 
 **/
public class ProducerEx {
	private static SimpleConsumerController kafkaController;
	private static SimpleConsumerController.ZkConnector zkConnector;
	private static ConsumerPartitionReader reader;
	public static void main(String[] args) throws InterruptedException{
		TestKafkaMessageSerializer serializer = new TestKafkaMessageSerializer();
      
        
		TestKafkaProducer producer = new TestKafkaProducer(UTGlobal.topic,
				UTGlobal.brokerList, serializer,"io.druid.firehose.kafka.support.SimplePartitioner");
		//TestKafkaProducer producer2 = new TestKafkaProducer(UTGlobal.topic2,
		//		UTGlobal.brokerList, serializer,"io.druid.firehose.kafka.support.SimpleTwoPartitioner");
		int count=10;
		while(true){
			producer.produce(10);
			//producer2.produce(10);
			//Thread.currentThread().sleep(100L);
			count--;
			System.out.println("Produce 100.");
		}
		

	
	}
}
