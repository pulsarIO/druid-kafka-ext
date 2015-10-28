package io.druid.firehose.kafka.support;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.pulsar.druid.firehose.kafka.support.ConsumerPartitionReader;
import com.ebay.pulsar.druid.firehose.kafka.support.SimpleConsumerController;
import com.ebay.pulsar.druid.firehose.kafka.support.SimpleConsumerEx;
import com.ebay.pulsar.druid.firehose.kafka.support.api.ZKConnector;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaConsumerConfig;

import kafka.message.MessageAndMetadata;

/**
 *@author qxing
 * 
 **/
public class MiddleLevelConsumer {

	private static SimpleConsumerController kafkaController;
	private static ZKConnector zkConnector;
	private static ConsumerPartitionReader reader;
	public static interface B<T>{
	    public int compareX(T o);
	}
	public static class A implements Comparable<A>{

		@Override
		public int compareTo(A o) {
			// TODO Auto-generated method stub
			return 0;
		}


	}
	public static void main(String[] args) throws InterruptedException{
		//String zkConnect="10.64.219.218:2181,10.64.219.219:2181,10.64.219.220:2181";

		Properties prop=new Properties();
		prop.put("zookeeper.connect",	UTGlobal.zkConnect);
		prop.put("zookeeper.connection.timeout.ms", "30000");
		prop.put("zookeeper.session.timeout.ms", "5000");
		prop.put("rebalance.max.retries", "20");
		prop.put("zookeeper.sync.time.ms", "5000");
		prop.put("group.id", UTGlobal.group);
		prop.put("fetch.message.max.bytes", "67108864");
		prop.put("fetch.min.bytes","1048576");
		prop.put("auto.offset.reset", "largest");
		prop.put("auto.commit.enable", "false");
		prop.put("rebalance.backoff.ms","5000");
		prop.put("rebalance.wait.ms","2000");
		prop.put("rebalance.first.wait.ms","2000");
		prop.put("num.consumer.fetchers", "5");
		prop.put("partition.assignment.strategy","com.ebay.pulsar.druid.firehose.kafka.support.DefaultPartitionCoordinator");
		
		TestKafkaMessageSerializer serializer = new TestKafkaMessageSerializer();
		UTGlobal.group.substring(2, 5);
		kafkaController = new SimpleConsumerController(new KafkaConsumerConfig(prop));
		//kafkaController.start();
		final SimpleConsumerEx consumer=kafkaController.createConsumer(UTGlobal.topic);
		
		Thread t1=new Thread(new Runnable(){
			@Override
			public void run() {
					while(true){
						try {
							consumer.markCommitOffset();
							consumer.commitOffset();
							Thread.currentThread().sleep(5000L);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
			}
		});
		t1.start();
		final Iterator<MessageAndMetadata<byte[], byte[]>>  itr1=consumer.iterator();
		final AtomicLong count=new AtomicLong(0);
		Thread running1=new Thread(new Runnable(){
			@Override
			public void run(){
				while(true){
					while(itr1.hasNext()){
						MessageAndMetadata itm=itr1.next();
						
						if(itm!=null){
							count.incrementAndGet();
							try{
							//consumer.commitOffset();
							//if(count.get()%5==0) consumer.markCommitOffset();
							}catch(Exception e){
								System.out.println("Commit Offset Exception:"+e.getMessage());
							}
							//System.out.println("Consumer count="+count.get()+", a.partition="+itm.partition()+",a.offset="+itm.offset());
						}
						//if(itm!=null)
						 //LOG.log("topic="+itm.topic()+",a.partition="+itm.partition()+",a.offset="+itm.offset());;
					}
				}
			}
		});
		running1.start();
		
		
//		
//		Properties prop2=new Properties();
//		prop2.putAll(prop);
//		SimpleConsumerController kafkaController2 = new SimpleConsumerController(new KafkaConsumerConfig(prop));
//		//kafkaController.start();
//		final SimpleConsumerEx consumer2=kafkaController2.createConsumer(UTGlobal.topic2);
//		
//		Thread t2=new Thread(new Runnable(){
//			@Override
//			public void run() {
//					while(true){
//						try {
//							consumer.commitOffset();
//							Thread.currentThread().sleep(30000L);
//						} catch (Exception e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//					}
//			}
//		});
//		final Iterator<MessageAndMetadata<byte[], byte[]>>  itr2=consumer2.iterator();
//		Thread running2=new Thread(new Runnable(){
//			@Override
//			public void run(){
//				while(true){
//					while(itr2.hasNext()){
//						MessageAndMetadata itm=itr2.next();
//						//if(itm!=null)
//						 //System.out.println("topic="+itm.topic()+",a.partition="+itm.partition()+",a.offset="+itm.offset());;
//					}
//				}
//			}
//		});
//		running2.start();
		running1.join();
//		running2.join();
	}

}
