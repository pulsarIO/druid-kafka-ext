package io.druid.firehose.kafka.support;

import java.util.Iterator;
import java.util.Properties;

import com.ebay.pulsar.druid.firehose.kafka.support.SimpleConsumerController;
import com.ebay.pulsar.druid.firehose.kafka.support.SimpleConsumerEx;
import com.ebay.pulsar.druid.firehose.kafka.support.api.ZKConnector;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaConsumerConfig;

import kafka.message.MessageAndMetadata;

/**
 *@author qxing
 * 
 **/
public class MiddleLevelConsumerInLocal {

	private static SimpleConsumerController kafkaController;
	private static TestZookeeperServer zkServer;
	private static TestKafkaServer kafkaBroker0;
	private static TestKafkaServer kafkaBroker1;	
	
	public static void main(String[] args) throws InterruptedException{
		//String zkConnect="10.64.219.218:2181,10.64.219.219:2181,10.64.219.220:2181";
		try {
			zkServer = new TestZookeeperServer(30000, 2183, 100);
			zkServer.startup();
		} catch (Exception e) {
			e.printStackTrace();
		}
		kafkaBroker0 = new TestKafkaServer("/kafka0/", 9082, 0, UTGlobal.zkConnect, 2);
		kafkaBroker1 = new TestKafkaServer("/kafka1/", 9083, 1, UTGlobal.zkConnect, 2);
		
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
		prop.put("num.consumer.fetchers", "5");
		prop.put("partition.assignment.strategy","io.druid.firehose.kafka.support.DefaultPartitionCoordinator");
		
		
		/** a string that uniquely identifies a set of consumers within the same consumer group */
//		  val groupId = props.getString("group.id")
//		  val consumerId: Option[String] = Option(props.getString("consumer.id", null))
//		  val socketTimeoutMs = props.getInt("socket.timeout.ms", SocketTimeout)
//		  val socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", SocketBufferSize)
//		  val fetchMessageMaxBytes = props.getInt("fetch.message.max.bytes", FetchSize)
//		  val numConsumerFetchers = props.getInt("num.consumer.fetchers", NumConsumerFetchers)
//		  val autoCommitEnable = props.getBoolean("auto.commit.enable", AutoCommit)
//		  val autoCommitIntervalMs = props.getInt("auto.commit.interval.ms", AutoCommitInterval)
//		  val queuedMaxMessages = props.getInt("queued.max.message.chunks", MaxQueuedChunks)
//		  val rebalanceMaxRetries = props.getInt("rebalance.max.retries", MaxRebalanceRetries)
//		  val fetchMinBytes = props.getInt("fetch.min.bytes", MinFetchBytes)
//		  val fetchWaitMaxMs = props.getInt("fetch.wait.max.ms", MaxFetchWaitMs)
//		  val rebalanceBackoffMs = props.getInt("rebalance.backoff.ms", zkSyncTimeMs)
//		  val refreshLeaderBackoffMs = props.getInt("refresh.leader.backoff.ms", RefreshMetadataBackoffMs)
//		  val offsetsChannelBackoffMs = props.getInt("offsets.channel.backoff.ms", OffsetsChannelBackoffMs)
//		  val offsetsChannelSocketTimeoutMs = props.getInt("offsets.channel.socket.timeout.ms", OffsetsChannelSocketTimeoutMs)
//		  val offsetsCommitMaxRetries = props.getInt("offsets.commit.max.retries", OffsetsCommitMaxRetries)
//		  val offsetsStorage = props.getString("offsets.storage", OffsetsStorage).toLowerCase
//		  val dualCommitEnabled = props.getBoolean("dual.commit.enabled", if (offsetsStorage == "kafka") true else false)
//		  val autoOffsetReset = props.getString("auto.offset.reset", AutoOffsetReset)
//		  val consumerTimeoutMs = props.getInt("consumer.timeout.ms", ConsumerTimeoutMs)
//		  val clientId = props.getString("client.id", groupId)
//		  val excludeInternalTopics = props.getBoolean("exclude.internal.topics", ExcludeInternalTopics)
//		  val partitionAssignmentStrategy = props.getString("partition.assignment.strategy", DefaultPartitionAssignmentStrategy)
//		  
		
		
		TestKafkaMessageSerializer serializer = new TestKafkaMessageSerializer();
		
        
		kafkaController = new SimpleConsumerController(new KafkaConsumerConfig(prop));
		//kafkaController.start();
		final SimpleConsumerEx consumer=kafkaController.createConsumer(UTGlobal.topic);
		Thread t=new Thread(new Runnable(){
			@Override
			public void run() {
					while(true){
						try {
							consumer.commitOffset();
							Thread.currentThread().sleep(30000L);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
			}
		});
		//t.start();
		Iterator<MessageAndMetadata<byte[], byte[]>>  itr=consumer.iterator();
		while(true){
			while(itr.hasNext()){
				MessageAndMetadata itm=itr.next();
				//if(itm!=null)
				 //System.out.println("topic="+itm.topic()+",a.partition="+itm.partition()+",a.offset="+itm.offset());;
			}
		}
		
	
	}

}
