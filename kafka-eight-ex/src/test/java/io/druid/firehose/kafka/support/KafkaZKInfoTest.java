package io.druid.firehose.kafka.support;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.common.TopicAndPartition;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ebay.pulsar.druid.firehose.kafka.support.SimpleConsumerController;
import com.ebay.pulsar.druid.firehose.kafka.support.api.ZKConnector;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaConsumerConfig;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaZKData;
import com.ebay.pulsar.druid.firehose.kafka.support.util.JSON;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * @author qxing
 * 
 **/
public class KafkaZKInfoTest {
	private class ZKStringSerializer implements ZkSerializer{
		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			try {
				return data.toString().getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new ZkMarshallingError(e);
			}
		}
		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			if (bytes == null)
			      return null;
			else
				try {
					return new String(bytes, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					throw new ZkMarshallingError(e);
				}
		}
	  }
	public static ZKConnector zkClient;
	@BeforeClass
	public static void setup() throws InterruptedException{
		
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
		prop.put("num.consumer.fetchers", "1");
		prop.put("partition.assignment.strategy","io.druid.firehose.kafka.support.DefaultPartitionCoordinator");
		//kafkaController = new SimpleConsumerController(new KafkaConsumerConfig(prop));
		SimpleConsumerController controller=new SimpleConsumerController(new KafkaConsumerConfig(prop));
		controller.stop();
		zkClient=controller.new ZkConnector();
		zkClient.start();
//		zkClient = CuratorFrameworkFactory.newClient(
//				UTGlobal.zkConnect,
//				30000,
//				30000,
//				new RetryNTimes(3,1000));
//		zkClient.start();
//		
//		boolean success = false;
//		try {
//			success = zkClient.getZookeeperClient()
//					.blockUntilConnectedOrTimedOut();
//		} catch (InterruptedException e) {
//		}
	}
	@Test
	public void testKafkaView(){
		
		List<String> allTopics=KafkaZKData.getAllTopics(zkClient);
		Cluster cluster=KafkaZKData.getCluster(zkClient);
		Set<TopicAndPartition> tp=KafkaZKData.getAllPartitions(zkClient);
		List<Broker> allBrokers=KafkaZKData.allBrokersInCluster(zkClient);
		Map<String,List<Integer>> view=KafkaZKData.assignedReplicasView(zkClient, UTGlobal.topic);
		System.out.println("AllTopics:"+JSON.toJSONString(allTopics));
		List<String> all=Lists.transform(allBrokers, new Function<Broker,String>(){
			@Override
			public String apply(Broker input) {
				return "host="+input.host()+",id="+input.id();
			}
		});
		System.out.println("View:"+JSON.toJSONString(all));
		System.out.println("ARV:"+JSON.toJSONString(KafkaZKData.assignedReplicasView(zkClient, UTGlobal.topic)));
		Set<Integer> allPartitions=KafkaZKData.allPartitionsOfTopic(zkClient, UTGlobal.topic);
		System.out.println("ALL-Partitions:"+JSON.toJSONString(allPartitions));
		for(Integer p: allPartitions){
			System.out.println("Leader-"+p+":"+KafkaZKData.leaderForPartition(zkClient, UTGlobal.topic, p));
		}
		List<String> allConsumers=KafkaZKData.getConsumersInGroup(zkClient, UTGlobal.group);
		System.out.println("allConsumers:"+JSON.toJSONString(allConsumers));
		Map<String,Set<Integer>> consumerView=KafkaZKData.partitionOwnerViewOfTopic(zkClient, UTGlobal.topic, UTGlobal.group);
		System.out.println("ConsuerView:"+JSON.toJSONString(consumerView));
	}
	
	@Test
	public void test() {
		//String zkConnect="10.64.219.218:2181,10.64.219.219:2181,10.64.219.220:2181";
		String topic = "Topic.test-4";
		String group="test-druid-zk-info";
//		System.out.println(KafkaZKInfo.leaderAndIsrForPartition(zkClient, topic, 0).toString());
//		System.out.println(KafkaZKInfo.leaderIsrAndEpochForPartition(zkClient, topic, 0).toString());
//		System.out.println(KafkaZKInfo.leaderForPartition(zkClient, topic, 0));
//		
//		System.out.println(KafkaZKInfo.leaderAndIsrForPartition(zkClient, topic, 1).toString());
//		System.out.println(KafkaZKInfo.leaderIsrAndEpochForPartition(zkClient, topic, 1).toString());
//		System.out.println(KafkaZKInfo.leaderForPartition(zkClient, topic, 1));
//		System.out.println(JSON.toJSONString(KafkaZKInfo.assignedReplicasView(zkClient, topic)));
//		System.out.println(JSON.toJSONString(KafkaZKInfo.assignedReplicasForPartition(zkClient, topic,1)));
		
		List<String> consumer=KafkaZKData.consumersOfTopic(zkClient,group , topic);
		System.out.println("consumers:"+JSON.toJSONString(consumer));
		System.out.println(JSON.toJSONString(KafkaZKData.getPartitionAssignmentForTopics(zkClient, ImmutableList.of(topic))));
		System.out.println(JSON.toJSONString(KafkaZKData.getPartitionsForTopics(zkClient, ImmutableList.of(topic))));

	}
}
