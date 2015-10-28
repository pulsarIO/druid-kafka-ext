package lowlevelapi.test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import kafka.message.MessageAndMetadata;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ebay.pulsar.druid.firehose.kafka.support.SimpleConsumerController;
import com.ebay.pulsar.druid.firehose.kafka.support.SimpleConsumerEx;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaConsumerConfig;

public class KafkaLowLevelApiTest {

	private SimpleConsumerController kafkaController;
	
	private static final String ZKCONNECT="10.9.206.198:2181,10.65.194.137:2181,10.65.194.139:2181";
	private static final Properties prop=new Properties();

	private static String TOPIC="T-00010";
	private static long TOTAL_MSG_COUNT=100000;
	private static String GROUPID= "cg-20150716000005";
	
	@BeforeClass
	public static void beforeCalss(){
		prop.put("zookeeper.connect",	ZKCONNECT);
		prop.put("zookeeper.connection.timeout.ms", "2000");
		prop.put("zookeeper.session.timeout.ms", "5000");
		prop.put("rebalance.max.retries", "20");
		prop.put("zookeeper.sync.time.ms", "5000");
		prop.put("group.id", GROUPID);
		prop.put("fetch.message.max.bytes", "67108864");
		prop.put("fetch.min.bytes","1048576");
		prop.put("auto.offset.reset", "smallest");
		prop.put("auto.commit.enable", "false");
		prop.put("rebalance.backoff.ms","5000");
		prop.put("rebalance.wait.ms","2000");
		prop.put("rebalance.first.wait.ms","2000");
		prop.put("num.consumer.fetchers", "5");
		prop.put("partition.assignment.strategy","io.druid.firehose.kafka.support.DefaultPartitionCoordinator");
	}
	
	@Before
	public void before() {
		TOTAL_MSG_COUNT=100000;
	}
	
	@Test
	public void testAddPartitionAtRuntime(){
		TOPIC="T-00040";
		TOTAL_MSG_COUNT = Long.MAX_VALUE;
		
		long count = doConsume(false, 20*60);
		assertThat(count, is(TOTAL_MSG_COUNT));
	}
	
	@Test
	public void testNormalRead(){
		GROUPID= "cg-201507170007";
		TOPIC="T-00070";
		beforeCalss();
		
		long count = doConsume(true, 5*60);
		assertThat(count, is(TOTAL_MSG_COUNT));
	}
	
	public void testNormalReadMultipleConsumer(){
		long count = doConsume(true);
		System.out.println("msg consumed: "+count);
	}
	
	@Test
	public void testNormalReadAfterCommitOffset() throws Exception{
		long count = doConsume(true);
		assertThat(count, is(TOTAL_MSG_COUNT));
		
		System.out.println("go check offset in zk");
		Thread.sleep(10*1000);
		
		count = doConsume(true);
		assertThat(count, is(0l));
	}
	
	/**
	Topic:Test-Druid-Kafka-Reb0     PartitionCount:5        ReplicationFactor:1     Configs:
        Topic: Test-Druid-Kafka-Reb0    Partition: 0    Leader: 3       Replicas: 3     Isr: 3
        Topic: Test-Druid-Kafka-Reb0    Partition: 1    Leader: -1      Replicas: 1     Isr:
        Topic: Test-Druid-Kafka-Reb0    Partition: 2    Leader: 2       Replicas: 2     Isr: 2
        Topic: Test-Druid-Kafka-Reb0    Partition: 3    Leader: 3       Replicas: 3     Isr: 3
        Topic: Test-Druid-Kafka-Reb0    Partition: 4    Leader: -1      Replicas: 1     Isr:


	[zk: localhost:2181(CONNECTED) 58] get /consumers/KafkaLowLevelApiTest-201507091006/offsets/Test-Druid-Kafka-Reb0/0
	20029
	[zk: localhost:2181(CONNECTED) 59] get /consumers/KafkaLowLevelApiTest-201507091006/offsets/Test-Druid-Kafka-Reb0/1
	0
	[zk: localhost:2181(CONNECTED) 60] get /consumers/KafkaLowLevelApiTest-201507091006/offsets/Test-Druid-Kafka-Reb0/2
	19891
	[zk: localhost:2181(CONNECTED) 61] get /consumers/KafkaLowLevelApiTest-201507091006/offsets/Test-Druid-Kafka-Reb0/3
	20151
	[zk: localhost:2181(CONNECTED) 62] get /consumers/KafkaLowLevelApiTest-201507091006/offsets/Test-Druid-Kafka-Reb0/4
	0
	 */
	@Test
	public void testSingleConsumer_LostKafkaBrokerAtRuntime() throws Exception{
		long count = doConsume(true);
		System.out.println("total consumed msg: " +count);
	}
	
	public void testMultipleConsumers_LostKafkaBrokerAtRuntime() throws Exception{
		long count = doConsume(true);
		System.out.println("total consumed msg: " +count);
	}
	
	/**
	[zk: localhost:2181(CONNECTED) 58] get /consumers/KafkaLowLevelApiTest-201507091006/offsets/Test-Druid-Kafka-Reb0/0
	20029
	[zk: localhost:2181(CONNECTED) 59] get /consumers/KafkaLowLevelApiTest-201507091006/offsets/Test-Druid-Kafka-Reb0/1
	0
	[zk: localhost:2181(CONNECTED) 60] get /consumers/KafkaLowLevelApiTest-201507091006/offsets/Test-Druid-Kafka-Reb0/2
	19891
	[zk: localhost:2181(CONNECTED) 61] get /consumers/KafkaLowLevelApiTest-201507091006/offsets/Test-Druid-Kafka-Reb0/3
	20151
	[zk: localhost:2181(CONNECTED) 62] get /consumers/KafkaLowLevelApiTest-201507091006/offsets/Test-Druid-Kafka-Reb0/4
	0
	
	-----start kafka-------
	[zk: localhost:2181(CONNECTED) 63] get /consumers/KafkaLowLevelApiTest-201507091006/offsets/Test-Druid-Kafka-Reb0/1
	20004
	[zk: localhost:2181(CONNECTED) 64] get /consumers/KafkaLowLevelApiTest-201507091006/offsets/Test-Druid-Kafka-Reb0/4
	19925

	 */
	@Test
	public void testSingleConsumer_AddKafkaBrokerAtRuntime() throws Exception{
		long count = doConsume(true);
		System.out.println("total consumed msg: " +count);
	}
	
	@Test
	public void testSingleConsumer_RestartKafkaBrokerAtRuntime() throws Exception{
		long count = doConsume(true, 100);
		System.out.println("total consumed msg: " +count);
	}
	
	@Test
	public void testSingleConsumer_RestartKafkaBrokerAtRuntime_2() throws Exception{
		long count = doConsume(true, 60*60);
		System.out.println("total consumed msg: " +count);
	}
	
	public void testMultipleConsumer_RestartKafkaBrokerAtRuntime() throws Exception{
		GROUPID= "cg-201507161517";
		TOPIC="Test-Druid-Kafka-Down0";
		beforeCalss();
		
		long count = doConsume(true, 600);
		System.out.println("total consumed msg: " +count);
	}
	
	public void testMultipleConsumer_RestartKafkaBrokerAtRuntime_2() throws Exception{
		long count = doConsume(true, 300);
		System.out.println("total consumed msg: " +count);
	}
	
	@Test
	public void testSingleConsumer_RestartZKEntirely(){
		long count = doConsume(true, 200);
		System.out.println("msg consumed: "+count);
		assertThat(count, is(TOTAL_MSG_COUNT));
	}
	
	public void testMultipleConsumers_RestartZKEntirely(){
		long count = doConsume(true, 200);
		System.out.println("msg consumed: "+count);
		assertThat(count, is(TOTAL_MSG_COUNT));
	}

	/**
	---5 part, 5 consumers:
	total consumed msg: 20151 partition num: [3]
	total consumed msg: 19891 partition num: [2]
	total consumed msg: 20004 partition num: [1]
	total consumed msg: 20029 partition num: [0]
	total consumed msg: 19925 partition num: [4]
	
	---5 part, 3 consumers:
	total consumed msg: 40155 partition num: [1, 3]
	total consumed msg: 39920 partition num: [0, 2]
	total consumed msg: 19925 partition num: [4]
	
	---5 part, 7 consumers:
	total consumed msg: 20151 partition num: [3]
	total consumed msg: 19891 partition num: [2]
	total consumed msg: 20004 partition num: [1]
	total consumed msg: 20029 partition num: [0]
	total consumed msg: 19925 partition num: [4]
	total consumed msg: 0 partition num: []
	total consumed msg: 0 partition num: []
	 */
	@Test
	public void testNormalReadWithMultipleConsumers(){
		long count = 0;
		final SimpleConsumerEx consumer=kafkaController.createConsumer(TOPIC);
		Iterator<MessageAndMetadata<byte[], byte[]>>  itr=consumer.iterator();
		long start = System.currentTimeMillis();
		Set<Integer> partSet = new HashSet<Integer>();
		while(true){
			while(itr.hasNext()){
				MessageAndMetadata<byte[], byte[]> itm=itr.next();
				int part = itm.partition();
				partSet.add(part);
				if(count == 0){
					System.out.println("topic="+itm.topic()+",a.partition="+itm.partition()+",a.offset="+itm.offset());;
				}
				count++;
			}
			
			long end = System.currentTimeMillis();
			if(end-start > 100*1000 || count == TOTAL_MSG_COUNT){
				break;
			}
		}
		System.out.println("total consumed msg: " +count);
		System.out.println("partition num: "+ partSet);
	}
	
	@Test
	public void testSingleConsumerLostZooKeeper() throws Exception{
		long count = 0;
		final SimpleConsumerEx consumer=kafkaController.createConsumer(TOPIC);
		Iterator<MessageAndMetadata<byte[], byte[]>>  itr=consumer.iterator();
		long start = System.currentTimeMillis();
		Set<Integer> partSet = new HashSet<Integer>();
		while(true){
			while(itr.hasNext()){
				MessageAndMetadata<byte[], byte[]> itm=itr.next();
				int part = itm.partition();
				partSet.add(part);
				if(count == 0){
					System.out.println("topic="+itm.topic()+",a.partition="+itm.partition()+",a.offset="+itm.offset());;
				}
				if(count == TOTAL_MSG_COUNT/3){
					System.out.println("about to sleep 10s, kill zk");
					Thread.sleep(10*1000);
				}
				count++;
			}
			
			long end = System.currentTimeMillis();
			if(end-start > 200*1000 || count == TOTAL_MSG_COUNT){
				break;
			}
		}
		System.out.println("total consumed msg: " +count);
		System.out.println("partition num: "+ partSet);
	}
	
	@Test
	public void testRebalance_LostConsumer() throws Exception{
		kafkaController = new SimpleConsumerController(new KafkaConsumerConfig(prop));
		
		long count = 0;
		final SimpleConsumerEx consumer=kafkaController.createConsumer(TOPIC);
		Iterator<MessageAndMetadata<byte[], byte[]>>  itr=consumer.iterator();
		long start = System.currentTimeMillis();
		Set<Integer> partSet = new HashSet<Integer>();
		while(true){
			while(itr.hasNext()){
				MessageAndMetadata<byte[], byte[]> itm=itr.next();
				if(null == itm){
					continue;
				}
				
				int part = itm.partition();
				partSet.add(part);
				if(count == 0){
					System.out.println("topic="+itm.topic()+",a.partition="+itm.partition()+",a.offset="+itm.offset());;
				}
				if(count == 5000 && part==0 && partSet.size()==1){
					System.out.println("this consumer exit, partition: "+part);
					System.out.println("msg count: "+ count);
					System.exit(1);
				}
				
				if(count % 999 ==0){System.out.print("-"+partSet);}
				count++;
			}
			
			long end = System.currentTimeMillis();
			if(end-start > 200*1000 && !itr.hasNext()){
				break;
			}
		}
		consumer.markCommitOffset();
		kafkaController.stop();
		System.out.println("total consumed msg: " +count);
		System.out.println("partition num: "+ partSet);
	}
	
	@Test
	public void testRebalance_LostConsumer_2() throws Exception{
		kafkaController = new SimpleConsumerController(new KafkaConsumerConfig(prop));
		
		long count = 0;
		final SimpleConsumerEx consumer=kafkaController.createConsumer(TOPIC);
		Iterator<MessageAndMetadata<byte[], byte[]>>  itr=consumer.iterator();
		long start = System.currentTimeMillis();
		Set<Integer> partSet = new HashSet<Integer>();
		while(true){
			while(itr.hasNext()){
				MessageAndMetadata<byte[], byte[]> itm=itr.next();
				if(null == itm){
					continue;
				}
				
				int part = itm.partition();
				partSet.add(part);
				if(count == 0){
					System.out.println("topic="+itm.topic()+",a.partition="+itm.partition()+",a.offset="+itm.offset());;
				}
				if(count == 5000 && partSet.contains(0) && partSet.size()==2){
					System.out.println("this consumer exit, partition: "+part);
					System.out.println("msg count: "+ count);
					System.exit(1);
				}
				
				if(count % 999 ==0){System.out.print("-"+partSet);}
				count++;
			}
			
			long end = System.currentTimeMillis();
			if(end-start > 200*1000 && !itr.hasNext()){
				break;
			}
		}
		consumer.markCommitOffset();
		kafkaController.stop();
		System.out.println("total consumed msg: " +count);
		System.out.println("partition num: "+ partSet);
	}
	
	@Test
	public void testRebalance_FUllGC() throws Exception{
		kafkaController = new SimpleConsumerController(new KafkaConsumerConfig(prop));
		
		long count = 0;
		final SimpleConsumerEx consumer=kafkaController.createConsumer(TOPIC);
		Iterator<MessageAndMetadata<byte[], byte[]>>  itr=consumer.iterator();
		long start = System.currentTimeMillis();
		Set<Integer> partSet = new HashSet<Integer>();
		while(true){
			while(itr.hasNext()){
				MessageAndMetadata<byte[], byte[]> itm=itr.next();
				int part = itm.partition();
				partSet.add(part);
				if(count == 0){
					System.out.println("topic="+itm.topic()+",a.partition="+itm.partition()+",a.offset="+itm.offset());;
				}
				if(count == 5000){
					System.out.println("this consumer exit, partition: "+part);
					System.out.println("msg count: "+ count);
					System.out.println("try to trigger full gc ");
					FullGc.main(null);
				}
				count++;
			}
			
			long end = System.currentTimeMillis();
			if(end-start > 200*1000 || count == TOTAL_MSG_COUNT){
				break;
			}
		}
		consumer.markCommitOffset();
		kafkaController.stop();
		System.out.println("total consumed msg: " +count);
		System.out.println("partition num: "+ partSet);
	}

	private long doConsume(boolean commit, boolean checkContent) {
		return doConsume(commit, 100, checkContent);
	}
	
	private long doConsume(boolean commit) {
		return doConsume(commit, 100, false);
	}
	
	private long doConsume(boolean commit, int count) {
		return doConsume(commit, count, false);
	}
	
	private long doConsume(boolean commit, long runtime, boolean checkContent) {
		kafkaController = new SimpleConsumerController(new KafkaConsumerConfig(prop));
		long count = 0;
		final SimpleConsumerEx consumer=kafkaController.createConsumer(TOPIC);
		Iterator<MessageAndMetadata<byte[], byte[]>>  itr=consumer.iterator();
		Set<Integer> ps = new HashSet<Integer>();
		
		long start = System.currentTimeMillis();
		while(true){
			while(itr.hasNext()){
				MessageAndMetadata<byte[], byte[]> itm=itr.next();
				if(null == itm){
					continue;
				}
				
				if(count == 0){
					System.out.println("topic="+itm.topic()+",a.partition="+itm.partition()+",a.offset="+itm.offset());;
				}
				
				ps.add(itm.partition());
				count++;
				
				if(count % 999 ==0){
					System.out.println("-"+ps);
					ps.clear();
					if(checkContent){	
						System.out.println(new String(itm.message()).length());
						//assertThat(itm.message().length, is(1024));
					}
				}
			}
			
			if(count >= TOTAL_MSG_COUNT){
				System.out.println("break out: count >= TOTAL_MSG_COUNT");
				break; 
			}
			long end = System.currentTimeMillis();
			if(end-start > runtime * 1000){ //&& !itr.hasNext()
				break;
			}
		}
		if(commit){
			consumer.markCommitOffset();
		}
		kafkaController.stop();
		System.out.println();
		return count;
	}
	

	public static void main(String[] args) throws Exception{
		KafkaLowLevelApiTest.beforeCalss();
		KafkaLowLevelApiTest test = new KafkaLowLevelApiTest();
		test.testNormalRead();
		System.out.println("--------------------MAIN EXIT--------------------");
	}
}
