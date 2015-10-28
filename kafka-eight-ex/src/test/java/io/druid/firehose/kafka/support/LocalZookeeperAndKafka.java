package io.druid.firehose.kafka.support;
/**
 *@author qxing
 * 
 **/
public class LocalZookeeperAndKafka {
	private static TestZookeeperServer zkServer;
	private static TestKafkaServer kafkaBroker0;
	private static TestKafkaServer kafkaBroker1;	
	public static void main(String[] args){
		//String zkConnect="10.64.219.218:2181,10.64.219.219:2181,10.64.219.220:2181";
		try {
			zkServer = new TestZookeeperServer(30000, 2183, 100);
			zkServer.startup();
		} catch (Exception e) {
			e.printStackTrace();
		}
		String zkConnector="localhost:2183";
		kafkaBroker0 = new TestKafkaServer("/kafka0/", 9082, 0,zkConnector, 2);
		kafkaBroker1 = new TestKafkaServer("/kafka1/", 9083, 1, zkConnector, 2);
		try{
			Thread.currentThread().join();
		}catch(Exception e){
			
		}
	}
}
