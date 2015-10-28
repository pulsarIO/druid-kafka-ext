/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.pulsar.druid.firehose.kafka.support.api.KafkaConsumer;
import com.ebay.pulsar.druid.firehose.kafka.support.api.SimpleController;
import com.ebay.pulsar.druid.firehose.kafka.support.api.ZKConnector;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaConsumerConfig;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaZKData;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaZKData.Pair;
import com.ebay.pulsar.druid.firehose.kafka.support.exceptions.KafkaZKException;
import com.ebay.pulsar.druid.firehose.kafka.support.util.LOG;

/**
 * @author qxing
 * 
 **/
public class SimpleConsumerController implements SimpleController {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(SimpleConsumerController.class.getName());
	public SimpleConsumerController(){
		
	}
	public SimpleConsumerController(KafkaConsumerConfig config) {
		try{
			this.config = config;
			start();
		}catch(Exception e){
			throw new RuntimeException("Start Controller Exception:",e);
		}
	}
	public SimpleConsumerEx createConsumer(String topic) {
		LOG.log("Create Consumer...");
		checkArgument(topic != null, "topic couldn't be null.");
		SimpleConsumerEx consumer = new SimpleConsumerEx(config, this, topic);
		consumer.start();
		this.consumers.put(consumer.getConsumerId().toString(), consumer);
		this.register(consumer.getConsumerId().toString(), consumer);
		LOG.log("Consumer created. id="+consumer.getConsumerId());
		return consumer;
	}

	public  class ZkConnector implements ZKConnector<CuratorFramework> {
		private CuratorFramework zkClient;
		private AtomicBoolean zkConnected = new AtomicBoolean(false);
		private List<String> nonresolvablehosts = new ArrayList<String>();
		private List<String> resolvablehosts = new ArrayList<String>();
		private ZKStringSerializer serializer=new ZKStringSerializer();
		public void start() throws InterruptedException {
			LOG.log("Init Zookeeper Connection....");
			String resolvablcnxnstr = findDNSResolvableZkHosts(config
					.zkConnect());
			//rebalance.set(false);
			zkClient = CuratorFrameworkFactory.newClient(
					resolvablcnxnstr,
					config.zkSessionTimeoutMs(),
					config.zkConnectionTimeoutMs(),
					new RetryNTimes(config.retryTimes(),config
							.sleepMsBetweenRetries()));
			zkClient.getConnectionStateListenable().addListener(
					new ConnectionStateListener() {
						@Override
						public void stateChanged(CuratorFramework client,
								ConnectionState newState) {
							LOG.log("Zookeeper Connection state is changed to "+ newState);
							if (newState == ConnectionState.CONNECTED) {
								zkConnected(true);
								rebalanceable(false);
								new Thread(new OnConnectedTask()).start();

							} else if (newState == ConnectionState.SUSPENDED
									|| newState == ConnectionState.LOST) {
								zkConnected(false);
								rebalanceable(false);
								handleZkDisconnected();
							} else if (newState == ConnectionState.RECONNECTED) {
								LOG.log("Zookeeper is reconnected and it's rebalanceable again.");
								zkConnected(true);
								handleZkConnected();
								rebalanceable(true);
							}
						}
					}
					
					);
			zkClient.start();
			boolean success = false;
			try {
				success = zkClient.getZookeeperClient()
						.blockUntilConnectedOrTimedOut();
			} catch (InterruptedException e) {
			}
	
			if (!success) {
				LOG.log("Zookeeper Not Connected.");
			} else {
				LOG.log("Init Zookeeper Completed.");
			}
		}

		public void close() {
			zkClient.close();
			LOG.log("Close Zookeeper.");
			zkConnected(false);
		}

		public boolean isZkConnected() {
			return zkConnected.get();
		}

		public CuratorFramework getZkClient() {
			if (isZkConnected())
				return zkClient;
			else
				throw new KafkaZKException("Zookeeper not connected.");
		}

		private void zkConnected(boolean connected) {
			LOG.log("Zookeeper " + (connected ? "Connected." : "Disconnected."));
			zkConnected.set(connected);
		}

		private String findDNSResolvableZkHosts(String origCnxnStr) {
			String[] hostporttuple = origCnxnStr.split(",");
			StringBuffer newcnxStr = new StringBuffer();
			int count = 1;
			// reset resolvable list
			resolvablehosts.clear();
			nonresolvablehosts.clear();
			for (String hostport : hostporttuple) {
				String host = hostport.split(":")[0];
				String port = hostport.split(":")[1];
				try {
					InetAddress.getAllByName(host);
					if (count != 1)
						newcnxStr.append(",");

					newcnxStr.append(host);
					newcnxStr.append(":");
					newcnxStr.append(port);
					resolvablehosts.add(host);
					count++;
				} catch (UnknownHostException ukhe) {
					// String trace = ExceptionUtils.getStackTrace(ukhe);
					nonresolvablehosts.add(host);
					// LOGGER.error(" Non Resolvable HostName : " + host + " : "
					// + trace);
				} catch (Throwable t) {
					// String trace = ExceptionUtils.getStackTrace(t);
					nonresolvablehosts.add(host);
					// LOGGER.error(" Got other exception while resolving hostname : "
					// + host + " : " + trace);
				}

			}
			return newcnxStr.toString();
		}
		
		/**
		 * 
		 * make sure a persistent path exists in ZK. Create the path if not exist.
		 * Never throw NoNodeException or NodeExistsException.
		 * 
		 * @throws Exception 
		 */
		public void makeSurePersistentPathExists( String path) throws Exception {
			if(!pathExists(path)){
				zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
			}
		}

		/**
		 * create the parent path
		 * @throws Exception 
		 */
		public void createParentPath( String path) throws Exception {
			String parentDir = path.substring(0, path.lastIndexOf('/'));
			if (parentDir.length() != 0)
				zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(parentDir);
		}

		/**
		 * Create an ephemeral node with the given path and data. Create parents if
		 * necessary.
		 * @throws Exception 
		 */
		public String createEphemeralPath( String path,
				String data) throws Exception {
			return zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, serializer.serialize(data));
			
		}

		/**
		 * Create an persistent node with the given path and data. Create parents if
		 * necessary.
		 * @throws Exception 
		 */
		public void createPersistentPath( String path,
				String data) throws Exception {
			zkClient.create().creatingParentsIfNeeded().forPath(path, serializer.serialize(data));
		}

		public String createSequentialPersistentPath(String path, String data) throws Exception {
			return zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(path,serializer.serialize(data));
		}
		/**
		 * Update the value of a persistent node with the given path and data.
		 * create parrent directory if necessary. 
		 * Never throw NodeExistException.
		 * @throws Exception 
		 */
		public void updateEphemeralPath( String path,
				String data) throws Exception {
			try {
				zkClient.setData().forPath(path, serializer.serialize(data));
			}catch(KeeperException.NoNodeException nne){
				createEphemeralPath(path,data);
			}
		}
		/**
		 * Update the value of a persistent node with the given path and data.
		 * create parrent directory if necessary. Never throw NodeExistException.
		 * Return the updated path zkVersion
		 * @throws Exception 
		 */
		public void updatePersistentPath(String path,
				String data) throws Exception {
			makeSurePersistentPathExists(path);
			zkClient.setData().forPath(path, serializer.serialize(data));
		}		
		/**
		 * delete path.
		 * Never throw NoNodeException.
		 * 
		 * @param path
		 * @return true is success and false is failed.
		 */
		public boolean deletePath( String path){
			try {
				return zkClient.delete().guaranteed().forPath(path) != null;
			} catch(KeeperException.NoNodeException nne){
				// this can happen during a connection loss event, return normally
				return false;
			}catch (Throwable e) {
				throw new KafkaZKException(e);
			} 
		}
		/**
		 * delete path recursively. 
		 * Never throw NoNodeException.
		 * 
		 * @param path
		 */
		public void deletePathRecursive( String path){
			try {
				zkClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
			}catch(KeeperException.NoNodeException nne){
				 //this can happen during a connection loss event, return normally
			}  catch(Throwable t){
				throw new KafkaZKException(t);
			}
		}
		/**
		 * read data from path.
		 * Exception thrown based on underlying zookeeper zkClient. 
		 * @param path
		 * @return
		 */
		public String readData( String path) {
			try{
			String dataStr = serializer.deserialize(zkClient.getData().forPath(path));
			return dataStr;
			}catch(Exception e){
				throw new KafkaZKException(e);
			}
		}
		/**
		 * read data from path, Pair.lhs is the String type data and Pair.rhs is the stat. 
		 * Never throw NoNodeException
		 * 
		 * @param path
		 * @return Pair.lhs may be null and the stat will be no use, otherwise return the data and stat pair.
		 */
		public Pair<String,Stat> readDataMaybeNullWithStat( String path) {
			Stat stat = new Stat();
			try {
				return new Pair<String,Stat>(serializer.deserialize(zkClient.getData().storingStatIn(stat).forPath(path)),stat);
			} catch(KeeperException.NoNodeException nne){
				return new Pair<String,Stat>(null,stat);
			}catch (Throwable e) {
				throw new KafkaZKException(e);
			} 
		}
		/**
		 * read data from path. 
		 * Never throw NoNodeException
		 * 
		 * @param path
		 * @return may be null if no data or no node exist.
		 */
		public String readDataMaybeNull( String path) {
			try {
				return serializer.deserialize(zkClient.getData().forPath(path));
			}catch(KeeperException.NoNodeException nne){
				return null;
			}catch (Throwable e) {
				throw new KafkaZKException(e);
			} 
		}
		/**
		 * get children for path. 
		 * Exception thrown based on underlying zookeeper zkClient. 
		 * 
		 * @param path
		 * @return
		 */
		public List<String> getChildren(String path) {
			try{
			return zkClient.getChildren().forPath(path);
			}catch (Throwable e) {
				throw new KafkaZKException(e);
			} 
		}
		/**
		 * get children for path, if parent deos not exist, return null, never throw NoNodeException.
		 *  
		 * @param path
		 * @return
		 */
		public List<String> getChildrenParentMayNotExist(
				String path) {
			try {
				return zkClient.getChildren().forPath(path);
			}catch (KeeperException.NoNodeException e) {
				return null;
			}  catch (Throwable e) {
				throw new KafkaZKException(e);
			} 
		}

		/**
		 * Check if the given path exists
		 * @throws Exception 
		 */
		public boolean pathExists( String path) throws Exception {
			return zkClient.checkExists().forPath(path) != null;
		}	
		
		public class ZKStringSerializer {
			public byte[] serialize(Object data) {
				try {
					return data.toString().getBytes("UTF-8");
				} catch (UnsupportedEncodingException e) {
					throw new KafkaZKException(e);
				}
			}
			public String deserialize(byte[] bytes) {
				if (bytes == null)
					return null;
				else
					try {
						return new String(bytes, "UTF-8");
					} catch (UnsupportedEncodingException e) {
						throw new KafkaZKException(e);
					}
			}
		}
	}

	private KafkaConsumerConfig config;
	private ZkConnector zkConnector;
	private Map<String, KafkaConsumer> consumers = new ConcurrentHashMap<String, KafkaConsumer>();
	private Timer timer = new Timer();
	private AtomicBoolean rebalanceable = new AtomicBoolean(false);
	private AtomicBoolean started = new AtomicBoolean(false);
	private Object lock = new Object();

	public ZKConnector<?> getZkConnector() {
		return zkConnector;
	}

	public List<String> getNonresolvablehosts() {
		return zkConnector.nonresolvablehosts;
	}

	public List<String> getResolvablehosts() {
		return zkConnector.resolvablehosts;
	}

	public void stop() {
		synchronized (lock) {
			if(started.get()){
				started.set(false);
				stopAllConsumers();
				cancelTimer();
				zkConnector.close();
				LOG.log("KafkaController is stopped.");
			}else{
				LOG.log("KafkaController is already stopped.");
			}
		}
	}

	public void start() throws InterruptedException {
		synchronized (lock) {
			if(!started.get()){
				LOG.log("Starting KafkaController...");
				if (zkConnector == null)
					zkConnector = new ZkConnector();
				startTimer();
				zkConnector.start();
				started.set(true);
				LOG.log("KafkaController is started.");
			}else{
				LOG.log("KafkaController is already started.");
			}
		}
	}
	public boolean isStarted() {
		return started.get();
	}

	private void startTimer() {
		LOG.log("Start Rebalance Timer...");
		if (config.rebalanceBackoffMs() <= 0)
			return;
		RebalanceTimerTask task = new RebalanceTimerTask();
		try {
			timer = new Timer();
			timer.schedule(task, config.rebalanceBackoffMs() + 2000,
					config.rebalanceBackoffMs()/* config.getRebalanceInterval() */);

		} catch (IllegalStateException ie) {
			LOGGER.error("Errors when schedule timer for KafkaController.");
		}
		LOG.log("Rebalance Timer is established, timer is scheduled.");
	}

	private void cancelTimer() {

		if (timer != null) {
			timer.cancel();
			timer = null;
			LOG.log("Rebalancer is shutdown, timer is canceled.");
		}
	}

	private void handleZkConnected() {
		try {
			LOG.log("Notice all Consumers to handlZkConnected.");
			startAllConsumers();
		} catch (Throwable th) {
			LOG.log("Error occurs when noticing kafka consumers about zk Connected.");
		}
	}

	private void handleZkDisconnected() {
		try {
			LOG.log("Notice all Consumers Zookeeper Disconnected.");
			stopAllConsumers();
		} catch (Throwable th) {
			LOG.log("Error occurs when noticing consumers about zk disconnected.");
		}
	}

	private void startAllConsumers() {
		if (consumers == null || consumers.isEmpty()) {
			LOG.log("No kafkaConsumer needs to be started.");
			return;
		}
		//stopAllConsumers();
		// consumers = new ConcurrentHashMap<String, KafkaConsumer>();
		for (String name : consumers.keySet()) {
			KafkaConsumer consumer = consumers.get(name);
			try {
				if(!consumer.isRunning()){
				consumer.start();
				// consumers.put(consumer.getConsumerId().toString(), consumer);
				register(consumer.getConsumerId().toString(), consumer);
				}
			} catch (Exception e) {
				LOGGER.error("Error occurs when starting consumer " + name, e);
			}
		}
	}

	private void stopAllConsumers() {
		LOG.log("Stop All Consumers.");
		if (consumers == null || consumers.isEmpty()) {
			LOG.log("No Consumer needs to be stopped.");
			return;
		}
		for (String name : consumers.keySet()) {
			KafkaConsumer consumer = consumers.get(name);
			try {
				if(consumer.isRunning()){
					consumer.stop();
					unregister(consumer.getConsumerId().toString());
				}
			} catch (Exception e) {
				LOGGER.error("Error occurs when stopping consumer " + name, e);
			}
		}
	}

	public void register(String name, KafkaConsumer consumer) {
		// regist consumer
		if (consumer != null && zkConnector.isZkConnected())
			KafkaZKData.registerConsumer(zkConnector, consumer
					.getTopic(), consumer.getConsumerId().getConsumerGroupId(),
					consumer.getConsumerId().toString());
		// this.consumers.put(name, kafkaConsumer);
		LOG.log("Register Consumer" + name + " in Zookeeper.");
	}

	public void unregister(String name) {
		if (this.consumers == null) {
			return;
		}
		KafkaConsumer consumer = this.consumers.get(name);
		if (consumer != null && zkConnector.isZkConnected()) {
			KafkaZKData.unregisterConsumer(zkConnector, consumer
					.getTopic(), consumer.getConsumerId().getConsumerGroupId(),
					consumer.getConsumerId().toString());
		}
		LOG.log("unRegister Consumer" + name + " in Zookeeper.");
	}

	public boolean isRebalanceable() {
		return rebalanceable.get();
	}

	private void rebalanceable(boolean enable) {
		LOG.log((enable ? "Enable" : "Disable") + " Rebalanaceable.");
		rebalanceable.set(enable);
	}

	class OnConnectedTask implements Runnable {

		@Override
		public void run() {
			try {
				LOG.log("OnConnected. It will be rebalanceable only after waiting for all other nodes reconnected.");
//				handleZkConnected();
				Thread.sleep(config.getRebalanceableWaitInMs());
				
			} catch (InterruptedException e) {
			}
			rebalanceable(true);
		}

	}

	class RebalanceTimerTask extends TimerTask {
		@Override
		public void run() {
			if (!zkConnector.isZkConnected() || !isRebalanceable()) {
				LOG.log("Not rebalanceable this round. zkConnected="+zkConnector.isZkConnected()+",isRebalanceable="+isRebalanceable());
				return;
			}
			if (consumers == null || consumers.isEmpty()) {
				LOG.log("No Consumer needs to be rebalanced this round.");
				return;
			}
			//LOG.log("Trigger rebalance one round.");
			for (String name : consumers.keySet()) {
				KafkaConsumer consumer = consumers.get(name);
				try {
					consumer.rebalance();
				} catch (Throwable e) {
					LOG.log("Error occurs during rebalance of " + name, e);
				}
			}
		}

	}

}
