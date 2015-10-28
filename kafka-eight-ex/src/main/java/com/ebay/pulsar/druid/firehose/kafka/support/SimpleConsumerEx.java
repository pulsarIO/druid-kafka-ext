/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support;


import static com.google.common.base.Preconditions.checkArgument;

import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;

import org.apache.zookeeper.KeeperException.ConnectionLossException;

import com.ebay.pulsar.druid.firehose.kafka.support.api.KafkaConsumer;
import com.ebay.pulsar.druid.firehose.kafka.support.api.PartitionCoordinatorStrategy;
import com.ebay.pulsar.druid.firehose.kafka.support.api.PartitionReader;
import com.ebay.pulsar.druid.firehose.kafka.support.api.SimpleController;
import com.ebay.pulsar.druid.firehose.kafka.support.api.ZKConnector;
import com.ebay.pulsar.druid.firehose.kafka.support.data.ConsumerId;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaConsumerConfig;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaZKData;
import com.ebay.pulsar.druid.firehose.kafka.support.exceptions.KafkaPartitionReaderException;
import com.ebay.pulsar.druid.firehose.kafka.support.exceptions.KafkaZKException;
import com.ebay.pulsar.druid.firehose.kafka.support.util.LOG;
import com.ebay.pulsar.druid.firehose.kafka.support.util.NameableThreadFactory;

/**
 *@author qxing
 * 
 **/
public class SimpleConsumerEx implements KafkaConsumer,Iterable<MessageAndMetadata<byte[],byte[]>>{
	//private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerEx.class.getName());

	private ExecutorService executor;
	private KafkaConsumerConfig config;
	private String topic;
	private ConsumerId consumerId;
	private Object lock = new Object();
	private AtomicBoolean runningFlag = new AtomicBoolean(false);
	private SimpleController constroller;
	private ZKConnector<?> zkConnector;
	private PartitionCoordinatorStrategy coordinator;
	private final Map<TopicAndPartition, PartitionReader> partitionMap = new ConcurrentHashMap<TopicAndPartition, PartitionReader>();
	private List<ConsumerTask> consumerTasks;
	private final BlockingQueue<PartitionReader> queue = new LinkedBlockingQueue<PartitionReader>();
	private KafkaConsumerIterator stream;
	
	private AtomicLong readCount=new AtomicLong(0L);
	private AtomicLong commitCount=new AtomicLong(0L);

	public SimpleConsumerEx(KafkaConsumerConfig config,SimpleController constroller,String topic){
		this.config=config;
		checkArgument(constroller.getZkConnector()!=null,"Controller is not started.Please start it first.");
		stream=new KafkaConsumerIterator(config.queuedMaxMessages());
		this.constroller=constroller;
		this.zkConnector=constroller.getZkConnector();
		this.topic=topic;
		if(consumerId==null)
			consumerId = new ConsumerId(config.groupId());
	}
	

	public class KafkaConsumerIterator implements Iterator<MessageAndMetadata<byte[],byte[]>> {
		private Queue<MessageAndMetadata<byte[],byte[]>> queue;
		private ReentrantLock lock=new ReentrantLock();
		private Condition notEmpty=lock.newCondition();
		private Condition notFull=lock.newCondition();
		private AtomicInteger count=new AtomicInteger(0);
		private long capability;
		public KafkaConsumerIterator(int capability){
			queue=new LinkedList<MessageAndMetadata<byte[],byte[]>>();//new LinkedBlockingQueue<MessageAndMetadata<byte[],byte[]>>(capability);
			this.capability=capability;
		}
		
		public void sink(MessageAndMetadata<byte[],byte[]> record) throws InterruptedException{
			lock.lock();
			try{
				while(queue.size()==capability){
					notFull.await();
				}
				count.getAndIncrement();
				queue.offer(record);
				notEmpty.signalAll();
			}finally{
				lock.unlock();
			}
		}
		public void sink(List<MessageAndMetadata<byte[],byte[]>> records) throws InterruptedException{
			for(MessageAndMetadata<byte[],byte[]> mam: records){
				sink(mam);
			}
		}
		public void await(){
			lock.lock();
			try{
				while(queue.size()==0){
					//LOG.log("Await..."+count.getAndIncrement());
					notEmpty.await();
				}
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				lock.unlock();
			}
		}
		@Override
		public boolean hasNext() {
			//await();
			lock.lock();
			try{
				while(queue.size()==0){
					notEmpty.await();
				}
				return queue.size()>0;
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				lock.unlock();
			}
			//return queue.size()>0;
			return false;
		}
		@Override
		public MessageAndMetadata<byte[],byte[]> next() {
			readCount.incrementAndGet();
			lock.lock();
			try {
				while(queue.size()==0){
					notEmpty.await();
				}
				MessageAndMetadata<byte[], byte[]> t= queue.poll();//queue.take();
				notFull.signalAll();
				TopicAndPartition key = new TopicAndPartition(t.topic(), t.partition());
				PartitionReader reader=partitionMap.get(key);
				if(reader!=null)
				{
					commitCount.incrementAndGet();
					reader.getAndSetCommitOffset(t.offset());
					return t;
				}
			} catch (Exception e) {
				LOG.log("Read Message next() exception:",e);
			}finally{
				lock.unlock();
			}
			return null;
		}
		@Override
		public void remove() {
			throw new RuntimeException("Not support remove operation.");
		}
	}
	
	
	public class ConsumerTask implements Runnable {
		private String taskId;
		private LinkedList<PartitionReader> hold = new LinkedList<PartitionReader>();
		private List<MessageAndMetadata<byte[],byte[]>> cache = new ArrayList<MessageAndMetadata<byte[],byte[]>>();
		private PartitionReader borrowReadableReaderAndBlokingIfNotExist() throws InterruptedException{
			PartitionReader preader =queue.poll();
			do{
				if (preader == null) {
					if (hold.isEmpty()) {
						preader = queue.take();
					} else {
						PartitionReader earliest = hold.getFirst();
						long wait = earliest.calcWaitTime();
						if (wait > 0) {
							preader = queue.poll(wait,
									TimeUnit.MILLISECONDS);
						}
						if (preader == null)
							preader = hold.poll();
					}
				}else{
					if (preader.calcWaitTime() > 0) {
						hold(preader);
						preader = null;
					}
				}
			}while(preader==null);
			return preader;
		}

		@Override
		public void run() {
			taskId = Thread.currentThread().getName();
			LOG.log("Start Task "+taskId);
			while (runningFlag.get() && !Thread.currentThread().isInterrupted()) {
				PartitionReader preader = null;
				try {
					cache.clear();
					// check whether to release hold partitions
					checkRelease();
					// get on  a readable reader, if no readable reader exist , block here.
					preader=borrowReadableReaderAndBlokingIfNotExist();
					//check zk down. TODO: consider it carefully whether we really need it.
					// read a batch data <events>.
					cache = preader.readEvents();
					// return back this reader and all holding unreadable reader to <queue> for other threads.
					if(cache.size()<=0){
						hold(preader);
						preader=null;
					}else{
						preader.clearIdleStats();
						// release all the hold partitions
						while (!hold.isEmpty()) {
							PartitionReader p = hold.poll();
							putPartitionToQueue(p);
						}
					}
					//enable events visible for processing.
					stream.sink(cache);
				}catch(KafkaPartitionReaderException e){
					handlePartitionReaderException(preader, e);
				}catch (Throwable ex) {
					//TODO: to confirm all the exceptions.
					LOG.log(ex.getClass().getName()+" Occurred.");
					if(ex instanceof KafkaZKException){
						LOG.log("KafkaZKException: "+ex.getMessage());
					}else if(ex instanceof ClosedChannelException){
						LOG.log("Kafka Channel Closed.");
						try{
							if(preader!=null && zkConnector.isZkConnected()){
								preader.reinit();
							}else if(preader!=null){
								preader.lost();
							}
						}catch(Throwable t){
							preader.lost();
						}
					}else if (ex.getCause() instanceof ConnectionLossException) {
						LOG.log("Zookeeper connection is lost.", ex);
					} else if (ex instanceof ClosedByInterruptException
							|| ex instanceof InterruptedException
							|| ex.getCause() instanceof InterruptedException) {
						LOG.log( taskId + " is interrupted.",ex);
						preader.release();
					} else {
						LOG.log( "Unknown Exception: "+ex.getClass().getName(), ex);
					}

				} finally {
					try{
						if (preader != null) {
							if (preader.isTaken() && !preader.isClosed()) {
								// put back partition having events
								putPartitionToQueue(preader);
							} else {
								//finish(preader);
								releasePartition(preader.getTopic(),preader.getPartition());
							}
							preader = null;
						}
					}catch(Throwable t){
						LOG.log("Exception for partition:", t);
					}
				}

			}
		}
		private void putPartitionToQueue(PartitionReader p){
			try{
				queue.put(p);
			}catch(Exception e){
				LOG.log("Put partition reader to queue failed.",e);
			}
		}
		// hold partition by nextFetchInMs order
		private void hold(PartitionReader p) {
			int index = 0;
			for (index = 0; index < hold.size(); index++) {
				if (p.compareTo(hold.get(index)) < 0) {
					hold.add(index, p);
					break;
				}
			}
			if (index == hold.size())
				hold.addLast(p);
		}
		private void checkRelease() {
			List<PartitionReader> toRelease = new ArrayList<PartitionReader>();
			for (PartitionReader p : hold) {
				if (!p.isTaken()) {
					toRelease.add(p);
					releasePartition(p.getTopic(),p.getPartition());
					//finish(p);
				}
			}
			hold.removeAll(toRelease);
		}
	}
	
	private void resubscribe() {
		while (true) {
			try {
				unsubscribe();
				synchronized (lock) {
							LOG.log( "Start to subscribe for "
									+ consumerId);
						//decide coordinator strategy & regist consumer_id
						this.decideCoordinatorStrategy();

						NameableThreadFactory factory = new NameableThreadFactory(
								config.groupId());
						executor = Executors.newFixedThreadPool(
								config.numConsumerFetchers(), factory);

						consumerTasks = new CopyOnWriteArrayList<ConsumerTask>();

						for (int i = 0; i < config.numConsumerFetchers(); i++) {
							ConsumerTask task = new ConsumerTask();
							executor.submit(task);
							consumerTasks.add(task);
						}
						runningFlag.set(true);
					return;
				}

			} catch (Throwable ex) {
				LOG.log(ex.getMessage(), ex);
			}
		}
	}
	private void unsubscribe() {
		synchronized (lock) {
			LOG.log("Unsubscribe All...");
			ExecutorService oldExecutor = executor;
			
			if (!partitionMap.isEmpty()) {
				for (PartitionReader preader : partitionMap.values()) {
					preader.release();
				}
			}

			if (oldExecutor != null) {
				try {
					oldExecutor.shutdownNow();
				} catch (Throwable ex) {
				}
				long start = System.currentTimeMillis();
				try {
					// wait for gracefully shutdown each task
					oldExecutor.awaitTermination(300000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					LOG.log( "awaitTermination interrupted in ", e);
					oldExecutor.shutdownNow();
				}

				if (!oldExecutor.isTerminated()) {
					LOG.log("Timed out when shutdown consumer : "+ consumerId);
				} else {
					LOG.log("It takes "+ (System.currentTimeMillis() - start)+ " ms to gracefully unsubscribe "+ consumerId);
				}
			}

			// queue and pmap can only be cleared after partitions in them are
			// committed and finished
			if (!queue.isEmpty() || !partitionMap.isEmpty()) {
				LOG.log("Fail to gracefully finish all partitions before unsubscribe: "
								+ consumerId + ", queueSize="
								+ queue.size() + ", pmapSize="
								+ partitionMap.size());

				for (PartitionReader p : queue) {
					unsubscribePartition(p);
				}

				for (PartitionReader p : partitionMap.values()) {
					unsubscribePartition(p);
				}
			}

			queue.clear();
			partitionMap.clear();
			executor = null;
		}
	}
	
	public boolean takePartition(String topic,int partition) throws Exception{
			TopicAndPartition key = new TopicAndPartition(topic, partition);
			LOG.log("Start to take Partition: "+key+", consumerId="+consumerId);
			boolean success=true;
			if (!partitionMap.containsKey(key)) {
				//regist consumer in zk /consumers/[group]/owners/[topic]/[partition] --> consumerId and thread id.
				if(KafkaZKData.takePartition(zkConnector, topic, config.groupId(), partition, consumerId.toString())){
					
					PartitionReader preader = createPartitionReader(topic, partition);
					//boolean isNew = zkCoordinator.isNewPartition(topic, partition);
					if(preader!=null){
						preader.initOffset();
						partitionMap.put(key, preader);
						queue.put(preader);
						LOG.log(consumerId+" takes partition "+key);
						LOG.log( consumerId + " takes partition " + key);
					}else{
						LOG.log("Could NOT create PartitionReader for topic["+topic+"] partition["+partition+"] group["+config.groupId()+"].");
						KafkaZKData.releasePartition(zkConnector, topic, config.groupId(), partition, consumerId.toString());
						success=false;
					}
				}else{
					LOG.log("partition "+key+" has been taken by other consumer.");
					success=false;
				}
			} else {
				LOG.log( key + " has been in the map and queue of "
						+ consumerId + ", will not put it again.");
			}
			LOG.log("Finished to take Partition: "+key+", consumerId="+consumerId);
			return success;

	}
	public void releasePartition(String topic,int partition){
		try{
		TopicAndPartition key = new TopicAndPartition(topic, partition);
		LOG.log("Start to release Partition: "+key+", consumerId="+consumerId);
		if (partitionMap.containsKey(key)) {
			//regist consumer in zk /consumers/[group]/owners/[topic]/[partition] --> consumerId and thread id.
			//boolean isNew = zkCoordinator.isNewPartition(topic, partition);
			PartitionReader reader=partitionMap.remove(key);
			//queue.remove(reader);
			reader.release();
			reader.close();
			if(zkConnector.isZkConnected()){
				KafkaZKData.releasePartition(zkConnector, topic, config.groupId(), partition,consumerId.toString());
				LOG.log(consumerId+" release partition "+key);
			}else{
				LOG.log("Zookeeper is not connected. Release partition breaks.");
			}
		} else {
			LOG.log(consumerId+" haven't taken partition "+key);
		}
		LOG.log("Finished to release Partition: "+key+", consumerId="+consumerId);
		}catch(Exception t){
			LOG.log("release partition exception:",t);
		}
		
	}
	public void handlePartitionReaderException(PartitionReader reader,KafkaPartitionReaderException e){
		if(e instanceof KafkaPartitionReaderException){
		LOG.log("ErrorExcepiton:"+e.getCode());
		LOG.log(ErrorMapping.exceptionFor(e.getCode()).getClass().getName());
		try{
			if (e.getCode() == ErrorMapping.OffsetOutOfRangeCode()) {
				reader.resetOffset();
			}else if(e.getCode()==ErrorMapping.NotLeaderForPartitionCode()){
				reader.reinit();
			}else if(e.getCode()==ErrorMapping.LeaderNotAvailableCode()){
				try {
					Thread.sleep(config.refreshLeaderBackoffMs());
				} catch (InterruptedException e1) {
				}
				reader.reinit();
			}else {
				LOG.log("Un-predicated KafkaException:"+e.getCode());
				reader.reinit();
			}
		}catch(Exception t){
			LOG.log("Handle Kafka PartitionReader Exception.",e);
		}
		}else{
			
		}
		
	}
	private void unsubscribePartition(PartitionReader p) {
		if(!p.isClosed()){
			releasePartition(p.getTopic(),p.getPartition());
		}
	}

	public String getTopic(){
		return this.topic;
	}
	@Override
	public void rebalance() {
		//LOG.log("Trigger Consumer["+consumerId.toString()+"] rebalancing...");
		coordinator.coordinate(new DefaultAssignorContext(constroller,this));
	}
	@Override
	public void start(){
		if(this.runningFlag.compareAndSet(false, true)){
			LOG.log("Starting consumer: "+consumerId.toString());
			resubscribe();
			LOG.log("Consumer ["+consumerId.toString()+"] Started successfully.");
		}else{
			LOG.log("Consumer ["+consumerId.toString()+"] already started.");
		}
	}
	@Override
	public void stop(){
		if(this.runningFlag.compareAndSet(true, false)){
			LOG.log("Stoping Conumer: "+consumerId.toString());
			unsubscribe();
			LOG.log("Consumer ["+consumerId.toString()+"] stopped successfully.");
		}else{
			LOG.log("Consumer "+consumerId.toString()+"  already stopped.");
		}
	}
	@Override
	public boolean isRunning(){
		return runningFlag.get();
	}
	@Override
	public void commitOffset() throws Exception {
		long rcnt=readCount.getAndSet(0L);
		long ccnt=commitCount.getAndSet(0L);
		LOG.log("["+topic+"]Consumer Commit Offset: rcnt="+rcnt+",ccnt="+ccnt);;
		for(PartitionReader reader: partitionMap.values()){
			reader.commitOffset();
			//((ConsumerPartitionReader)reader).resetOffsetOrignal();
		}
	}
	@Override
	public void markCommitOffset(){
		long batchSize=0L;
		for(PartitionReader reader: partitionMap.values()){
			reader.markCommitOffset();
			batchSize+=reader.getBatchSize();
			//((ConsumerPartitionReader)reader).resetOffsetOrignal();
		}
		LOG.log("["+topic+"]Consumer BatchSize:"+batchSize);
		
	}
	@Override
	public void resetOffset(String topic, String group, int partition,
			long offset) {
		
	}
	@Override
	public Iterator<MessageAndMetadata<byte[],byte[]>> iterator() {
		return stream;
	}
	public ConsumerId getConsumerId() {
		return consumerId;
	}
	
	private PartitionReader createPartitionReader(String topic, int partition) {
		try{
			return new ConsumerPartitionReader(topic, partition, config, zkConnector);
		}catch(Exception e){
			LOG.log("Create partition reader exception.", e);
		}
		return null;
	}
	private void decideCoordinatorStrategy() throws ClassNotFoundException, InstantiationException, IllegalAccessException{
		//coordinator=new PartitionCoordinator();
		String partitionCoordinatorStrategyClass=null;
		if(config.partitionAssignmentStrategy()==null ||
			config.partitionAssignmentStrategy().length()==0){
			partitionCoordinatorStrategyClass="io.druid.firehose.kafka.support.DefaultPartitionCoordinator";
		}else{
			partitionCoordinatorStrategyClass=config.partitionAssignmentStrategy();
		}
		if("range".equalsIgnoreCase(partitionCoordinatorStrategyClass) ||
				"roundrobin".equalsIgnoreCase(partitionCoordinatorStrategyClass)){
			LOG.log("Change default  'partition.assignment.strategy'  ["+partitionCoordinatorStrategyClass+"] to [io.druid.firehose.kafka.support.DefaultPartitionCoordinator]" );
			partitionCoordinatorStrategyClass="com.ebay.pulsar.druid.firehose.kafka.support.DefaultPartitionCoordinator";
		}
		LOG.log("partition.assignment.strategy="+partitionCoordinatorStrategyClass);
		coordinator=instance(partitionCoordinatorStrategyClass);
		coordinator.init(config,topic,config.groupId(),consumerId.toString());
	}
	private PartitionCoordinatorStrategy instance(String clazz) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
		Class<?> clz=Class.forName(clazz);
    	if(PartitionCoordinatorStrategy.class.isAssignableFrom(clz)){
    		PartitionCoordinatorStrategy ret= PartitionCoordinatorStrategy.class.cast(clz.newInstance());
    		return ret;
    	}
    	throw new RuntimeException("class are not assignable to PartitionCoordinatorStrategy."+clazz);
	}
}