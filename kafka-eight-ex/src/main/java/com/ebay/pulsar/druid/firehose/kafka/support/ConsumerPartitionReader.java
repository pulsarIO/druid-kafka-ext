/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support;

import static kafka.api.OffsetRequest.CurrentVersion;
import static kafka.api.OffsetRequest.EarliestTime;
import static kafka.api.OffsetRequest.LargestTimeString;
import static kafka.api.OffsetRequest.LatestTime;
import static kafka.api.OffsetRequest.SmallestTimeString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.ebay.pulsar.druid.firehose.kafka.support.api.PartitionReader;
import com.ebay.pulsar.druid.firehose.kafka.support.api.ZKConnector;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaConsumerConfig;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaZKData;
import com.ebay.pulsar.druid.firehose.kafka.support.exceptions.KafkaPartitionReaderException;
import com.ebay.pulsar.druid.firehose.kafka.support.util.LOG;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import kafka.serializer.Decoder;

/**
 *@author qxing
 * 
 **/
public class ConsumerPartitionReader implements PartitionReader  {
	private final ZKConnector<?> zkConnector;
	private final String topic;
	private final String groupId;
	private final int partition;
	private final String clientId;
	private final KafkaConsumerConfig config;

	//private final String topicPartitionLeaderAndIsrPath;
	private final String offsetPath;

	private Broker leader=null;
	private SimpleConsumer consumer;
	private long readOffset = -1L;
	private AtomicLong readUnCommitOffset=new AtomicLong(-1L);
	private AtomicLong commitOffset=new AtomicLong(-1L);
	private long batchSize=0L;
	private int nextBatchSizeBytes = -1;
	private final AtomicBoolean taken = new AtomicBoolean(true);
	private long nextFetchInMs = 0;
	private long idleTimestamp = 0;
	private boolean onIdled = false;
	private AtomicBoolean closed=new AtomicBoolean(false);
	private RawKafkaKVDecoder decoder=new RawKafkaKVDecoder();
	public ConsumerPartitionReader(String topic, int partition,KafkaConsumerConfig config,ZKConnector<?> zkConnector) {
		//LOG.log("Start to create PartitionReader for Topic["+topic+"] partition["+partition+"]");
		this.groupId = config.groupId();
		this.topic = topic;
		this.partition = partition;
		//this.topicPartitionLeaderAndIsrPath = KafkaZKData.topicPartitionLeaderAndIsrPath(topic, partition);
		this.offsetPath =new KafkaZKData.ZKGroupTopicDirs(groupId, topic).partitionOffset(partition);
		this.clientId = this.topic + "_" + this.partition;
		this.config = config;
		this.zkConnector = zkConnector;
		this.init();
		//LOG.log("End to create PartitionReader for Topic["+topic+"] partition["+partition+"]");
	}
	public ConsumerPartitionReader startFrom(long offset){
		this.readOffset=offset;
		//LOG.log("Start Read Offset from ["+offset+"]  for PartitionReader, Topic["+topic+"] partition["+partition+"]");
		return this;
	}
	public class RawKafkaKVDecoder implements Decoder<byte[]>{
		@Override
		public byte[] fromBytes(byte[] raw) {
			return raw;
		}
	}
	private void init() {
		//LOG.log("Start to Init PartitionReader, Topic["+topic+"] partition["+partition+"]");
		if(consumer!=null){
			//LOG.log("Start to Close PartitionReader, Topic["+topic+"] partition["+partition+"]");
			consumer.close();
			//LOG.log("Closed  for PartitionReader, Topic["+topic+"] partition["+partition+"]");
		}
		int retryTimes=config.retryTimes();
		do{//Fixed when topic partition only has on replicas, the broker down, we couldn't get the leader for this partitions.
			this.leader =KafkaZKData.getBrokerInfo(zkConnector, 
				KafkaZKData.leaderForPartition(zkConnector, topic, partition));
			if(this.leader==null) {
				try{
					Thread.sleep(config.sleepMsBetweenRetries());
				}catch(Exception ignore){
					
				}
			}
			retryTimes--;
		}while(!Thread.currentThread().isInterrupted() && (retryTimes>0) && this.leader==null);
		if(this.leader==null) throw new KafkaPartitionReaderException(ErrorMapping.LeaderNotAvailableCode(),"Leader not available for partition["+partition+"] topic["+topic+"]");
		this.consumer = new SimpleConsumer(leader.host(), leader.port(),
				config.socketTimeoutMs(),
				config.socketReceiveBufferBytes(), clientId);
		this.nextBatchSizeBytes = config.fetchMinBytes();//config.getBatchSizeBytes();
		this.closed.set(false);
		taken.set(true);

	}
	public void reinit(){
		//LOG.log("Start to ReInit PartitionReader, Topic["+topic+"] partition["+partition+"]");
		init();
		//LOG.log("Finished to ReInit PartitionReader, Topic["+topic+"] partition["+partition+"]");
	}
	/**
	 * 	read events.
	 * 
	 * any errors occurred druing the read process are wrapped as KafkaPartitionReaderException which contains the error code
	 * the exception should be processed by consumer.
	 * 
	 * @return
	 * @throws KafkaPartitionReaderException
	 */
	public List<MessageAndMetadata<byte[],byte[]>> readEvents() throws KafkaPartitionReaderException {
		List<MessageAndMetadata<byte[],byte[]> > events = new ArrayList<MessageAndMetadata<byte[],byte[]>>();
		if(isClosed()){
			return events;
		}
		//LOG.log("Start Reading PartitionReader from ["+readOffset+"] once, Topic["+topic+"] partition["+partition+"]");
		if (nextBatchSizeBytes < 0)
			nextBatchSizeBytes = config.fetchMinBytes();//config.getBatchSizeBytes();

		if (nextBatchSizeBytes == 0) {
			// nextBatchSize only affects one fetch
			nextBatchSizeBytes = config.fetchMinBytes();//config.getBatchSizeBytes();
			return events;
		}

		boolean  hasMessage=false;
		ByteBufferMessageSet messageSet=null;
		do{
			FetchRequest req = new FetchRequestBuilder()
			.clientId(clientId)
			.addFetch(topic, partition, readOffset,
					nextBatchSizeBytes).build();

			FetchResponse fetchResponse = null;
			fetchResponse = consumer.fetch(req);
			if (fetchResponse.hasError()) {
				short code = fetchResponse.errorCode(topic, partition);
				throw new KafkaPartitionReaderException(code);
			} else {
				messageSet = fetchResponse.messageSet(topic, partition);
				hasMessage = messageSet.iterator().hasNext();
				if(!hasMessage)
				nextBatchSizeBytes = Math.min(
						nextBatchSizeBytes * 2,config.fetchMessageMaxBytes()
						/*config.getMaxBatchSizeBytes()*/);
			}
		}while(!hasMessage && !readToTheEnd());//TODO: test readToTheEnd() , consider the config.getMaxBatchSizeBytes().
		if(!hasMessage){
			//set this reader on idle.
			onIdle();
			nextBatchSizeBytes =config.fetchMinBytes();// config.getBatchSizeBytes();
			return events;//return empty events.
		}
		for (MessageAndOffset messageAndOffset : messageSet) {
			long currentOffset = messageAndOffset.offset();
			if (currentOffset < readOffset) {
				continue;
			}
			readOffset = messageAndOffset.nextOffset();
			Message message = messageAndOffset.message();
			MessageAndMetadata<byte[],byte[]> mam=new MessageAndMetadata<byte[],byte[]>(topic, partition, message, readOffset, decoder, decoder);
			events.add(mam);
		
		}
		// nextBatchSize only affects one fetch
		nextBatchSizeBytes = config.fetchMinBytes();//config.getBatchSizeBytes();
		return events;
	}
	public boolean readToTheEnd() {
		return readOffset >= getLargestOffset();
	}
	public long getLargestOffset() {
		return fetchResetOffset(LargestTimeString());
	}
	public long fetchSmallestOffset(){
		return fetchResetOffset(SmallestTimeString());
	}
	
	public long fetchResetOffset(String reset){
		long time = LatestTime();
		if (reset != null && reset.equals(SmallestTimeString()))
			time = EarliestTime();
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		TopicAndPartition tp = new TopicAndPartition(topic, partition);
		PartitionOffsetRequestInfo info = new PartitionOffsetRequestInfo(time,1);
		requestInfo.put(tp, info);
		OffsetRequest request = new OffsetRequest(requestInfo,CurrentVersion(), clientId);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			//ErrorMapping.exceptionFor(response.errorCode(topic, partition)).printStackTrace();
			throw new KafkaPartitionReaderException(response.errorCode(topic, partition));
		}
		long[] offsets = response.offsets(topic, partition);
		//TODO: confirm with xiaoju why we need this check?
//		if (offsets.length <= 0)
//			continue;
		return offsets[0];
	}
	
	public void commitOffset() throws Exception {
		long v=commitOffset.get();
		if(v>=0){//once the reader is created and haven't reset the offset or some exception occurred when reset the commitOffset/readOffset, 
			        //no proper readOffset was set, so the default value is -1, at this time,when closing the consumer or closing the partition reader,
			        //it will commit the invalid offset[-1] to zookeeper. thus, the next time or when other consumers take this partition, it will reset 
			       // the readOffset to config.autoOffsetReset(). this will cause event lost.
			zkConnector.updatePersistentPath(offsetPath, String.valueOf(v));
			
		}else{
			LOG.log("Give up commit invalid offset["+v+"] for partition reader. Topic["+topic+"] partition["+partition+"]");
		}
		//LOG.log("Commit Offset["+v+"] UnCommitOffset["+readUnCommitOffset.get()+"] for PartitionReader, Topic["+topic+"] partition["+partition+"]");
	}
	public void markCommitOffset(){
		long mark=readUnCommitOffset.get();
		batchSize=mark-commitOffset.get();
		commitOffset.set(mark);
	}
	public long getBatchSize(){
		return batchSize;
	}

	public void initOffset() throws Exception {
		if(isClosed()) return;
		boolean isOffsetExist = zkConnector.pathExists(offsetPath);//KafkaZKInfo.pathExists(zkConnector, offsetPath);
		boolean needCommit=false;
		if (!isOffsetExist) {
			readOffset = fetchResetOffset(SmallestTimeString());
			LOG.log("InitOffset to EarliestTime time for partition reader. Topic["+topic+"] partition["+partition+"]");
			needCommit=true;
			// commit the init offset first, useful to revert
		} else {
			if(zkConnector.pathExists(offsetPath)){
				readOffset = KafkaZKData.getOffset(zkConnector,offsetPath);
			}
			if (readOffset <=0) {
				readOffset = fetchResetOffset(config.autoOffsetReset());
				if(config.autoOffsetReset() != null && config.autoOffsetReset().equals(LargestTimeString())){
					LOG.log("InitOffset to Largest for partition reader. Topic["+topic+"] partition["+partition+"]");
				}else{
					LOG.log("InitOffset to EarliestTime for partition reader. Topic["+topic+"] partition["+partition+"]");
				}
				needCommit=true;
			}
			
		}
		if (readOffset < 0)
			throw new RuntimeException(
					"Fatal errors in getting init offset for " + clientId);
		if(readOffset<commitOffset.get()){
			readOffset=commitOffset.get();
			needCommit=true;
		}else{
			
			commitOffset.set(readOffset);
		}
		if(needCommit)		commitOffset();
		readUnCommitOffset.set(readOffset);
		//LOG.log("Init Offset["+commitOffset.get()+"] for PartitionReader, Topic["+topic+"] partition["+partition+"]");

	}
	public void resetOffset() throws Exception {
		if(isClosed()) return;
		if(config.autoOffsetReset()!=null && config.autoOffsetReset().equals(LargestTimeString())){
			LOG.log("Reset offset to LatestTime for partition reader. Topic["+topic+"] partition["+partition+"]");
		}else{
			LOG.log("Reset offset to EarliestTime for partition reader. Topic["+topic+"] partition["+partition+"]");
		}
		readOffset = fetchResetOffset(config.autoOffsetReset());
		if(readOffset<commitOffset.get()){
			readOffset=commitOffset.get();
		}else{
			commitOffset.getAndSet(readOffset);
		}
		if (readOffset < 0)
			throw new RuntimeException("Fatal errors in resetting offset for "
					+ clientId);
		commitOffset();
		readUnCommitOffset.set(readOffset);
		//LOG.log("Reset Offset["+commitOffset.get()+"] for PartitionReader, Topic["+topic+"] partition["+partition+"]");

	}
	public long getAndSetCommitOffset(long val){
		return readUnCommitOffset.getAndSet(val);
	}
	
	public boolean isTaken(){
		return taken.get();
	}
	public void release() {
		taken.set(false);
	}
	
	public long calcWaitTime() {
		return nextFetchInMs - System.currentTimeMillis();
	}
	
	public long getReadOffset() {
		return readOffset;
	}
	public void setReadOffset(long readOffset) {
		this.readOffset = readOffset;
	}

	public String getGroupId() {
		return groupId;
	}
	public String getClientId() {
		return clientId;
	}
	public String getTopic() {
		return topic;
	}
	public int getPartition() {
		return partition;
	}


	public void onIdle(){
		idleTimestamp = System.currentTimeMillis();
		nextFetchInMs=config.fetchWaitMaxMs()/*config.getFetchWaitMaxMs()*/+idleTimestamp;
		onIdled = true;
	}

	public boolean isOnIdled() {
		return onIdled;
	}

	public void clearIdleStats() {
		idleTimestamp = 0;
		nextFetchInMs=0;
		onIdled = false;
	}
	public void close() throws Exception {
		//LOG.log("Closing PartitionReader, Topic["+topic+"] partition["+partition+"]");
		this.closed.set(true);
		release();
		consumer.close();
		commitOffset();
		//LOG.log("Closed PartitionReader, Topic["+topic+"] partition["+partition+"]");
	}
	public boolean isClosed(){
		return this.closed.get();
	}
	public void lost() {
		taken.set(false);
	}
	public boolean isLost() {
		return taken.get();
	}	
	@Override
	public int compareTo(PartitionReader o) {
		ConsumerPartitionReader p=(ConsumerPartitionReader)o;
		if (nextFetchInMs < p.nextFetchInMs)
			return -1;
		else if (nextFetchInMs > p.nextFetchInMs)
			return 1;
		else
			return 0;
	}
}
