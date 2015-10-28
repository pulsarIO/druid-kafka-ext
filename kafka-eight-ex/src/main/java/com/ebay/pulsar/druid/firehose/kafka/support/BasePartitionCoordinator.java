/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ebay.pulsar.druid.firehose.kafka.support.api.AssignorContext;
import com.ebay.pulsar.druid.firehose.kafka.support.api.PartitionCoordinatorStrategy;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaConsumerConfig;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaZKData;
import com.ebay.pulsar.druid.firehose.kafka.support.data.RebalanceResult;
import com.ebay.pulsar.druid.firehose.kafka.support.util.LOG;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

/**
 *@author qxing
 * 
 **/
public abstract class BasePartitionCoordinator implements PartitionCoordinatorStrategy {
	protected String groupId;
	protected String topic;
	protected KafkaConsumerConfig config;
	protected Set<Integer> myPartitions;
	protected String consumerId;
	
	public Set<Integer> getMyPartitions(){
		return myPartitions;
	}
	@Override
	public void coordinate(AssignorContext ctx){
		//LOG.log("Check whether need to do rebalance.");
		//1. get all partitions for this topic-groupid;
		Set<Integer> partitions=KafkaZKData.allPartitionsOfTopic(ctx.getZkConnector(), topic);
		//2. get all consumers
		List<String> allConsumers=KafkaZKData.consumersOfTopic(ctx.getZkConnector(), groupId,topic);
		//3. calculate which partitions should be taken which consumer
		Map<String,Set<Integer>> view=KafkaZKData.partitionOwnerViewOfTopic(ctx.getZkConnector(), topic, groupId);
		//Fix BUGs that when zookeeper down and up again, partitions released in consumer context but znode still exist
		//in zookeeper.
		String self=ctx.getConsumer().getConsumerId().toString();
		Set<Integer> selfTaken=view.get(self);
		if(selfTaken==null) selfTaken=Sets.newHashSet();
		Set<Integer> diff=Sets.difference(selfTaken, getMyPartitions()).immutableCopy();
		selfTaken.removeAll(diff);
		if(selfTaken.size()!=getMyPartitions().size()){
			LOG.log("Kafka broker exception caused this occurred,try to recovery.....................,consumerId="+self);
			getMyPartitions().clear();
			getMyPartitions().addAll(selfTaken);
		}
		
		RebalanceResult rr=calculate(ctx.getConsumer().getConsumerId().toString(),partitions,allConsumers,view);
		boolean log=false;
		if(rr.getShouldTaken()>0 && rr.getShouldTaken()!=getMyPartitions().size()){
			LOG.log("Do rebalance...");
			LOG.log("Coordinating partitions: ["+topic+"] ["+consumerId+"]AllPartitions:"+Joiner.on(",").join(partitions));
			LOG.log("Coordinating partitions: ["+topic+"] ["+consumerId+"]myPartitions:"+Joiner.on(",").join(getMyPartitions()));
			LOG.log("Coordinating partitions: ["+topic+"] ["+consumerId+"]shouldTake:"+rr.getShouldTaken());
			log=true;
		}
		if(rr.getIdlePartitions().size()>0 && rr.getShouldTaken()>getMyPartitions().size()){
			for(Integer p: rr.getIdlePartitions()){
				if(beforeTakePartition(topic,p)){
					try{
						if(ctx.getConsumer().takePartition(topic, p))
							afterTakePartition(topic,p);
						else{
							LOG.log("Take partition Failed.partition="+p);
						}
					}catch(Throwable t){
						t.printStackTrace();
					}
				}
				if(rr.getShouldTaken()<=getMyPartitions().size()) break;
			}
		}
		if(rr.getShouldRelease().size()>0 && rr.getShouldTaken()<getMyPartitions().size()){
			Integer[] mine=rr.getShouldRelease().toArray(new Integer[]{});
			for(int i=0;i<mine.length;i++){
				if(beforeReleasePartition(topic,mine[i])){
					try{
						ctx.getConsumer().releasePartition(topic, mine[i]);
						afterReleasePartition(topic,mine[i]);
					}catch(Throwable t){
						LOG.log("Release Exception for partition:"+mine[i]);
					}
				}
				if(rr.getShouldTaken()>=getMyPartitions().size()) break;
			}
		}
		if(log){
			LOG.log("Finished rebalance...");
		}else{
			//LOG.log("No need to do rebalance.");
		}
	}
	
	@Override
	public void init(KafkaConsumerConfig config, String topic,String groupId,String consumerId) {
		myPartitions=Sets.newConcurrentHashSet();//.newHashSet();
		this.config=config;
		this.groupId=groupId;
		this.topic=topic;
		this.consumerId=consumerId;
	}
	@Override
	public void afterTakePartition(String topic, int partition) {
		//You could do any after process for this partition being taken 
		//e.g. keep the taken partitions for coordinate use.
		getMyPartitions().add(partition);
	}
	@Override
	public boolean beforeTakePartition(String topic, int partition) {
		//You could make decision whether will take this partition.
		//return true indicate you will take it finally. otherwise you won't take it.
		return true;
	}
	public boolean beforeReleasePartition(String topic, int partition){
		return true;
	}
	public void afterReleasePartition(String topic, int partition){
		getMyPartitions().remove(partition);
	}
	/**
	 * implement this method to give the <code>RebalanceResult</code>
	 * coordinator will invoke this method, passing the current informations like consumerId, 
	 * allPartitions, allConsumers and current partition assignment view.
	 * 
	 * From all the information aware, it should work out how many partitions should be taken by 
	 * this consumer and all the idle partitions, as well as all the release-able partitions if over taken.
	 * 
	 * 
	 * @param consumerId  current consumer id
	 * @param allPartitions: all partitions for this topic
	 * @param allConsumers: all registed consumers 
	 * @param view: key is consumer id, and value is a set of partitions taken by that consumer.
	 * @return total partitions count should be taken, all idle partitions, all the partitions that can be release if needed.
	 */
	public abstract RebalanceResult calculate(String consumerId, Set<Integer> allPartitions,List<String> allConsumers,Map<String,Set<Integer>> view);

}
