/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.data;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import kafka.api.LeaderAndIsr;
import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.common.TopicAndPartition;
import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.utils.Json;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

import com.ebay.pulsar.druid.firehose.kafka.support.api.ZKConnector;
import com.ebay.pulsar.druid.firehose.kafka.support.exceptions.KafkaZKException;
import com.ebay.pulsar.druid.firehose.kafka.support.util.JSON;
import com.ebay.pulsar.druid.firehose.kafka.support.util.TopicCount;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

/**
 * @author qxing
 * 
 **/
public class KafkaZKData {
	public static String ConsumersPath = "/consumers";
	public static String BrokerIdsPath = "/brokers/ids";
	public static String BrokerTopicsPath = "/brokers/topics";
	public static String TopicConfigPath = "/config/topics";

	public static String TopicConfigChangesPath = "/config/changes";
	public static String ControllerPath = "/controller";
	public static String ControllerEpochPath = "/controller_epoch";
	public static String ReassignPartitionsPath = "/admin/reassign_partitions";
	public static String DeleteTopicsPath = "/admin/delete_topics";
	public static String PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election";

	public static ObjectMapper mapper = new ObjectMapper();
	public static TypeReference<Map<String, Object>> typeMap = new TypeReference<Map<String, Object>>() {
	};
	//public static ZKStringSerializer serializer=new ZKStringSerializer();

	public static String topicPath(String topic) {
		return BrokerTopicsPath + "/" + topic;
	}

	public static String topicPartitionsPath(String topic) {
		return topicPath(topic) + "/partitions";
	}

	public static String topicConfigPath(String topic) {
		return TopicConfigPath + "/" + topic;
	}

	public static String deleteTopicPath(String topic) {
		return DeleteTopicsPath + "/" + topic;
	}

	public static String topicPartitionPath(String topic, int partitionId) {
		return topicPartitionsPath(topic) + "/" + partitionId;
	}

	public static String topicPartitionLeaderAndIsrPath(String topic,
			int partitionId) {
		return topicPartitionPath(topic, partitionId) + "/" + "state";
	}

	public static String partitionOwnerPath(String group, String topic,
			int partition) {
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(group, topic);
		return topicDirs.consumerOwnerDir() + "/" + partition;
	}
	public static Integer controller(ZKConnector<?> zkClient) {
		try{
			String data= zkClient.readDataMaybeNull(ControllerPath);
			checkState(data!=null,"Controller data is empty.");
			Map<String, Object> jsonObj = mapper.reader(typeMap).readValue(data);
			checkState(jsonObj!=null,"");
			Object id = jsonObj.get("brokerid");
			checkState(id!=null,"broker id does not exist.");
			return (Integer)id;
		}catch(Exception e){
			 throw new KafkaZKException(String.format("Failed to get controller data"),e);
		}
	}
	public static List<String> sortedBrokerList(ZKConnector<?> zkClient) {
		try{
			List<String> list=zkClient.getChildren(BrokerIdsPath);
			Collections.sort(list);
			return list;
		}catch(Throwable t){
			throw new KafkaZKException(t);
		}
	}

	public static List<Broker> allBrokersInCluster(final ZKConnector<?> zkClient) {
		try{
			List<String> brokerIds = Ordering.natural().sortedCopy(zkClient.getChildrenParentMayNotExist(BrokerIdsPath));
			return FluentIterable.from(brokerIds).transform(new Function<String,Broker>(){
				@Override
				public Broker apply(String input) {
					Integer i = Integer.parseInt(input);
					return getBrokerInfo(zkClient, i);
				}
			}).filter(new Predicate<Broker>() {
				@Override
				public boolean apply(Broker input) {
					return input != null;
				}
			}).toList();
		}catch(Exception e){
			return Lists.newArrayList();
		}
		
	}

	public static long getOffset(ZKConnector<?> zkClient, String path) {
		try {
			if (!zkClient.pathExists(path))
				return -1;
			String offsetStr = zkClient.readData(path);
			if (offsetStr == null)
				return -1;
			return Long.parseLong(offsetStr);
		} catch (Throwable t) {
			throw new KafkaZKException(t);
		} 
	}

	public static Set<Integer> allPartitionsOfTopic(ZKConnector<?> client,
			String topic) {
		try{
			Set<Integer> partitions = Sets.newHashSet();//
			String path = topicPartitionsPath(topic);
			List<String> pstrs = client.getChildrenParentMayNotExist(path);
			if (pstrs == null || pstrs.isEmpty()) {
				return partitions;
			}
			return ImmutableSet.copyOf(Iterables.transform(pstrs,
					new Function<String, Integer>() {
						@Override
						public Integer apply(String input) {
							return Integer.parseInt(input);
						}
				}));
		}catch(Throwable t){
			throw new KafkaZKException(t);
		}
	}

//	 def getConsumersPerTopic(zkClient: ZkClient, group: String, excludeInternalTopics: Boolean) : mutable.Map[String, List[ConsumerThreadId]] = {
//			    val dirs = new ZKGroupDirs(group)
//			    val consumers = getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir)
//			    val consumersPerTopicMap = new mutable.HashMap[String, List[ConsumerThreadId]]
//			    for (consumer <- consumers) {
//			      val topicCount = TopicCount.constructTopicCount(group, consumer, zkClient, excludeInternalTopics)
//			      for ((topic, consumerThreadIdSet) <- topicCount.getConsumerThreadIdsPerTopic) {
//			        for (consumerThreadId <- consumerThreadIdSet)
//			          consumersPerTopicMap.get(topic) match {
//			            case Some(curConsumers) => consumersPerTopicMap.put(topic, consumerThreadId :: curConsumers)
//			            case _ => consumersPerTopicMap.put(topic, List(consumerThreadId))
//			          }
//			      }
//			    }
//			    for ( (topic, consumerList) <- consumersPerTopicMap )
//			      consumersPerTopicMap.put(topic, consumerList.sortWith((s,t) => s < t))
//			    consumersPerTopicMap
//			  }

	
	public static Map<String, Set<Integer>> partitionOwnerViewOfTopic(
			final ZKConnector<?> zkClient, String topic, String groupId) {
		Map<String, Set<Integer>> partitionOwnerView = Maps.newHashMap();
		try{
			String path = topicPartitionsPath(topic);
			List<String> pstrs = zkClient.getChildrenParentMayNotExist(path);
			if (pstrs == null || pstrs.isEmpty()) {
				return partitionOwnerView;
			}
			for (String pn : pstrs) {
				ZKGroupTopicDirs gt = new ZKGroupTopicDirs(groupId, topic);
				String ownerId = zkClient.readDataMaybeNull(gt.consumerOwnerDir()
						+ "/" + pn);
				if (ownerId != null) {
					Set<Integer> set = partitionOwnerView.get(ownerId);
					if (set == null) {
						set = Sets.newHashSet();
						partitionOwnerView.put(ownerId, set);
					}
					set.add(Integer.parseInt(pn));
				}
			}
		}catch(Throwable t){
			throw new KafkaZKException(t);
		}
		return partitionOwnerView;
	}

	public static Integer leaderForPartition(ZKConnector<?> zkClient, String topic,
			int partition) {
		try {
			String leaderAndIsrOpt = zkClient.readDataMaybeNull(topicPartitionLeaderAndIsrPath(topic, partition));
			checkNotNull(leaderAndIsrOpt);
			Map<String, Object> jsonObj = mapper.reader(typeMap).readValue(
					leaderAndIsrOpt);
			checkNotNull(jsonObj);
			Object leader = jsonObj.get("leader");
			checkNotNull(leader);
			return Integer.parseInt(leader.toString());
		} catch (Exception e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static LeaderIsrAndControllerEpoch leaderIsrAndEpochForPartition(
			ZKConnector<?> zkClient, String topic, int partition) {
		
		try{
			String leaderAndIsrPath = topicPartitionLeaderAndIsrPath(topic, partition);
			Pair<String,Stat> leaderAndIsrInfo =zkClient.readDataMaybeNullWithStat(leaderAndIsrPath);
			Map<String, Object> jsonObj = mapper.reader(typeMap).readValue(leaderAndIsrInfo.lhs);
			Integer leader=(Integer)jsonObj.get("leader");
			Integer epoch=(Integer)jsonObj.get("leader_epoch");
			List<Object> isr=(List<Object>)jsonObj.get("isr");
			Integer controllerEpoch=(Integer)jsonObj.get("controller_epoch");
			return new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader,epoch,(scala.collection.immutable.List<Object>) isr,leaderAndIsrInfo.rhs.getVersion()),controllerEpoch);
		}catch(Exception e){
			
		}
		return  null;
	}

	public static LeaderAndIsr leaderAndIsrForPartition(ZKConnector<?> zkClient,
			String topic, int partition) {
		LeaderIsrAndControllerEpoch entity = leaderIsrAndEpochForPartition(
				zkClient, topic, partition);
		if (entity != null)
			return entity.leaderAndIsr();
		return null;
	}

	public static void registerConsumer(ZKConnector<?> zkClient, String topic,
			String groupId, String consumerId) {
		ZKGroupTopicDirs dirs = new ZKGroupTopicDirs(groupId, topic);
		TopicCount topicCount = TopicCount.constructTopicCount(consumerId,
				ImmutableMap.of(topic, 1));
		String timestamp = String.valueOf(System.currentTimeMillis());
		ImmutableMap<String, Object> data = ImmutableMap.of("version", 1, "subscription",
				topicCount.getTopicCountMap(), "pattern", topicCount.pattern(),
				"timestamp", timestamp);
		String consumerRegistrationInfo = JSON.toJSONString(data);

		createEphemeralPathExpectConflictHandleZKBug(zkClient,
				dirs.consumerRegistryDir() + "/" + consumerId,
				consumerRegistrationInfo, consumerId,
				new Function<Pair<String, Object>, Boolean>() {
					@Override
					public Boolean apply(Pair<String, Object> input) {
						return true;
					}
				}, 5000);
	}
	public static void unregisterConsumer(ZKConnector<?> zkClient,String topic,String groupId,String consumerId){
		try{
		ZKGroupTopicDirs dirs = new ZKGroupTopicDirs(groupId, topic);
		zkClient.deletePath(dirs.consumerRegistryDir()+"/"+consumerId);
		Map<String,Set<Integer>> view=KafkaZKData.partitionOwnerViewOfTopic(zkClient, topic, groupId);
		Set<Integer> partitions=view.get(consumerId);
		if(partitions!=null && partitions.size()>0){
			for(Integer p: partitions){
				releasePartition(zkClient,topic,groupId,p,consumerId);
			}
		}
		}catch(Throwable t){
			throw new KafkaZKException(t);
		}
	}

	public static boolean takePartition(ZKConnector<?> zkClient, String topic,
			String groupId, int partition, String consumerId) {
		try {
			ZKGroupTopicDirs dirs = new ZKGroupTopicDirs(groupId, topic);
			zkClient.makeSurePersistentPathExists(dirs.consumerOwnerDir());
			// TODO: check the consumer register infor
			//throw new RuntimeException("Debug.");
			KafkaZKData.createEphemeralPathExpectConflictHandleZKBug(zkClient,
					dirs.consumerOwnerDir() + "/" + partition, consumerId,
					consumerId, new Function<Pair<String,Object>,Boolean>(){
						@Override
						public Boolean apply(Pair<String, Object> input) {
							return input.rhs.equals(input.lhs);
						}
				
			}, 5000);
			return true;
		} catch (Throwable t) {
			return false;
		}
	}

	public static void releasePartition(ZKConnector<?> zkClient, String topic,
			String groupId, int partition, String consumerId) {
		ZKGroupTopicDirs dirs = new ZKGroupTopicDirs(groupId, topic);
		String ownerPath = dirs.consumerOwnerDir() + "/" + partition;
		try{
			if (zkClient.pathExists(ownerPath)
					&& consumerId.equals(zkClient.readData(ownerPath))) {
				zkClient.deletePath(ownerPath);
			}
		}catch(Exception e){
			e.printStackTrace();
			//exception when communicate zookeeper, return normally.
		}
	}

	public static void setupCommonPaths(ZKConnector<?> zkClient) throws Exception {
		for (String path : ImmutableList.of(ConsumersPath, BrokerIdsPath,
				BrokerTopicsPath, TopicConfigChangesPath, TopicConfigPath,
				DeleteTopicsPath)) {
			zkClient.makeSurePersistentPathExists(path);
		}
	}

	/**
	 * This API should read the epoch in the ISR path. It is sufficient to read
	 * the epoch in the ISR path, since if the leader fails after updating epoch
	 * in the leader path and before updating epoch in the ISR path, effectively
	 * some other broker will retry becoming leader with the same new epoch
	 * value.
	 * @throws Exception 
	 */
	public static Integer epochForPartition(ZKConnector<?> zkClient, String topic,
			int partition) {
		LeaderAndIsr isr = leaderAndIsrForPartition(zkClient, topic, partition);
		if (isr != null)
			return isr.leaderEpoch();
		else
			throw new KafkaZKException(String.format(
					"No epoch, ISR path for partition [%s,%d] is empty", topic,
					partition));
	}

	@SuppressWarnings("unchecked")
	public static List<Integer> isrForPartition(ZKConnector<?> zkClient,
			String topic, int partition) {
		LeaderAndIsr isr = leaderAndIsrForPartition(zkClient, topic, partition);
		if (isr == null)
			throw new KafkaZKException(
					String.format(
							"No epoch, leaderAndISR data for partition [%s,%d] is invalid",
							topic, partition));
		return (List<Integer>) isr.isr();
	}

	public static List<Integer> assignedReplicasForPartition(ZKConnector<?> zkClient,
			String topic, int partition) {
		return assignedReplicasView(zkClient, topic).get(
				String.valueOf(partition));
	}

	@SuppressWarnings("unchecked")
	public static Map<String, List<Integer>> assignedReplicasView(
			ZKConnector<?> zkClient, String topic) {
		try {
			String jsonPartitionMapOpt = zkClient.readDataMaybeNull(	topicPath(topic));
			checkNotNull(jsonPartitionMapOpt);
			Map<String, Object> jsonObj = mapper.reader(typeMap).readValue(
					jsonPartitionMapOpt);
			checkNotNull(jsonObj);
			Map<String, List<Integer>> partitions = (Map<String, List<Integer>>) jsonObj
					.get("partitions");
			checkNotNull(partitions);
			return partitions;
		} catch (Exception e) {
			return Maps.newHashMap();
		}
	}

	public static String leaderAndIsrZkData(LeaderAndIsr leaderAndIsr,
			int controllerEpoch) {
		return Json.encode(ImmutableMap.of("version", 1, "leader",
				leaderAndIsr.leader(), "leader_epoch",
				leaderAndIsr.leaderEpoch(), "controller_epoch",
				controllerEpoch, "isr", leaderAndIsr.isr()));
	}

	/**
	 * Get JSON partition to replica map from zookeeper.
	 */
	public static String replicaAssignmentZkData(Map<String, List<Integer>> map) {
		return Json.encode(ImmutableMap.of("version", 1, "partitions", map));
	}

	/**
	 * Create an ephemeral node with the given path and data. Throw
	 * NodeExistException if node already exists.
	 * @throws NodeExistsException 
	 * @throws Throwable 
	 */
	public static void createEphemeralPathExpectConflict(ZKConnector<?> client,
			String path, String data) throws NodeExistsException{
		try {
			client.createEphemeralPath(path, data);
		} catch ( KeeperException.NodeExistsException e) {
			// this can happen when there is connection loss; make sure the data
			// is what we intend to write
			String storedData = null;
			try {
				storedData = client.readData(path);
			}catch (Throwable t) {
				throw new KafkaZKException(t);
			}
			if (storedData == null || storedData != data) {
				// info("conflict in " + path + " data: " + data +
				// " stored data: " + storedData)
				throw  e ;
			} else {
				// otherwise, the creation succeeded, return normally
				// info(path + " exists with value " + data +
				// " during connection loss; this is ok")
			}
		} catch (Throwable t) {
			throw new KafkaZKException(t);
		}
	}

	/**
	 * Create an ephemeral node with the given path and data. Throw
	 * NodeExistsException if node already exists. Handles the following ZK
	 * session timeout bug:
	 *
	 * https://issues.apache.org/jira/browse/ZOOKEEPER-1740
	 *
	 * Upon receiving a NodeExistsException, read the data from the conflicted
	 * path and trigger the checker function comparing the read data and the
	 * expected data, If the checker function returns true then the above bug
	 * might be encountered, back off and retry; otherwise re-throw the
	 * exception
	 * @throws NodeExistsException 
	 * @throws Exception 
	 */
	public static void createEphemeralPathExpectConflictHandleZKBug(
			ZKConnector<?> zkClient, String path, String data,
			Object expectedCallerData,
			Function<Pair<String, Object>, Boolean> checker, int backoffTime) {
				
		while (true) {
			try {
				createEphemeralPathExpectConflict(zkClient, path, data);
				return;
			} catch (KeeperException.NodeExistsException e) {
				// An ephemeral node may still exist even after its
				// corresponding session has expired
				// due to a Zookeeper bug, in this case we need to retry writing
				// until the previous node is deleted
				// and hence the write succeeds without ZkNodeExistsException
				try{
				String writtenData = zkClient.readDataMaybeNull(path);
				if (writtenData != null) {
					if (checker.apply(Pair.of(writtenData, expectedCallerData))) {
						// info("I wrote this conflicted ephemeral node [%s] at %s a while back in a different session, ".format(data,
						// path)
						// +
						// "hence I will backoff for this node to be deleted by Zookeeper and retry");
						try {
							zkClient.deletePath(path);
							Thread.sleep(backoffTime);
						} catch (Throwable e1) {
							e1.printStackTrace();
						}
					} else {
						throw new KafkaZKException(e);
					}
				}
				}catch(Throwable t){
					throw new KafkaZKException(t);
				}

			} catch (Throwable t) {
				throw new KafkaZKException(t);
			}
		}
	}

//
//	/**
//	 * Conditional update the persistent path data, return (true, newVersion) if
//	 * it succeeds, otherwise (the path doesn't exist, the current version is
//	 * not the expected version, etc.) return (false, -1)
//	 *
//	 * When there is a ConnectionLossException during the conditional update,
//	 * zkClient will retry the update and may fail since the previous update may
//	 * have succeeded (but the stored zkVersion no longer matches the expected
//	 * one). In this case, we will run the optionalChecker to further check if
//	 * the previous write did indeed succeeded.
//	 */
//	public static Pair<Boolean, Integer> conditionalUpdatePersistentPath(
//			ZKConnector<?> zkClient,
//			String path,
//			String data,
//			int expectVersion,
//			Function<Triple<ZKConnector<?>, String, String>, Pair<Boolean, Integer>> checker) {
//		try {
//			Stat stat=zkClient.setData().withVersion(expectVersion).forPath(path, serializer.serialize(data));
//			//Stat stat = zkClient.writeDataReturnStat(path, data, expectVersion);
//			return Pair.of(true, stat.getVersion());
//		} catch (KeeperException.BadVersionException e1) {
//			if (checker != null)
//				checker.apply(Triple.of(zkClient, path, data));
//			else {
//				// log warn:
//				// debug("Checker method is not passed skipping zkData match")
//				// warn(String.format("Conditional update of path %s with data %s and expected version %d failed due to %s",path,
//				// data, expectVersion, e1.getMessage))
//				return Pair.of(false, -1);
//			}
//		} catch (Exception e2) {
//			// warn(String.format("Conditional update of path %s with data %s and expected version %d failed due to %s",path,
//			// data,expectVersion, e2.getMessage))
//			return Pair.of(false, -1);
//		}
//		return null;
//
//	}

//	/**
//	 * Conditional update the persistent path data, return (true, newVersion) if
//	 * it succeeds, otherwise (the current version is not the expected version,
//	 * etc.) return (false, -1). If path doesn't exist, throws ZkNoNodeException
//	 * @throws NoNodeException 
//	 */
//	public static Pair<Boolean, Integer> conditionalUpdatePersistentPathIfExists(
//			ZKConnector<?> zkClient, String path, String data, int expectVersion) throws NoNodeException {
//		try {
//			Stat stat=zkClient.setData().withVersion(expectVersion).forPath(path, serializer.serialize(data));
//			//Stat stat = zkClient.writeDataReturnStat(path, data, expectVersion);
//			return Pair.of(true, stat.getVersion());
//		} catch (KeeperException.NoNodeException nne) {
//			throw nne;
//		} catch (Exception e) {
//			// error(String.format("Conditional update of path %s with data %s and expected version %d failed due to %s",path,
//			// data,expectVersion, e.getMessage))
//			return Pair.of(false, -1);
//		}
//	}

	

	public static Cluster getCluster(ZKConnector<?> zkClient) {
		try{
			Cluster cluster = new Cluster();
			List<String> nodes = zkClient.getChildrenParentMayNotExist(BrokerIdsPath);
			for (String node : nodes) {
				String brokerZKString = zkClient.readData( BrokerIdsPath + "/"	+ node);
				cluster.add(Broker.createBroker(Integer.parseInt(node),
						brokerZKString));
			}
			return cluster;
		}catch(Throwable t){
			throw new KafkaZKException(t);
		}
	}

	public static Map<String, Map<Integer, List<Integer>>> getPartitionAssignmentForTopics(
			final ZKConnector<?> zkClient, List<String> topics) {
		return Maps.toMap(topics,
				new Function<String, Map<Integer, List<Integer>>>() {
					@SuppressWarnings("unchecked")
					@Override
					public Map<Integer, List<Integer>> apply(String topic) {
						try {
							String jsonPartitionMapOpt = zkClient.readDataMaybeNull(topicPath(topic));
							checkNotNull(jsonPartitionMapOpt);
							Map<String, Object> jsonObj = mapper
									.reader(typeMap).readValue(
											jsonPartitionMapOpt);
							checkNotNull(jsonObj);
							Map<String, List<Integer>> partitions = (Map<String, List<Integer>>) jsonObj
									.get("partitions");
							checkNotNull(partitions);
							Map<Integer, List<Integer>> tmp = Maps.newHashMap();
							for (Entry<String, List<Integer>> entity : partitions
									.entrySet()) {
								tmp.put(Integer.parseInt(entity.getKey()),
										entity.getValue());
							}
							return tmp;
						} catch (Exception e) {
							return Maps.newHashMap();
						}
					}
				});
	}

	public static Map<String, List<Integer>> getPartitionsForTopics(
			final ZKConnector<?> zkClient, final List<String> topics) {
		return Maps.toMap(topics, new Function<String, List<Integer>>() {
			@Override
			public List<Integer> apply(String topic) {
				return Ordering.natural().sortedCopy(
						getPartitionAssignmentForTopics(zkClient, topics).get(
								topic).keySet());
			}
		});
	}

	@SuppressWarnings("rawtypes")
	public static String getPartitionReassignmentZkData(
			Map<TopicAndPartition, List<Integer>> partitionsToBeReassigned) {
		return Json
				.encode(ImmutableMap.of(
						"version",
						1,
						"partitions",
						Lists.transform(
								Lists.newArrayList(partitionsToBeReassigned
										.entrySet()),
								new Function<Entry<TopicAndPartition, List<Integer>>, Map>() {
									@Override
									public Map apply(
											Entry<TopicAndPartition, List<Integer>> input) {
										return ImmutableMap.of("topic", input
												.getKey().topic(), "partition",
												input.getKey().partition(),
												"replicas", input.getValue());
									}
								})));
	}

	public static void updatePartitionReassignmentData(ZKConnector<?> zkClient,
			Map<TopicAndPartition, List<Integer>> partitionsToBeReassigned) {
		try{
		if (partitionsToBeReassigned.size() == 0) {// need to delete the
													// /admin/reassign_partitions
													// path
			zkClient.deletePath(ReassignPartitionsPath);
		} else {
			String jsonData = getPartitionReassignmentZkData(partitionsToBeReassigned);
			zkClient.updatePersistentPath(ReassignPartitionsPath, jsonData);
		}
		}catch(Throwable t){
			throw new KafkaZKException(t);
		}
	}

	@SuppressWarnings("unchecked")
	public static Set<TopicAndPartition> getPartitionsUndergoingPreferredReplicaElection(
			ZKConnector<?> zkClient) {
		try{
		// read the partitions and their new replica list
			String jsonPartitionListOpt = zkClient.readDataMaybeNull(PreferredReplicaLeaderElectionPath);
			Map<String, Object> jsonObj = mapper.reader(typeMap).readValue(	jsonPartitionListOpt);
			checkState(jsonObj!=null,String.format("Preferred replica election data.[%s]", jsonPartitionListOpt));
			List<Map<String,Object>> partitions=(List<Map<String,Object>>)jsonObj.get("partitions");
			checkState(partitions!=null,"Preferred replica election data is empty");
			List<TopicAndPartition> plist=Lists.transform(partitions, new Function<Map<String,Object>,TopicAndPartition>(){
				@Override
				public TopicAndPartition apply(Map<String, Object> input) {
					return new TopicAndPartition((String)input.get("topic"),(Integer)input.get("partition"));
				}
			});
			Set<TopicAndPartition> sets=ImmutableSet.copyOf(plist);
			checkState(sets.size()==plist.size(),"Preferred replica election data contains duplicate partitions");
			return sets;
		}catch(Throwable t){
			throw new KafkaZKException(t);
		}
	}

	public static void deletePartition(ZKConnector<?> zkClient, int brokerId,
			String topic) {
		try{
		String brokerIdPath = BrokerIdsPath + "/" + brokerId;
		zkClient.deletePath(brokerIdPath);
		String brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/"
				+ brokerId;
		zkClient.deletePath(brokerPartTopicPath);//.delete(brokerPartTopicPath);
		}catch(Throwable t){
			throw new KafkaZKException(t);
		}
	}

	public static List<String> getConsumersInGroup(ZKConnector<?> zkClient,
			String group) {
		try{
		ZKGroupDirs dirs = new ZKGroupDirs(group);
		return zkClient.getChildren(dirs.consumerRegistryDir());
		}catch(Throwable t){
			throw new KafkaZKException(t);
		}
	}

	public static List<String> consumersOfTopic(ZKConnector<?> zkClient,
			String group, String topic) {
		try{
		ZKGroupDirs dirs = new ZKGroupDirs(group);
		List<String> consumers = zkClient.getChildrenParentMayNotExist(dirs.consumerRegistryDir());
		Map<String, List<String>> consumersPerTopicMap = Maps.newHashMap();
		for (String consumer : consumers) {
			TopicCount topicCount = TopicCount.constructTopicCount(zkClient,
					group, consumer);
			for (Entry<String, Set<TopicCount.ConsumerThreadId>> entity : topicCount
					.getConsumerThreadIdsPerTopic().entrySet()) {
				for (TopicCount.ConsumerThreadId consumerId : entity.getValue()) {
					List<String> c = consumersPerTopicMap.get(entity.getKey());
					if (c == null) {
						c = Lists.newArrayList();
						consumersPerTopicMap.put(entity.getKey(), c);
					}
					c.add(consumerId.getConsumer());
				}
			}
		}
		return consumersPerTopicMap.get(topic);
		}catch(Exception t){
			throw new KafkaZKException(t);
		}
	}

	/**
	 * This API takes in a broker id, queries zookeeper for the broker metadata
	 * and returns the metadata for that broker or throws an exception if the
	 * broker dies before the query to zookeeper finishes
	 * 
	 * @param brokerId
	 *            The broker id
	 * @param zkClient
	 *            The zookeeper client connection
	 * @return An optional Broker object encapsulating the broker metadata
	 * @throws Exception 
	 */
	public static Broker getBrokerInfo(ZKConnector<?> zkClient, int brokerId) {
		try{
		String brokerInfo = zkClient.readDataMaybeNull(BrokerIdsPath + "/"	+ brokerId);
		if (brokerInfo != null)
			return Broker.createBroker(brokerId, brokerInfo);
		}catch(Throwable t){
		}
		return null;
	}

	public static List<String> getAllTopics(ZKConnector<?> zkClient) {
		try{
			return zkClient.getChildrenParentMayNotExist(BrokerTopicsPath);
		}catch(Throwable t){
			throw new KafkaZKException(t);
		}
	}

	public static Set<TopicAndPartition> getAllPartitions(
			final ZKConnector<?> zkClient) {
		try{
		List<String> topics = zkClient.getChildrenParentMayNotExist(BrokerTopicsPath);
		if(topics==null) return Sets.newHashSet();
		return Sets.newHashSet(Iterables.concat(Lists.transform(topics,
				new Function<String, Iterable<TopicAndPartition>>() {
					@Override
					public Iterable<TopicAndPartition> apply(
							final String topic) {
						try{
						List<String> partitions = zkClient.getChildren(topicPartitionsPath(topic));
						return Lists.transform(partitions,
								new Function<String, TopicAndPartition>() {
									@Override
									public TopicAndPartition apply(
											String input) {
										return new TopicAndPartition(topic,
												Integer.parseInt(input));
									}
								});
						}catch(Throwable t){
							
						}
						return Lists.newArrayList();
					}
				})));
		}catch(Throwable t){
			throw new KafkaZKException(t);
		}
	}

	public static class Triple<T1, T2, T3> {
		public static <T1, T2, T3> Triple<T1, T2, T3> of(T1 lhs, T2 mhs, T3 rhs) {
			return new Triple<>(lhs, mhs, rhs);
		}

		public final T1 lhs;
		public final T2 mhs;
		public final T3 rhs;

		public Triple(T1 lhs, T2 mhs, T3 rhs) {
			this.lhs = lhs;
			this.rhs = rhs;
			this.mhs = mhs;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((lhs == null) ? 0 : lhs.hashCode());
			result = prime * result + ((mhs == null) ? 0 : mhs.hashCode());
			result = prime * result + ((rhs == null) ? 0 : rhs.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Triple<?,?,?> other = (Triple<?,?,?>) obj;
			if (lhs == null) {
				if (other.lhs != null)
					return false;
			} else if (!lhs.equals(other.lhs))
				return false;
			if (mhs == null) {
				if (other.mhs != null)
					return false;
			} else if (!mhs.equals(other.mhs))
				return false;
			if (rhs == null) {
				if (other.rhs != null)
					return false;
			} else if (!rhs.equals(other.rhs))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "Triple{" + "lhs=" + lhs + ",mhs=" + mhs + ", rhs=" + rhs
					+ '}';
		}

		public static <T1, T2, T3> Function<Triple<T1, T2, T3>, T1> lhsFn() {
			return new Function<Triple<T1, T2, T3>, T1>() {
				@Override
				public T1 apply(Triple<T1, T2, T3> input) {
					return input.lhs;
				}
			};
		}

		public static <T1, T2, T3> Function<Triple<T1, T2, T3>, T2> mhsFn() {
			return new Function<Triple<T1, T2, T3>, T2>() {
				@Override
				public T2 apply(Triple<T1, T2, T3> input) {
					return input.mhs;
				}
			};
		}

		public static <T1, T2, T3> Function<Triple<T1, T2, T3>, T3> rhsFn() {
			return new Function<Triple<T1, T2, T3>, T3>() {
				@Override
				public T3 apply(Triple<T1, T2, T3> input) {
					return input.rhs;
				}
			};
		}
	}

	public static class Pair<T1, T2> {

		public static <T1, T2> Pair<T1, T2> of(T1 lhs, T2 rhs) {
			return new Pair<>(lhs, rhs);
		}

		public final T1 lhs;
		public final T2 rhs;

		public Pair(T1 lhs, T2 rhs) {
			this.lhs = lhs;
			this.rhs = rhs;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			Pair<?,?> pair = (Pair<?,?>) o;

			if (lhs != null ? !lhs.equals(pair.lhs) : pair.lhs != null) {
				return false;
			}
			if (rhs != null ? !rhs.equals(pair.rhs) : pair.rhs != null) {
				return false;
			}

			return true;
		}

		@Override
		public int hashCode() {
			int result = lhs != null ? lhs.hashCode() : 0;
			result = 31 * result + (rhs != null ? rhs.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return "Pair{" + "lhs=" + lhs + ", rhs=" + rhs + '}';
		}

		public static <T1, T2> Function<Pair<T1, T2>, T1> lhsFn() {
			return new Function<Pair<T1, T2>, T1>() {
				@Override
				public T1 apply(Pair<T1, T2> input) {
					return input.lhs;
				}
			};
		}

		public static <T1, T2> Function<Pair<T1, T2>, T2> rhsFn() {
			return new Function<Pair<T1, T2>, T2>() {
				@Override
				public T2 apply(Pair<T1, T2> input) {
					return input.rhs;
				}
			};
		}

		public static <T1> Comparator<Pair<T1, ?>> lhsComparator(
				final Comparator<T1> comparator) {
			return new Comparator<Pair<T1, ?>>() {
				@Override
				public int compare(Pair<T1, ?> o1, Pair<T1, ?> o2) {
					return comparator.compare(o1.lhs, o2.lhs);
				}
			};
		}

	}

	public static class ZKGroupDirs {
		private String consumerDir = ConsumersPath;
		private String group;

		public ZKGroupDirs(String group) {
			this.group = group;
		}

		public String consumerGroupDir() {
			return consumerDir + "/" + group;
		}

		public String consumerRegistryDir() {
			return consumerGroupDir() + "/ids";
		}
	}

	public static class ZKGroupTopicDirs extends ZKGroupDirs {
		private String topic;

		public ZKGroupTopicDirs(String group, String topic) {
			super(group);
			this.topic = topic;
		}

		public String consumerOffsetDir() {
			return consumerGroupDir() + "/offsets/" + topic;
		}

		public String consumerOwnerDir() {
			return consumerGroupDir() + "/owners/" + topic;
		}

		public String partitionOffset(Integer partition) {
			return consumerOffsetDir() + "/" + partition;
		}
	}
//	public static class ZKStringSerializer {
//		public byte[] serialize(Object data) {
//			try {
//				return data.toString().getBytes("UTF-8");
//			} catch (UnsupportedEncodingException e) {
//				throw new KafkaZKException(e);
//			}
//		}
//		public String deserialize(byte[] bytes) {
//			if (bytes == null)
//				return null;
//			else
//				try {
//					return new String(bytes, "UTF-8");
//				} catch (UnsupportedEncodingException e) {
//					throw new KafkaZKException(e);
//				}
//		}
//	}
}
