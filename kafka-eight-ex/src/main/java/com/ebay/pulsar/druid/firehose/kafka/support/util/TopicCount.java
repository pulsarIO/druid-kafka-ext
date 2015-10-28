/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.util;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import kafka.consumer.Blacklist;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;

import com.ebay.pulsar.druid.firehose.kafka.support.api.ZKConnector;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaZKData;
import com.ebay.pulsar.druid.firehose.kafka.support.exceptions.KafkaZKException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Translate scala code from kafka TopicCount to Java code
 * @author qxing
 * 
 **/
public abstract class TopicCount {

	public abstract Map<String, Set<ConsumerThreadId>> getConsumerThreadIdsPerTopic();

	public abstract Map<String, Integer> getTopicCountMap();

	public abstract String pattern();

	public static class ConsumerThreadId implements Comparable<ConsumerThreadId> {
		String consumer;
		int threadId;

		public ConsumerThreadId(String consumer, int threadId) {
			this.consumer = consumer;
			this.threadId = threadId;
		}
		
		public String getConsumer() {
			return consumer;
		}

		public void setConsumer(String consumer) {
			this.consumer = consumer;
		}

		public int getThreadId() {
			return threadId;
		}

		public void setThreadId(int threadId) {
			this.threadId = threadId;
		}



		@Override
		public String toString() {
			return String.format("%s-%d", consumer, threadId);
		}

		@Override
		public int compareTo(ConsumerThreadId other) {
			if (other == null)
				return 1;
			return toString().compareTo(other.toString());
		}
	}

	protected static final String whiteListPattern = "white_list";
	protected static final String blackListPattern = "black_list";
	protected static final String staticPattern = "static";

	public String makeThreadId(String consumerIdString, int threadId) {
		return consumerIdString + "-" + threadId;
	}

	public Map<String, Set<ConsumerThreadId>> makeConsumerThreadIdsPerTopic(
			String consumerIdString, Map<String, Integer> topicCountMap) {
		Map<String, Set<ConsumerThreadId>> consumerThreadIdsPerTopicMap = Maps
				.newHashMap();
		for (Entry<String, Integer> entity : topicCountMap.entrySet()) {
			Set<ConsumerThreadId> consumerSet = Sets.newHashSet();
			assert (entity.getValue() >= 1);
			for (int i = 0; i < entity.getValue(); i++) {
				consumerSet.add(new ConsumerThreadId(consumerIdString, i));
			}
			consumerThreadIdsPerTopicMap.put(entity.getKey(), consumerSet);
		}
		return consumerThreadIdsPerTopicMap;
	}

	@SuppressWarnings("unchecked")
	public static TopicCount constructTopicCount(ZKConnector<?> zkClient, String group,
			String consumerId) {
		KafkaZKData.ZKGroupDirs dirs = new KafkaZKData.ZKGroupDirs(group);
		String subscriptionPattern = null;
		Map<String, Integer> topMap = null;
		try {
			String topicCountString = zkClient.readData(dirs.consumerRegistryDir() + "/" + consumerId);
			ObjectMapper mapper = new ObjectMapper();
			TypeReference<Map<String, Object>> typeMap = new TypeReference<Map<String, Object>>() {
			};
			Map<String, Object> jsonObj = mapper.reader(typeMap).readValue(
					topicCountString);
			if (jsonObj == null)
				throw new KafkaZKException("error constructing TopicCount : "
						+ topicCountString);
			Object pattern = jsonObj.get("pattern");
			if (pattern == null)
				throw new KafkaZKException("error constructing TopicCount : "
						+ topicCountString);
			subscriptionPattern = (String) pattern;
			Object sub = jsonObj.get("subscription");
			if (sub == null)
				throw new KafkaZKException("error constructing TopicCount : "
						+ topicCountString);
			topMap = (Map<String, Integer>) sub;

		} catch (Throwable t) {
			throw new KafkaZKException(t);
		}

		boolean hasWhiteList = whiteListPattern.equals(subscriptionPattern);
		boolean hasBlackList = blackListPattern.equals(subscriptionPattern);

		if (topMap.isEmpty() || !(hasWhiteList || hasBlackList)) {
			return new StaticTopicCount(consumerId, topMap);
		} else {
			String regex = null;
			Integer numStreams = -1;
			for (Entry<String, Integer> entity : topMap.entrySet()) {
				regex = entity.getKey();
				numStreams = entity.getValue();
				break;
			}
			TopicFilter filter = hasWhiteList ? new Whitelist(regex)
					: new Blacklist(regex);

			return new WildcardTopicCount(zkClient, consumerId, filter,
					numStreams);
		}

	}

	public static TopicCount constructTopicCount(String consumerIdString,
			Map<String, Integer> topicCount) {
		return new StaticTopicCount(consumerIdString, topicCount);
	}

	public static TopicCount constructTopicCount(ZKConnector<?> zkClient,
			String consumerIdString, TopicFilter filter, int numStreams) {
		return new WildcardTopicCount(zkClient, consumerIdString, filter,
				numStreams);
	}

	public static class StaticTopicCount extends TopicCount {
		private String consumerIdString;
		private Map<String, Integer> topicCountMap;

		public StaticTopicCount(String consumerIdString,
				Map<String, Integer> topicCountMap) {
			this.consumerIdString = consumerIdString;
			this.topicCountMap = topicCountMap;
		}

		public Map<String, Set<ConsumerThreadId>> getConsumerThreadIdsPerTopic() {
			return makeConsumerThreadIdsPerTopic(this.consumerIdString,
					this.topicCountMap);
		}

		public Map<String, Integer> getTopicCountMap() {
			return topicCountMap;
		}

		public String pattern() {
			return staticPattern;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime
					* result
					+ ((consumerIdString == null) ? 0 : consumerIdString
							.hashCode());
			result = prime * result
					+ ((topicCountMap == null) ? 0 : topicCountMap.hashCode());
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
			StaticTopicCount other = (StaticTopicCount) obj;
			if (consumerIdString == null) {
				if (other.consumerIdString != null)
					return false;
			} else if (!consumerIdString.equals(other.consumerIdString))
				return false;
			if (topicCountMap == null) {
				if (other.topicCountMap != null)
					return false;
			} else if (!topicCountMap.equals(other.topicCountMap))
				return false;
			return true;
		}

	
		// override def equals(obj: Any): Boolean = {
		// obj match {
		// case null => false
		// case n: StaticTopicCount => consumerIdString == n.consumerIdString &&
		// topicCountMap == n.topicCountMap
		// case _ => false
		// }
		// }
	}

	public static class WildcardTopicCount extends TopicCount {
		TopicFilter topicFilter;
		Integer numStreams;
		String consumerIdString;
		ZKConnector<?> zkClient;

		public WildcardTopicCount(ZKConnector<?> zkClient, String consumerIdString,
				TopicFilter topicFilter, Integer numStreams) {
			this.consumerIdString = consumerIdString;
			this.numStreams = numStreams;
			this.topicFilter = topicFilter;
			this.zkClient = zkClient;
		}

		public Map<String, Set<ConsumerThreadId>> getConsumerThreadIdsPerTopic() {
			try{
			Map<String, Integer> wildcardTopics = Maps.toMap(Iterables.filter(
					zkClient.getChildrenParentMayNotExist(KafkaZKData.BrokerTopicsPath),
					new Predicate<String>() {
						@Override
						public boolean apply(String input) {
							topicFilter.isTopicAllowed(input, true);
							return false;
						}
					}), new Function<String, Integer>() {
				@Override
				public Integer apply(String input) {
					return numStreams;
				}

			});
			return makeConsumerThreadIdsPerTopic(consumerIdString,
					wildcardTopics);
			}catch(Exception t){
				throw new KafkaZKException(t);
			}
		}

		public Map<String, Integer> getTopicCountMap() {
			return ImmutableMap.of(topicFilter.regex(), numStreams);
		}

		public String pattern() {
			if (topicFilter instanceof Whitelist)
				return whiteListPattern;
			else if (topicFilter instanceof Blacklist)
				return blackListPattern;
			else
				throw new KafkaZKException("Invalid topicFilter.");
		}
	}

}
