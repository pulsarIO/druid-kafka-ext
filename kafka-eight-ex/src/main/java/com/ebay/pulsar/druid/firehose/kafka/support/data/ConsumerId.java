package com.ebay.pulsar.druid.firehose.kafka.support.data;
/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import static com.google.common.base.Preconditions.checkArgument;
/**
 * @author qxing
 * 
 */
public final class ConsumerId {
	private String consumerGroupId;
	private String hostName;
	private String consumerUuid;
	public ConsumerId(String consumerGroupId) {
		this(consumerGroupId, null, null);
	}

	public ConsumerId(String consumerGroupId, String uniqueId) {
		this(consumerGroupId, null, uniqueId);
	}

	public ConsumerId(String consumerGroupId, String hostName, String uniqueId) {
		checkArgument(consumerGroupId!=null,"consumerGroupId can't be null.");
		this.consumerGroupId = consumerGroupId;
		UUID uuid = UUID.randomUUID();
		if (hostName == null) {
			try {
				InetAddress s = InetAddress.getLocalHost();
				this.hostName = s.getHostName();
			} catch (UnknownHostException e) {
				throw new RuntimeException("Can resolve local host name", e);
			}
		}
		 consumerUuid = String.format("%s-%d-%s",
			        this.hostName, System.currentTimeMillis(),
			        Long.toHexString(uuid.getMostSignificantBits()).substring(0,8));
	}

	public String getConsumerGroupId() {
		return consumerGroupId;
	}

	public void setConsumerGroupId(String consumerGroupId) {
		this.consumerGroupId = consumerGroupId;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getConsumerUuid() {
		return consumerUuid;
	}
	public void setConsumerUuid(String consumerUuid) {
		this.consumerUuid = consumerUuid;
	}

	@Override
	public String toString() {
		return consumerGroupId+"_"+consumerUuid;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((consumerGroupId == null) ? 0 : consumerGroupId.hashCode());
		result = prime * result
				+ ((consumerUuid == null) ? 0 : consumerUuid.hashCode());
		result = prime * result
				+ ((hostName == null) ? 0 : hostName.hashCode());
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
		ConsumerId other = (ConsumerId) obj;
		if (consumerGroupId == null) {
			if (other.consumerGroupId != null)
				return false;
		} else if (!consumerGroupId.equals(other.consumerGroupId))
			return false;
		if (consumerUuid == null) {
			if (other.consumerUuid != null)
				return false;
		} else if (!consumerUuid.equals(other.consumerUuid))
			return false;
		if (hostName == null) {
			if (other.hostName != null)
				return false;
		} else if (!hostName.equals(other.hostName))
			return false;
		return true;
	}

	
}
