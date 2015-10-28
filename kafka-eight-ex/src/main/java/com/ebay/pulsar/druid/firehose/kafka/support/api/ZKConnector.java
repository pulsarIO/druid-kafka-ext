/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.api;

import java.util.List;

import org.apache.zookeeper.data.Stat;

import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaZKData.Pair;


/**
 * All the operations with Zookeeper should delegate to ZKConnector.
 * 
 * the underlying Zookeeper implementation can be various.
 * 
 *@author qxing
 * 
 **/
public interface ZKConnector<T> {
	/**
	 * Init the underlying zookeeper client, make sure it is connected.
	 * init the zookeeper state change listener and processor,
	 * especially for CONNECTED, DISCONNECTED, RECONNECTED, SESSION_EXPIRED states or states equivalent underlying framework.
	 * 
	 * @throws InterruptedException
	 */
	public void start() throws InterruptedException ;
	/**
	 * close zookeeper channel. 
	 * Cannot communicate with zookeeper any more unless <code>init()</code> invoked again and <code>isConnected()</code> return true;
	 * once close() invoked, isZkConnected() will return false;
	 */
	public void close() ;
	/**
	 * return current ZKConnector state, connected or disconnected.
	 * @return true is connected and false for disconnected.
	 */
	public boolean isZkConnected() ;
	/**
	 * return underlying zookeeper framework client.
	 * @return
	 */
	public T getZkClient();
	
	/**
	 * 
	 * make sure a persistent path exists in ZK. Create the path if not exist.
	 * Never throw NoNodeException or NodeExistsException.
	 * 
	 * @throws Exception 
	 */
	public void makeSurePersistentPathExists(String path) throws Exception ;

	/**
	 * create the parent path
	 * @throws Exception 
	 */
	public void createParentPath(String path) throws Exception ;
	/**
	 * Create an ephemeral node with the given path and data. Create parents if
	 * necessary.
	 * @throws Exception 
	 */
	public String createEphemeralPath(String path,
			String data) throws Exception ;
	/**
	 * Create an persistent node with the given path and data. Create parents if
	 * necessary.
	 * @throws Exception 
	 */
	public void createPersistentPath(String path,
			String data) throws Exception ;
	
	public String createSequentialPersistentPath(String path, String data) throws Exception ;
	/**
	 * Update the value of a persistent node with the given path and data.
	 * create parrent directory if necessary. 
	 * Never throw NodeExistException.
	 * @throws Exception 
	 */
	public void updateEphemeralPath(String path,
			String data) throws Exception ;
	/**
	 * Update the value of a persistent node with the given path and data.
	 * create parrent directory if necessary. Never throw NodeExistException.
	 * Return the updated path zkVersion
	 * @throws Exception 
	 */
	public void updatePersistentPath(String path,
			String data) throws Exception;
	/**
	 * delete path.
	 * Never throw NoNodeException.
	 * 
	 * @param path
	 * @return true is success and false is failed.
	 */
	public boolean deletePath(String path) throws Exception;
	/**
	 * delete path recursively. 
	 * Never throw NoNodeException.
	 * 
	 * @param path
	 */
	public void deletePathRecursive(String path) throws Exception;
	/**
	 * read data from path.
	 * Exception thrown based on underlying zookeeper client. 
	 * @param path
	 * @return
	 */
	public String readData(String path) throws Exception;
	/**
	 * read data from path, Pair.lhs is the String type data and Pair.rhs is the stat. 
	 * Never throw NoNodeException
	 * 
	 * @param path
	 * @return Pair.lhs may be null and the stat will be no use, otherwise return the data and stat pair.
	 */
	public Pair<String,Stat> readDataMaybeNullWithStat(String path) throws Exception;
	/**
	 * read data from path. 
	 * Never throw NoNodeException
	 * 
	 * @param path
	 * @return may be null if no data or no node exist.
	 */
	public String readDataMaybeNull(String path) throws Exception;
	/**
	 * get children for path. 
	 * Exception thrown based on underlying zookeeper client. 
	 * 
	 * @param path
	 * @return
	 */
	public List<String> getChildren(String path) throws Exception;
	/**
	 * get children for path, if parent deos not exist, return null, never throw NoNodeException.
	 *  
	 * @param path
	 * @return
	 */
	public List<String> getChildrenParentMayNotExist(String path) throws Exception;

	/**
	 * Check if the given path exists
	 * @throws Exception 
	 */
	public boolean pathExists(String path) throws Exception;
	
}
