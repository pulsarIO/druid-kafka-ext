/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.util;

import java.util.Properties;
/**
 * 
 * @author qxing
 *
 */
public class PropertiesUtils {
	public static int getInt(Properties props, String key, int defaultValue){
		String v = props.getProperty(key);
		return v==null?defaultValue:Integer.parseInt(v);
	}
	public static Integer getInt(Properties props, String key){
		String v = props.getProperty(key);
		return v==null?null:Integer.parseInt(v);
	}
	public static long getLong(Properties props, String key, long defaultValue){
		String v = props.getProperty(key);
		return v==null?defaultValue:Long.parseLong(v);
	}
	public static String getString(Properties props, String key, String defaultValue){
		return props.getProperty(key,defaultValue);
	}
	public static String getString(Properties props, String key){
		return props.getProperty(key);
	}
	public static boolean getBoolean(Properties props, String key, boolean defaultValue){
		String v = props.getProperty(key);
		return v==null?defaultValue:Boolean.parseBoolean(v);
	}
}
