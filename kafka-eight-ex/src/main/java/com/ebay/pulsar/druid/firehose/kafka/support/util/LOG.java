/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.util;

import com.metamx.common.logger.Logger;

/**
 *@author qxing
 * 
 **/
public class LOG {
	private static final Logger log = new Logger(LOG.class);
	public static void log(String msg){
		log.info("EX[%d][%s]:%s", Thread.currentThread().getId(),Thread.currentThread().getName(),msg);
		//String s="EX["+Thread.currentThread().getId()+"]["+Thread.currentThread().getName()+"]:"+msg;
		//System.out.println(s);
		//log.info(s);
		
	}
	public static void log(String msg, Throwable t){
		//log.info("EX[%d][%s]:%s", Thread.currentThread().getId(),Thread.currentThread().getName(),msg);
		log.error(t, "EX[%d][%s]:%s", Thread.currentThread().getId(),Thread.currentThread().getName(),msg);
		//String s="EX["+Thread.currentThread().getId()+"]["+Thread.currentThread().getName()+"]:"+msg;
		//System.out.println(s);
		//t.printStackTrace();
		//log.info(s,t);
	}
}
