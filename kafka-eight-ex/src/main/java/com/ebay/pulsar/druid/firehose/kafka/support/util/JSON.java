/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is licensed under the Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.druid.firehose.kafka.support.util;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JSON {
	//private static JsonGenerator jsonGenerator = null;
    private static ObjectMapper objectMapper = null;
    static{
    	objectMapper=new ObjectMapper();
    	
    }
    public static String toJSONString(Object obj){
    	try {
    		
			return objectMapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
    }
    public static <T> T fromJSON(String json, Class<T> clazz){
    	try {
			return objectMapper.readValue(json, clazz);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }
    @SuppressWarnings("unchecked")
	public static <T> List<T> fromJSON(String json, Class<?> collectionClass, Class<T> clazz){
    	try {
    		JavaType jt=objectMapper.getTypeFactory().constructParametricType(collectionClass, clazz);
            return (List<T>)objectMapper.readValue(json, jt);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }
}
