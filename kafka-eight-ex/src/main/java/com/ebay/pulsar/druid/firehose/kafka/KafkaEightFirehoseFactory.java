/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.ebay.pulsar.druid.firehose.kafka;


import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import kafka.message.MessageAndMetadata;

import com.ebay.pulsar.druid.firehose.kafka.support.SimpleConsumerController;
import com.ebay.pulsar.druid.firehose.kafka.support.SimpleConsumerEx;
import com.ebay.pulsar.druid.firehose.kafka.support.data.KafkaConsumerConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;

/**
 * modify druid extension KafkaEightFirehoseFactory to using kafka low level api code.
 * 
 */
public class KafkaEightFirehoseFactory implements FirehoseFactory<ByteBufferInputRowParser>
{
  private static final Logger log = new Logger(KafkaEightFirehoseFactory.class);

  @JsonProperty
  private final Properties consumerProps;

  @JsonProperty
  private final String feed;

  private AtomicLong readCount=new AtomicLong(0L);
  private AtomicLong commitCount=new AtomicLong(0L);
  @JsonCreator
  public KafkaEightFirehoseFactory(
      @JsonProperty("consumerProps") Properties consumerProps,
      @JsonProperty("feed") String feed
  )
  {
    this.consumerProps = consumerProps;
    this.feed = feed;
  }

  @Override
  public Firehose connect(final ByteBufferInputRowParser firehoseParser) throws IOException
  {
	  Set<String> newDimExclus = Sets.union(
		        firehoseParser.getParseSpec().getDimensionsSpec().getDimensionExclusions(),
		        Sets.newHashSet("feed")
		    );
		    final ByteBufferInputRowParser theParser = firehoseParser.withParseSpec(
		        firehoseParser.getParseSpec()
		                      .withDimensionsSpec(
		                          firehoseParser.getParseSpec()
		                                        .getDimensionsSpec()
		                                        .withDimensionExclusions(
		                                            newDimExclus
		                                        )
		                      )
		    );


    final SimpleConsumerController kafkaController = new SimpleConsumerController(new KafkaConsumerConfig(consumerProps));
    final SimpleConsumerEx consumer=kafkaController.createConsumer(feed);
    final Iterator<MessageAndMetadata<byte[], byte[]>>  iter=consumer.iterator();

//    final ConsumerConnector connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));
//
//    final Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(
//        ImmutableMap.of(
//            feed,
//            1
//        )
//    );
//
//    final List<KafkaStream<byte[], byte[]>> streamList = streams.get(feed);
//    if (streamList == null || streamList.size() != 1) {
//      return null;
//    }
//
//    final KafkaStream<byte[], byte[]> stream = streamList.get(0);
//    final ConsumerIterator<byte[], byte[]> iter = stream.iterator();

    return new Firehose()
    {
      @SuppressWarnings("static-access")
	@Override
      public boolean hasMore()
      {
    	  //Block when no more event available.
    	  while(!iter.hasNext() && !Thread.currentThread().interrupted()){
    		 
    	  }
    	  if(Thread.currentThread().interrupted()) return false;// when interrupted return false.
    	  return true;
      }

      @Override
      public InputRow nextRow()
      {
    	  readCount.incrementAndGet();
    	  do{
	    	  final MessageAndMetadata<byte[], byte[]> msgAndMeta=iter.next();
	    	  if(msgAndMeta!=null){
	    		  final byte[] message = msgAndMeta.message();
	    	        if (message != null) {
	    	        	commitCount.incrementAndGet();
	    	        	return theParser.parse(ByteBuffer.wrap(message));
	    	        }
	    	  }
    	  }while(!Thread.currentThread().isInterrupted());//Block when  null occurred until thread interrupted.
    	  return null;//shouldn't be returned except thread interrupted.
      }

      @Override
      public Runnable commit()
      {
    	  final long rcnt=readCount.getAndSet(0L);
    	  final long ccnt=commitCount.getAndSet(0L);
    	  consumer.markCommitOffset();//mark the offset which should be commit.
        return new Runnable()
        {
          
          @Override
          public void run()
          {
               /*
                 This is actually not going to do exactly what we want, cause it will be called asynchronously
                 after the persist is complete.  So, it's going to commit that it's processed more than was actually
                 persisted.  This is unfortunate, but good enough for now.  Should revisit along with an upgrade
                 of our Kafka version.
               */
            log.info("committing offsets["+feed+"]: rcnt="+rcnt+",ccnt="+ccnt);
            try {
            	/**
            	 * there are 3 offset for each of partition reader in a consumer: readOffset, markedCommitOffset, commitedOffset.
            	 * readOffset --->increased when read each record from kafka. In fact we will set it to the last record's offset
            	 * markedCommitOffset--> increased when Firehose.nextRow() return not null, in fact we set it to the last event's offset.
            	 * commitedOffset --> when we invoke consumer.markCommitOffset(), we set commitedOffset=markedCommitOffset.
            	 *                              when invoke consumer.commitOffset(), write commitedOffset to zookeeper.
            	 *                              
            	 * Why we do it like this, that's because there is a time gap between druid invoke Firehose.commit() and call run().
            	 * 
            	 */
				consumer.commitOffset();// commit offset which is marked by markCommitOffset() last time.
			} catch (Exception e) {
				log.error("commit offsets exception", e);
			}
          }
        };
      }

      @Override
      public void close() throws IOException
      {
    	  log.info("Close Firehose.[%s]",feed);
    	  /**
    	   * this will stop all the consumers created by this controller.
    	   * when stop consumer, we will close all the partitions, submit the last marked commit offset.
    	   * but this still will cause duplicate event.
    	   */
    	  kafkaController.stop();
      }
    };
  }
}
