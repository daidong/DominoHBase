/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
  
package org.apache.hadoop.hbase.trigger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

/**
 * The WritePrepared encapulates all the writes into HBase tables, and we highly recommend
 * developers using this API instead of plain HBase client apis because WritePrepared delay
 * the writes until you explicitely call 'flush()' method to save the network bandwidth, 
 * and guarantee the writes will
 * finally carry on no matter what failures happen during writing. It is critical for 
 * fault tolerance and recovery in Domino.
 *  
 * @author daidong
 *
 */
public class WritePrepared{

  private static final Log LOG = LogFactory.getLog(WritePrepared.class);
  
  private static String CurrentRS = "current-rs-need-to-get";
  private static Configuration conf = HBaseConfiguration.create();
  
  public static ConcurrentHashMap<Integer, LinkedBlockingQueue<WriteUnit>> cachedElements = 
      new ConcurrentHashMap<Integer, LinkedBlockingQueue<WriteUnit>>();
  
  public static ConcurrentHashMap<Integer, AtomicBoolean> flags = new ConcurrentHashMap<Integer, AtomicBoolean>();
  
  private static byte[] lock = new byte[0];
  
  //One Region Server shares the name to table mappping.
  private static ConcurrentHashMap<byte[], HTable> nameToTableMap = new ConcurrentHashMap<byte[], HTable>();
  
  
  public static void recordZKActionRound(String node, int tid, long version){
    
  }
  public static void recordZKWritesFlushed(String node, int tid, long version){
    
  }
  public static void addElement(int k, WriteUnit v) throws InterruptedException{
	  LinkedBlockingQueue<WriteUnit> curr = cachedElements.get(k);
    if (curr == null){
      curr = new LinkedBlockingQueue<WriteUnit>();
      cachedElements.put(k, curr);
    }
    curr.put(v);
    if (!flags.containsKey(k)){
    	AtomicBoolean n = new AtomicBoolean(false);
    	flags.put(k, n);
    }
    	
  }

  
  public static void logElements(){
    for (int k : cachedElements.keySet()){
      LOG.info("=======> Trigger-"+k+ " contains these write units: ");
      for (WriteUnit w : cachedElements.get(k)){
        LOG.info(w);
      }
    }
  }
  /**
   * When trigger function users implemented call append, it must provide a write instance,
   * and 'this'. We will get current round id and trigger id by 'this' and
   * record this information into ZooKeeper. So, in the future, if the
   * 'flush()' operation does not success, we can recover it by knowing the round id 
   * that we have not flushed.
   * @param action
   * @param write
   * @return
   */
  public static boolean append(HTriggerAction action, WriteUnit write){
    try{
      long round = action.getCurrentRound();
      int triggerId = action.getHTrigger().getTriggerId();
      recordZKActionRound(CurrentRS, triggerId, round);
      
      addElement(triggerId, write);
    } catch (Exception e){
      e.printStackTrace();
      return false;
    }
    return true;
  }
  
  private static HTable getOrNewHTableInstance(byte[] name) throws IOException{
    HTable ins = nameToTableMap.get(name);
    if (ins == null){
      ins = new HTable(conf, name);
      nameToTableMap.put(name, ins);
    }
    return ins;
  }
  
  /**
   * Flush current action's pending Puts.
   * TODO I am pretty sure there is an error while processing the flush operations
   * It seems that the for statement does not work at all, and different flush may cause
   * interacts. I need more information about Java Multi-Thread programming!
   * @param action
   */
  public static void flush(HTriggerAction action){
    System.out.println("enter Flush");
    int triggerId = action.getHTrigger().getTriggerId();
    long round = action.getCurrentRound();
    LinkedBlockingQueue<WriteUnit> writes= cachedElements.get(triggerId);
    //AtomicBoolean flag = flags.get(triggerId);
    
    //if other thread is flushing, we should just leave.
    /*
     * It is not necessary to stop other flushes as there is no possible other flush existing.
    if (!flag.compareAndSet(false, true))
    	return;
    */

    try{
    	WriteUnit w = writes.poll();
    	HTable ins = null;
    	while (w != null){
    		System.out.println("start flush: " + w + "remain: " + writes.size());
    		ins = getOrNewHTableInstance(w.getTableName());
    		ins.put(w.getPut());
    		w = writes.poll();
    		System.out.println("end flush: " + w + "remain: " + writes.size());
    	}
    	ins.flushCommits();
    } catch (Exception e){
    	LOG.info("Exceptions While Calling HTable's Put");
    }
    //record successful flush for future recovery.
    //In fact, there should be a watcher monitoring on these dir and
    //delete entries written by recordZKActionRound.
    recordZKWritesFlushed(CurrentRS, triggerId, round);
    //LOG.info("Trigger" + triggerId + " at round" + round + " Flush OK");
  }

  
  
  
  /*
  try{
  	System.out.println("before while");
  	while (writes.size() != 0){
  		System.out.println("inside while");
  		for (int i = 0; i < writes.size(); i++){
  			WriteUnit w = writes.get(i);
  			HTable ins = getOrNewHTableInstance(w.getTableName());
  			ins.put(w.getPut());

          	if (w.isWriteToIncr()){
            		ins.put(w.getAccompPut());
          	}
  			writes.remove(i);
  			System.out.println("flush " + i++);
  			//ins.flushCommits();
  		}
  	}
  	System.out.println("we have flushed all elements");
      removeElement(triggerId);
  } catch (IOException e){
    LOG.info("Exceptions While Calling HTable's Put");
  }
  */
}
