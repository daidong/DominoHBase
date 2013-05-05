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
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
  
  private static ConcurrentHashMap<Integer, ArrayList<WriteUnit>> cachedElements = 
      new ConcurrentHashMap<Integer, ArrayList<WriteUnit>>();
  
  //One Region Server shares the name to table mappping.
  private static HashMap<byte[], HTable> nameToTableMap = new HashMap<byte[], HTable>();
  
  
  private static void recordZKActionRound(String node, int tid, long version){
    
  }
  private static void recordZKWritesFlushed(String node, int tid, long version){
    
  }
  /**
   * When trigger function users implelmented call append, it must provide a write instance,
   * and 'this'. We will get current round id and trigger id by 'this' and
   * record this information into ZooKeeper. So, in the furture, if the
   * 'flush()' operation does not success, we can recover it by knowing the round id 
   * that we have not flush.
   * @param action
   * @param write
   * @return
   */
  public static boolean append(HTriggerAction action, WriteUnit write){
    try{
      long round = action.getCurrentRound();
      int triggerId = action.getHTrigger().getTriggerId();
      recordZKActionRound(CurrentRS, triggerId, round);

      ArrayList<WriteUnit> created = new ArrayList<WriteUnit>();
      ArrayList<WriteUnit> existed = cachedElements.putIfAbsent(triggerId, created);
      existed.add(write);
    } catch (Exception e){
      return false;
    }
    return true;
  }
  
  /**
   * Flush current action's pending Puts.
   * @param action
   */
  public static void flush(HTriggerAction action){
    int triggerId = action.getHTrigger().getTriggerId();
    long round = action.getCurrentRound();
    ArrayList<WriteUnit> writes= cachedElements.get(triggerId);
    HashMap<HTable, ArrayList<Put>> aggWrites = new HashMap<HTable, ArrayList<Put>>();
    
    for (WriteUnit w:writes){
      if (nameToTableMap.containsKey(w.getTableName())){
        HTable instance = nameToTableMap.get(w.getTableName());
        aggWrites.get(instance).add(w.getPut());
      } else {
        //We have not create table instance and its put lists
        try {
          HTable created = new HTable(conf, w.getTableName());
          nameToTableMap.put(w.getTableName(), created);
          ArrayList<Put> writesPerTable = new ArrayList<Put>();
          writesPerTable.add(w.getPut());
          aggWrites.put(created, writesPerTable);
        }catch (IOException e) {
          // TODO 
          LOG.info("Can not create HTable instance due to bad configuration.");
        }
      }
    }
    for (HTable t : aggWrites.keySet()){
      try{
        t.put(aggWrites.get(t));
      } catch (IOException e){
        //TODO do some reties here until a obvious fail happens.          
      }
      //record successful flush for future recovery.
      //In fact, there should be a watcher monitoring on these dir and
      //delete entries written by recordZKActionRound.
      recordZKWritesFlushed(CurrentRS, triggerId, round);
    }
  }

}
