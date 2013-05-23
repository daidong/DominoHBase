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
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

/**
 * 
 * @author daidong
 * This class starts a thread to run the initial run of a initial trigger.
 * 
 * The way: go through all the local data, apply action function.
 */
public class InitialLocalRunner implements Runnable{
  
  private static final Log LOG = LogFactory.getLog(InitialLocalRunner.class);
  
  private HTrigger ht;
  private HRegionServer local;
  
  public InitialLocalRunner(HTrigger ht, HRegionServer local){
    this.ht = ht;
    this.local = local;
  }

  /**
   * HTrigger contains the initilized action class. Run the action on the local dataset one by one
   * Here are two choices: 
   * 1) make the initial run different from ordinary. Run it directly in this thread. 
   * 2) treat the initial run just as the ordinary. Create events to event queue.
   * Here, we choose the second strategy for simplity.
   */
  @Override
  public void run() {
    long version = 0L;
    HTriggerKey htk = ht.getHTriggerKey();
    int id = ht.getTriggerId();
    
    byte[] tname = htk.getTableName();
    byte[] cf = htk.getColumnFamily();
    byte[] c = htk.getColumn();
    
    byte[] rowKey = null;
    byte[] values = null;
    byte[] oldValues = null;
    
    List<HRegion> allRegions = local.getOnlineRegions(tname);
    LOG.info(new String(tname) + " has " + allRegions.size() + " regions");
    
    for (HRegion r : allRegions){
      Scan scan = new Scan();

      if (!"*".equalsIgnoreCase(new String(c))){
        scan.addColumn(cf, c);
      } else {
        scan.addFamily(cf);
      }
      
      RegionScanner rs = null;
      try {
        rs = r.getScanner(scan);
        List<KeyValue> results = new ArrayList<KeyValue>();
        /**
         * Each time new value is appended into results, cause redutant 
         * addition. 
         */
        while (rs.next(results)){}
        
        for (KeyValue kv : results){
          rowKey = kv.getRow();
          values = oldValues = kv.getValue();
          version = kv.getTimestamp();
          
          HTriggerEvent firedEvent = new HTriggerEvent(htk, rowKey, values, oldValues, version, r, true);
          HTriggerEventQueue.append(firedEvent);
        }
        
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }
  }
  
}
