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
package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.trigger.HTriggerEvent;
import org.apache.hadoop.hbase.trigger.HTriggerKey;
import org.apache.hadoop.hbase.trigger.LocalTriggerManage;
import org.apache.hadoop.hbase.trigger.HTriggerEventQueue;
import org.apache.hadoop.hbase.trigger.WritePrepared;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Revised 2013/05/04
 * Add multi-version supports in Domino. To reduce the modification on code repo, we use
 * the multi-version ability of HBase.
 * 
 * @author daidong Created with IntelliJ IDEA. User: daidong Date: 13-3-2 Time:
 *         6:46 To change this template use File | Settings | File Templates.
 */
public class WALDetection {
  private static final Log LOG = LogFactory.getLog(WALDetection.class);
  
  public static boolean checkDispatch(HRegionInfo info, byte[] tableName, WALEdit currWal) {
    //System.out.println("in checkDispatch, tableName: " + new String(tableName));
    List<KeyValue> syncPairs = currWal.getKeyValues();
    for (KeyValue kv : syncPairs) {
      byte[] rowKey = kv.getRow();
      byte[] columnFamily = kv.getFamily();
      byte[] column = kv.getQualifier();
      long curVersion = kv.getTimestamp();
      
      HTriggerKey triggerMeta = new HTriggerKey(tableName, columnFamily, column);
      //System.out.println("processing trigger key: " + triggerMeta.toString() + " at Row: " + new String(rowKey));
      //System.out.println("current registered trigger key: " + LocalTriggerManage.prettyPrint());
      
      if (LocalTriggerManage.containsTrigger(triggerMeta)) {
        byte[] oldValues = null;
        byte[] values = null;
        values = kv.getValue();
        oldValues = values;
        
        try {
          /**
           * 2013/05/04 REVISE 2
           * In fact, the execution of get old value inside the same Region is quite fast,  
           * its typical execution time is much less than 1 million seconds. So do not need to 
           * move it the the filter function. 
           *  
           * 2013/05/04 REVISE
           * We will run cluster several times to get the execution time of this part,
           * if It really costs lots of time, we should move it to the filter function. 
           * I means, Domino apps developers should be aware of whether they need the old
           * value or not in filter function, so they should choose whether this part of code
           * should be executed.
           */
          //long before = System.currentTimeMillis();
          //if contain this trigger, we construct the old value;
          HRegion r = info.theRegion;
          if (r != null){
            Get get = new Get(rowKey);
            get.addColumn(columnFamily, column);
            Result result = r.get(get, null);

            if (result.size() == 0){  //no element
              oldValues = "0".getBytes();
            } else {                  //get element
              KeyValue[] olds = result.raw();
              oldValues = olds[0].getValue();
            }
          }
          //long after = System.currentTimeMillis();
          //LOG.info("DOMINO=PERFORMANCE=CHECK: Construct the old value costs: " + (after - before));
          
          /*System.out.println("this update fires a trigger: values: " + new String(values, "utf-8") + " | "
              + "old values: " + new String(oldValues, "utf-8"));
          */
          
          HTriggerKey key = new HTriggerKey(tableName, columnFamily, column);
          HTriggerEvent firedEvent = new HTriggerEvent(key, rowKey, values, oldValues, curVersion, r);
          HTriggerEventQueue.append(firedEvent);
        } catch (UnsupportedEncodingException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (IOException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }
    }
    return true;
  }
}
