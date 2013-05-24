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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;

public abstract class AccHTriggerAction extends HTriggerAction{

  private static final Log LOG = LogFactory.getLog(AccHTriggerAction.class);
  
  AccumulatorReader reader = null;
  
  public AccumulatorReader getReader(){
    return this.reader;
  }
  
  @Override
  public abstract void action(HTriggerEvent hte);

  @Override
  public abstract boolean filter(HTriggerEvent hte);

  /**
   * TODO: HRgion.get() is expensive due to its complexity 
   * We need a simple way to get the newest value of the same column family
   * AND
   * We need a simple way to get the stored intermediate value.
   */
  @Override
  public void actionWrapper(HTriggerEvent hte){
    HTriggerKey triggeredElement = hte.getEventTriggerKey();
    byte[] tableName = triggeredElement.getTableName();
    byte[] columnFamily = triggeredElement.getColumnFamily();
    byte[] rowKey = hte.getRowKey();
    long version = hte.getVersion();
    HRegion r = hte.getRegion();
    this.setRound((version + 1) % MAX_ROUND);
    boolean flag = true;

    System.out.println("AccHTriggerAction Begins at " + new String(rowKey));
    Method incrMethod = this.getIncr();
    try {
      if (incrMethod != null){
        System.out.println("ActionWrapper...Execute incr");
        PartialResult pr = new PartialResult(tableName, rowKey, columnFamily, r);
        if (!pr.getPartial().isEmpty()){
          flag = false;
          incrMethod.invoke(hte, pr);
        }
      }
    } catch (Exception e){
      e.printStackTrace();
    }
    
    if (flag){
      try {
        System.out.println("ActionWrapper...Execute action");
        this.reader = new AccumulatorReader(tableName, columnFamily, rowKey, this.getRound(), r);
      } catch (IOException e) {
        e.printStackTrace();
      }
      this.action(hte);
    }
    //Do some after work
  }
  
  
}
