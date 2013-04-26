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

import java.util.Comparator;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-1-15
 * Time: 下午11:31
 * To change this template use File | Settings | File Templates.
 */
public class HTriggerEvent{
    private long currTS;
    private long lastTS;
    private byte[] newValue;
    private byte[] oldValue;
    private byte[] rowKey;
    private HTriggerKey htk;
    public long buildTs;

    public HTriggerEvent(HTriggerKey htk, long tsn, byte[] vn, long tso, byte[] vo){
        this.htk = htk;
        this.currTS = tsn;
        this.newValue = vn;
        this.oldValue = vo;
        this.lastTS = tso;
        this.buildTs = System.currentTimeMillis(); 
    }
    
    public HTriggerEvent(HTriggerKey htk, byte[] rowKey, long tsn, byte[] vn, long tso, byte[] vo){
      this(htk, tsn, vn, tso, vo);
      this.rowKey = rowKey;
    }
    
    public byte[] getRowKey(){
      return this.rowKey;
    }
    public HTriggerKey getEventTriggerKey(){
        return this.htk;
    }
    
    public byte[] getNewValue(){
      return this.newValue;
    }
    public long getNewTS(){
      return this.currTS;
    }
    public byte[] getOldValue(){
      return this.oldValue;
    }
    public long getOldTS(){
      return this.lastTS;
    }
 
    @Override
    public int hashCode() {
      return htk.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
      if (o instanceof HTriggerEvent){
        HTriggerEvent that = (HTriggerEvent)o;
        return htk.equals(that.getEventTriggerKey());
      }
      return false;
    }
}
