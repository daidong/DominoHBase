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
import java.util.Arrays;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-1-15
 * Time: 下午11:31
 * To change this template use File | Settings | File Templates.
 */
public class HTriggerEvent{
    /** Do not Use TS here in EVENT, we use TS as Version Control
     * 
      private long currTS;
      private long lastTS;
    */
    private byte[] newValue;
    private byte[] oldValue;
    private byte[] rowKey;
    private HTriggerKey htk;
    private long version;
    private HRegion r;
    private boolean initEvent = false;
    private boolean isAcc = false;
    private long timestamp = 0L;
    
    public boolean isInitEvent(){
      return this.initEvent;
    }
    
    public boolean isAccEvent(){
      return this.isAcc;
    }
    
    public long getTimeStamp(){
      return this.timestamp;
    }
    
    public HTriggerEvent(HTriggerKey htk, byte[] vn, byte[] vo, long ver){
        this.htk = htk;
        this.newValue = vn;
        this.oldValue = vo;
        this.version = ver;
        this.isAcc = LocalTriggerManage.containsAccumulator(htk);
        this.timestamp = System.currentTimeMillis();
    }
    
    public HTriggerEvent(HTriggerKey htk, byte[] rowKey, byte[] vn, byte[] vo, long ver){
      this(htk, vn, vo, ver);
      this.rowKey = rowKey;
    }
    
    public HTriggerEvent(HTriggerKey htk, byte[] rowKey, byte[] vn, byte[] vo, long ver, HRegion region){
      this(htk, rowKey, vn, vo, ver);
      this.r = region;
    }
    
    public HTriggerEvent(HTriggerKey htk, byte[] rowKey, byte[] vn, byte[] vo, long ver, HRegion region, boolean init){
      this(htk, rowKey, vn, vo, ver, region);
      this.initEvent = init;
    }
    
    public HRegion getRegion(){
      return this.r;
    }
    
    public long getVersion(){
      return this.version;
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
    public byte[] getOldValue(){
      return this.oldValue;
    }
 
    @Override
    public int hashCode() {
      return (int)this.getVersion();
    }
    
    @Override
    public boolean equals(Object o) {
      if (o instanceof HTriggerEvent){
        HTriggerEvent that = (HTriggerEvent)o;
        return (this.getVersion() == that.getVersion() && Arrays.equals(this.getRowKey(), that.getRowKey()));
        //return htk.equals(that.getEventTriggerKey());
      }
      return false;
    }

	@Override
	public String toString() {
		return ("["+new String(this.rowKey) + ":" + this.version + "]"); 
	}
    
    
}
