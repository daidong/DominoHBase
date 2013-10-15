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

package org.apache.hadoop.hbase.percolator;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Get;


public class Transaction {

  private ArrayList<PWrite> writes = new ArrayList<PWrite>();
  private long startTs;
  
  public void Set(PWrite w){
    writes.add(w);
  }
  
  public Transaction(){
    startTs = System.currentTimeMillis();
  }
  
  public byte[] Get(String tableName, byte[] rowkey, byte[] cf, byte[] c) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    HTable t = new HTable(conf, tableName.getBytes());  
    while (true){
      Get g = new Get(rowkey);
      g.setTimeRange(0, startTs+1).addColumn(cf, (new String(c) + "lock").getBytes());
      Result result = t.get(g);
      if (!result.isEmpty()){
        BackoffAndCleanupLocks(rowkey, cf, c);
        continue;
      }
      
      g = new Get(rowkey);
      g.setTimeRange(0, startTs+1).setMaxVersions(1).addColumn(cf, (new String(c) + "write").getBytes());
      result = t.get(g);
      if (result.isEmpty())
        return null;
      KeyValue kv = result.getColumnLatest(cf, (new String(c) + "write").getBytes());
      String value = new String(kv.getValue());
      String dataColumn = value.split("@")[0];
      long dataTs = Long.parseLong(value.split("@")[1]);
      
      g = new Get(rowkey);
      g.setTimeRange(dataTs, dataTs+1).setMaxVersions(1).addColumn(cf, (dataColumn + "write").getBytes());
      result = t.get(g);
      kv = result.getColumnLatest(cf, (dataColumn + "write").getBytes());
      return kv.getValue();
    }
  }

  public boolean Prewrite(PWrite w, PWrite primary) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    HTable t = new HTable(conf, w.getTableName().getBytes());
    
    Get g = new Get(w.row);
    g.setTimeRange(startTs, Long.MAX_VALUE).addColumn(w.cfamily, (new String(w.column) + "write").getBytes());
    Result result = t.get(g);
    if (!result.isEmpty()) return false;
    
    g = new Get(w.row);
    g.setTimeRange(0, Long.MAX_VALUE).addColumn(w.cfamily, (new String(w.column) + "lock").getBytes());
    result = t.get(g);
    if (!result.isEmpty()) return false;
    
    Put p = new Put(w.row);
    p.add(w.cfamily, (new String(w.column) + "data").getBytes(), startTs, w.value);
    t.put(p);
    
    p = new Put(w.row);
    p.add(w.cfamily, (new String(w.column) + "lock").getBytes(), startTs, 
        (new String(primary.row) + "@" + new String(primary.column)).getBytes());
    t.put(p);
    return true;
  }
  
  public boolean Commit() throws IOException{
    PWrite primary = writes.get(0);
    if (!Prewrite(primary, primary))
      return false;
    writes.remove(0);
    for (PWrite p:writes){
      if (!Prewrite(p, primary))
        return false;
    }
    long commitTs = System.currentTimeMillis();
    
    Configuration conf = HBaseConfiguration.create();
    HTable t = new HTable(conf, primary.getTableName().getBytes());
    Get g = new Get(primary.row);
    g.setTimeRange(startTs, startTs+1).setMaxVersions(1).addColumn(primary.cfamily, (new String(primary.column) + "lock").getBytes());
    Result result = t.get(g);
    if (!result.isEmpty())
      return false;
	return false;
  }
  
  private void BackoffAndCleanupLocks(byte[] rowkey, byte[] cf, byte[] c) {
    // TODO Auto-generated method stub
    
  }
  
  
  
}
