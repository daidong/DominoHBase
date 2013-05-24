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
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * 
 * @author daidong
 * This class is used to get partial result before create AccumulatorReader.
 * The preemption is not always correct, so we avoid the preemption in current implementation.
 * 
 * Without preemption, we can safely assume that when next event in accumulator happens, the previous 
 * partial result has been written into local sever in order. For example:
 * 
 * at first, all version of data is [ 0, 0, 0, 0 ]
 * after one updates, data became   [ 1, 0, 0, 0 ]
 * after two updates, data became   [ 2, 0, 0, 0 ]
 *  ...
 *  ...
 * Finally,                         [ 2, 2, 2, 2 ]
 * 
 * There is a strict order. and this time only needs results of last time. So, we store the partial result in 
 * current column family's '_partial_result_' column.
 * 
 */
public class PartialResult {
  
  private Result result = null;
  private byte[] tname = null;
  private byte[] rowKey = null;
  private byte[] cf = null;
  private HRegion r = null;
  private byte[] value = null;
  
  public PartialResult(byte[] tableName, byte[] rowKey,  byte[] columnFamily, HRegion r) throws IOException{
    this.tname = tableName;
    this.rowKey = rowKey;
    this.cf = columnFamily;
    this.r = r;
    
    Get get = new Get(rowKey);
    get.addColumn(columnFamily, "_partial_result_".getBytes()).setMaxVersions(1);
    this.result = r.get(get, null);    
    value = result.value();
  }
  public byte[] getValue(){
    return this.value;
  }
  public byte[] getTableName(){
    return this.tname;
  }
  public byte[] getRowKey(){
    return this.rowKey;
  }
  public byte[] getCF(){
    return this.cf;
  }
  public HRegion getRegion(){
    return this.r;
  }
  public Result getPartial(){
    return this.result;
  }
}
