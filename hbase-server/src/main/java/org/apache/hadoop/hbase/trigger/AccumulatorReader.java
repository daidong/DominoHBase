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
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 *  @author daidong
 *  This class is used for accumulator pattern.
 *  In 'Accumulator Pattern', developers write particial results from different action functions into a 
 *  HTable's column-family and Domino will automatically gather these data together according to your 
 *  synchronous model (strict model, eventually synchronous model, or asynchronous model).
 *  
 *  HINT:
 *  This is not necessary for every trigger that monitors on column-family instead of column. It only used on
 *  Triggers whose action function needs to read other columns in the same column-family to accumulate the final
 *  result.
 *
 */
public class AccumulatorReader {
  
  private static final Log LOG = LogFactory.getLog(AccumulatorReader.class);
  private Result result = null;
  private Map<byte[], byte[]> values = null;
  
  public AccumulatorReader(byte[] tableName, byte[] columnFamily, byte[] rowKey, long version, HRegion r) throws IOException{
    Get get = new Get(rowKey);
    //Get all elements that has version number less than 'version'. setTimeRange is Exclusive API
    get.setTimeRange(0, version).setMaxVersions(1).addFamily(columnFamily);
    this.result = r.get(get, null);
        
    //LOG.info("After Get Result");
    
    values  = this.result.getFamilyMap("nodes".getBytes());
    values.remove("_partial_result_".getBytes());
    
    //LOG.info("After Remove Partial");
  }
  
  public Result GetValues(){
    return this.result;
  }
  
  public Map<byte[], byte[]> GetMapValues(){
    return this.values;
  }
}
