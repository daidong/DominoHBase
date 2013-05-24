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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;

/**
 * A WriteUnit contains 
 * @author daidong
 * 
 * We must make WriteUnit automatically support the incr method.
 */
public class WriteUnit {
  
  private static final Log LOG = LogFactory.getLog(WriteUnit.class);
  
  private Put p = null;
  private byte[] tableName = null;
  private byte[] row = null;
  private byte[] columnFamily = null;
  private byte[] value = null;
  private boolean writeToIncr = false;
  private Put accompPut = null;
  
  public WriteUnit(byte[] tname, Put p){
    this.tableName = tname;
    this.p = p;
  }
  
  public WriteUnit(HTriggerAction action, byte[] tname, byte[] row, byte[] columnFamily, byte[] column ,byte[] value){
    this.tableName = tname;
    p = new Put(row, action.getCurrentRound());
    /*
    LOG.info("WriteUnit Put with version: " + action.getCurrentRound() + " to " + 
              new String(tname) + " on " + new String(row) + " at " + new String(columnFamily) + ":" + 
              new String(column) + " with value: " + new String(value));
     */
    p.add(columnFamily, column, value);
    this.row = row;
    this.columnFamily = columnFamily;
    this.value = value;
  }
  
  public WriteUnit(HTriggerAction action, byte[] tname, byte[] row, 
      byte[] columnFamily, byte[] column ,byte[] value, boolean writeToIncr){
    this(action, tname, row, columnFamily, column, value);
    this.accompPut= new Put(row, action.getCurrentRound());
    this.accompPut.add(columnFamily, "_partial_result_".getBytes(), value);
    this.writeToIncr = writeToIncr;
  }
  
  public boolean isWriteToIncr(){
    return this.writeToIncr;
  }
  
  public byte[] getRow(){
    return this.row;
  }
  public byte[] getCF(){
    return this.columnFamily;
  }
  public byte[] getValue(){
    return this.value;
  }
  public byte[] getTableName(){
    return this.tableName;
  }
  public Put getPut(){
    return this.p;
  }
  public Put getAccompPut(){
    return this.accompPut;
  }
  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append(new String(tableName) + " " + new String(p.getRow()));
    return sb.toString();
  }
}
