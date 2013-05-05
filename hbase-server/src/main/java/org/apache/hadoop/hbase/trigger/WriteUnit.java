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
 */
public class WriteUnit {
  
  private static final Log LOG = LogFactory.getLog(WriteUnit.class);
  
  private Put p = null;
  private byte[] tableName = null;
  
  public WriteUnit(byte[] tname, Put p){
    this.tableName = tname;
    this.p = p;
  }
  
  public WriteUnit(HTriggerAction action, byte[] tname, byte[] row, byte[] columnFamilly, byte[] column ,byte[] value){
    this.tableName = tname;
    p = new Put(row);
    LOG.info("In WriteUnit Construct, we create a Put instance on version: " + action.getCurrentRound());
    p.add(columnFamilly, column, action.getCurrentRound(), value);
  }
  
  public byte[] getTableName(){
    return this.tableName;
  }
  public Put getPut(){
    return this.p;
  }
 
  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append(new String(tableName) + " " + new String(p.getRow()));
    return sb.toString();
  }
}
