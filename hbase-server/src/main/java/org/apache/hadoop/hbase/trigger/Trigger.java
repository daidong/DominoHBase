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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Trigger {

  private TriggerConf conf;
  private TriggerClient triggerClient;
  
  public Trigger(){
    this.conf = new TriggerConf();
  }
  
  public Trigger(Configuration conf){
    this.conf = (TriggerConf) conf;
  }
  
  public Trigger(Configuration conf, String triggerName){
    this.conf = (TriggerConf) conf;
    setTriggerName(triggerName);
  }
  
  public void setTriggerOnTable(String tableName){
    conf.set("trigger.on.table", tableName);
  }
  public String getTriggerOnTable(){
    return conf.get("trigger.on.table");
  }
  
  public void setTriggerOnColumFamily(String columnFamily){
    conf.set("trigger.on.table.columnfamily", columnFamily);
  }
  public void setTriggerOnColumn(String column){
    conf.set("trigger.on.table.column", column);
  }
  public String getTriggerOnColumnFamily(){
    return conf.get("trigger.on.table.columnfamily");
  }
  public String getTriggerOnColumn(){
    return conf.get("trigger.on.table.column");
  }
  
  
  public void setTriggerName(String tn){
    conf.set("trigger.name", tn);
  }
  public String getTriggerName(){
    return conf.get("trigger.name");
  }
  
  public TriggerClient getTriggerClient(){
    return this.triggerClient;
  }
  
  public void setWorkingDirectory(Path dir){
    conf.set("trigger.work.dir", dir.toString());
  }
  
  public void setActionClass(Class<? extends HTriggerAction> ta){
    conf.setClass("trigger.action.class", ta, HTriggerAction.class);
  }
  
  public void setActionClassName(String name){
    conf.set("trigger.action.class.name", name);
  }
  
  public Class getActionClass(){
    return conf.getClass("trigger.action.class", DefaultTriggerAction.class);
  }
  
  public void setTriggerId(int i){
    conf.set("trigger.id", String.valueOf(i));
  }
  
  public String getTriggerId(){
    return conf.get("trigger.id");
  }
  
  public void setJarByClass(Class<?> cls){
    conf.setJarByClass(cls);
  }
  
  public String getJar(){
    return conf.getJar();
  }

  public void submit() throws Exception {
    // connect to the HMaster and submit the job
    triggerClient = new TriggerClient(conf);
    int id  = triggerClient.submitJobInternal(conf);
    setTriggerId(id);
  }

  public boolean waitForCompletion() throws Exception{
    submit();
    return true;
  }
  
  public boolean stop(String tn, int tid) throws Exception{
    triggerClient = new TriggerClient(conf);
    return triggerClient.stop(tn, tid);
  }
}
