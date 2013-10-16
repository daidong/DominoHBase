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

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-1-15
 * Time: 下午11:32
 * To change this template use File | Settings | File Templates.
 */
public abstract class HTriggerAction{
    long MAX_ROUND = Long.MAX_VALUE; 
    
    private static final Log LOG = LogFactory.getLog(HTriggerAction.class);
    
    public String TestAlive(){
      return "Lives";
    }
    HTrigger belongToInst = null;
    long round = 0L;
    
    public abstract void action(HTriggerEvent hte);
    public abstract boolean filter(HTriggerEvent hte);
    
    public void setRound(long r){
      this.round = r;
    }
    public long getRound(){
      return this.round;
    }
    
    public HTrigger getHTrigger(){
      return this.belongToInst;
    }
    public long getCurrentRound(){
      return round;
    }

    public void setHTrigger(HTrigger hTrigger) {
      this.belongToInst = hTrigger;
    }
    
    public Method getIncr(){
      Class<?> currentClass = this.getClass();
      Class<?>[] cargs = new Class[2];
      cargs[0] = HTriggerEvent.class;
      cargs[1] = PartialResult.class;
      Method m = null;
      try {
        m = currentClass.getMethod("incr", cargs);
      } catch (SecurityException e) {
        return null;
      } catch (NoSuchMethodException e) {
        return null;
      }
      return m;
    }
    
    public boolean filterWrapper(HTriggerEvent hte){
      //initial vesion must pass the check
      if (hte.isInitEvent())
        return true;
      else
        return this.filter(hte);
    }
    
    public void actionWrapper(HTriggerEvent hte){
      //Do some before work
      System.out.println("HTriggerAction Begins at " + new String(hte.getRowKey()));
      this.setRound((hte.getVersion() + 1) % MAX_ROUND); 
      this.action(hte);
      System.out.println("HTriggerAction After at " + new String(hte.getRowKey()));
      //Do some after work
    }
}
