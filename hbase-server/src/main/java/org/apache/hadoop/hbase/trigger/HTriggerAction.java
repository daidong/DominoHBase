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

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-1-15
 * Time: 下午11:32
 * To change this template use File | Settings | File Templates.
 */
public abstract class HTriggerAction{
    long MAX_ROUND = Long.MAX_VALUE; 
    
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
    
    public void actionWrapper(HTriggerEvent hte){
      //Do some before work
      this.setRound((hte.getVersion() + 1) % MAX_ROUND); 
      this.action(hte);
      //Do some after work
    }
}
