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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-3-2
 * To change this template use File | Settings | File Templates.
 */
public class ActionThreadManager implements Runnable{
  
  ConcurrentHashMap<HTrigger, ActionThread> actionThreads = null;

  public ActionThreadManager(){
    actionThreads = new ConcurrentHashMap<HTrigger, ActionThread>();
    HTriggerEventQueue.register(this);
  }
  
  /**
   * @author daidong
   * LOG 2013/05/04
   * We eliminate the 'dispatch' function to reduce the function call times.
   */
  @Override
  public void run(){
    while (true){
      HTriggerEvent hte = null;
      
      //This hte will wait until has an event
      try {
        hte = HTriggerEventQueue.poll();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      
      HTriggerKey currentFiredKey = hte.getEventTriggerKey();
      ArrayList<HTrigger> waitOnTriggers = LocalTriggerManage.getTriggerByMeta(currentFiredKey);
      
      for (HTrigger ht : waitOnTriggers){
        if (actionThreads.containsKey(ht)){
          actionThreads.get(ht).feed(hte);
        } else {
          ActionThread curThread = new ActionThread(ht.getActionClass());
          Thread actionThread = new Thread(curThread);
          actionThread.start();
          
          actionThreads.put(ht, curThread);
          curThread.feed(hte);
        }
      }
    }
  }

  public void restart(HTrigger t){
  }
  public void kill(HTrigger t){
  }
}
