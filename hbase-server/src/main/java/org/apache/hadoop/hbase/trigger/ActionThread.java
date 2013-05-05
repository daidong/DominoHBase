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

import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Created with IntelliJ IDEA. User: daidong Date: 13-3-2 Time: To change this
 * template use File | Settings | File Templates.
 */
public class ActionThread implements Runnable {

  private ConcurrentLinkedQueue<HTriggerEvent> inputDS = null;
  private HTriggerAction action = null;
  private HTrigger ht = null;

  public ActionThread(HTriggerAction action) {
    inputDS = new ConcurrentLinkedQueue<HTriggerEvent>();
    this.action = action;
  }

  /**
   * The work in run() is simple: 1, init actionClass according to users
   * submission; 2, wait on the inputDS queue and once new element exist, call
   * actionClass.action(newElement) 3, Report Status Periodically
   */
  @Override
  public void run() {    
    long lastTS = EnvironmentEdgeManager.currentTimeMillis();
    synchronized (inputDS) {
      while (true) {
        if (inputDS.isEmpty()) {
          try {
            inputDS.wait();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        } else {
          HTriggerEvent currEvent = inputDS.poll();
          if (action.filter(currEvent)) {
            //Use wrapper instead of just action
            action.actionWrapper(currEvent);
            //action.action(currEvent);
          }
        }
        long currTS = EnvironmentEdgeManager.currentTimeMillis();
        if (currTS - lastTS > 1000) {
          // report(ht);
          lastTS = currTS;
        }
      }
    }
  }

  public void feed(HTriggerEvent hte) {
    synchronized(inputDS) {
      inputDS.add(hte);
      inputDS.notify();
    }
  }

}
