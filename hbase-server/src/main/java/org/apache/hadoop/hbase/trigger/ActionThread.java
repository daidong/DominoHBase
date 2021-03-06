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

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created with IntelliJ IDEA. User: daidong Date: 13-3-2 Time: To change this
 * template use File | Settings | File Templates.
 */
public class ActionThread implements Runnable {

  private static final Log LOG = LogFactory.getLog(ActionThread.class);

  private LinkedBlockingQueue<HTriggerEvent> inputDS = null;
  private HTriggerAction action = null;
  private HTrigger ht = null;

  public ActionThread(HTriggerAction action) {
    inputDS = new LinkedBlockingQueue<HTriggerEvent>();
    this.action = action;
  }

  /**
   * The work in run() is simple: 1, wait on the inputDS queue and once new element exist, call
   * actionClass.action(newElement) 2, Report Status Periodically
   */
  @Override
  public void run() {    
    while (true){
      HTriggerEvent currEvent;
      try {
        currEvent = inputDS.take();
        if (action.filterWrapper(currEvent)){
          action.actionWrapper(currEvent);
        }
      } catch (InterruptedException e) {
        System.out.println("ERROR IN ActionThread run()");
      }
    }
  }
  
  public void PrintQueue(){
    String out = this + ":";
    for (HTriggerEvent e:inputDS){
      out = out + e +"-->";
    }
    LOG.info(out);
  }

  public void feed(HTriggerEvent hte) {
    try {
      //if (hte.isAccEvent() && inputDS.contains(hte)){
      if (inputDS.contains(hte)){
        inputDS.remove(hte);
        System.out.println("remove redundant");
      }
      inputDS.put(hte);
      //PrintQueue();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  /*
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
          if (action.filterWrapper(currEvent)) {
            //Use wrapper instead of just action
            action.actionWrapper(currEvent);
          }
        }
        this.processedNum ++;
      }
    }
  }
  
  public void run() {
	  while(true) {
		try {
			HTriggerEvent currEvent;
			currEvent = inputDS.take();
		  if (action.filter(currEvent)) {
			  action.action(currEvent);
		  }
		  this.processedNum++;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
  }
  */
  /*
  public void feed(HTriggerEvent hte) {
    synchronized(inputDS) {
      inputDS.add(hte);
      inputDS.notify();
      this.feededNum++;
      System.out.println("current haven't processed event num = " + (this.feededNum - this.processedNum) 
    		  + " processedNum="+this.processedNum + " feededNum=" + this.feededNum);
    }
  }
  */


}
