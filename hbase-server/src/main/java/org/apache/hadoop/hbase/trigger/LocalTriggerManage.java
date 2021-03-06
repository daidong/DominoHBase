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
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-3-2
 * Time: 下午6:56
 * To change this template use File | Settings | File Templates.
 */
public class LocalTriggerManage {
     
    public static String prettyPrint() {
      StringBuilder sb = new StringBuilder();
      for (HTriggerKey htk:activeTriggers.keySet()){
        sb.append(htk.toString());
      }
      return sb.toString();
    }
    
    private static HashMap<HTriggerKey, ArrayList<HTrigger>> activeTriggers = new HashMap<HTriggerKey, ArrayList<HTrigger>>();
    private static HashMap<Integer, HTriggerKey> registeredTriggers = new HashMap<Integer, HTriggerKey>();

    public static boolean register(HTrigger trigger){
      ArrayList<HTrigger> currTriggers = activeTriggers.get(trigger.getHTriggerKey());
      /**
       * As we only use HTriggerKey when we want to distinguish different triggers.
       * For future operations, we need to record all the id->HTriggerKey map.
       */
      registeredTriggers.put(trigger.getTriggerId(), trigger.getHTriggerKey());
      
      if (currTriggers == null){
        currTriggers = new ArrayList<HTrigger>();
      }
      currTriggers.add(trigger);
      activeTriggers.put(trigger.getHTriggerKey(), currTriggers);
      System.out.println("register finished");
      return true;
    }
    
    /**
     * The HTrigger as argument here is incomplete. It only contains triggerId, so 
     * we need to get the relevant HTriggerKey by query registeredTriggers instead of 
     * calling HTrigger.getHTriggerKey();
     * 
     * Here, we query currentTriggers which monitors on the same trigger key, and move
     * the relevant trigger. The HTrigger instances are equal if their ids are equal. 
     */
    public static boolean unregister(HTrigger t){
      System.out.println("inside unregister");
      HTriggerKey currentTriggerKey = registeredTriggers.get(t.getTriggerId());
      if (currentTriggerKey != null){
        //System.out.println("find currentTriggerKey responding to trigger id: " + t.getTriggerId() + " is " + currentTriggerKey);
        ArrayList<HTrigger> currTriggers = activeTriggers.get(currentTriggerKey);
        if (currTriggers == null)
          return true;
        //System.out.println("find current trigger: " + currTriggers.get(0) + " for trigger id: " + t.getTriggerId());
        currTriggers.remove(t);
        if (currTriggers.size() == 0){
          activeTriggers.remove(currentTriggerKey);
        } else {
          activeTriggers.put(currentTriggerKey, currTriggers);
        }
        return true;
      } else {
        return true;
      }
    }
    
    public static boolean cancel(HTrigger t){
        for (Map.Entry trigger: activeTriggers.entrySet()){
            ArrayList<HTrigger> triggerSet = (ArrayList <HTrigger>) trigger.getValue();
            if (triggerSet.contains(t)){
                triggerSet.remove(t);
                activeTriggers.put((HTriggerKey)trigger.getKey(), triggerSet);
            }
        }
        return true;
    }

    public static ArrayList<HTrigger> getTriggerByMeta(HTriggerKey tk){
        return activeTriggers.get(tk);
    }
    public static boolean containsTrigger(HTriggerKey tk){
        return activeTriggers.containsKey(tk);
    }
    
    public static boolean containsAccumulator(HTriggerKey tk){
      for (HTrigger ht:activeTriggers.get(tk)){
        TRIGGERTYPE curr = TRIGGERTYPE.fromString(ht.getConf().getTriggerType());
        if (curr == TRIGGERTYPE.ACCUMULATOR ||
            curr == TRIGGERTYPE.ACCUMULATORWITHCONVERGE)
          return true;
      }
      return false;
    }
    
    public static boolean containsConvergeOrIncr(HTriggerKey tk){
        for (HTrigger ht:activeTriggers.get(tk)){
          TRIGGERTYPE curr = TRIGGERTYPE.fromString(ht.getConf().getTriggerType());
          
          if (ht.getActionClass().getIncr() != null)
            return true;
          
          //System.out.println("check type: " + ht.triggerId + " type is: " + ht.getConf().getTriggerType());
          if (curr == TRIGGERTYPE.CONVERGE
                || curr == TRIGGERTYPE.INITIALWITHCONVERGE
                || curr == TRIGGERTYPE.ACCUMULATORWITHCONVERGE)
            return true;
        }
        return false;
    }
}
