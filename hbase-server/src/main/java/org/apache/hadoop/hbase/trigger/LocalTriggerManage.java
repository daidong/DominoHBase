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

    public static boolean register(HTrigger trigger){
      ArrayList<HTrigger> currTriggers = activeTriggers.get(trigger.getHTriggerKey());
      if (currTriggers == null){
        currTriggers = new ArrayList<HTrigger>();
      }
      currTriggers.add(trigger);
      activeTriggers.put(trigger.getHTriggerKey(), currTriggers);
      System.out.println("register finished");
      return true;
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
}
