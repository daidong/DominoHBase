package org.apache.hadoop.hbase.trigger;

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
    private static HashMap<HTriggerKey, Set<HTrigger>> activeTriggers;

    public static boolean register(HTriggerKey tk){
        Set<HTrigger> currTriggers = activeTriggers.get(tk);
        currTriggers.add(new HTrigger());
        activeTriggers.put(tk, currTriggers);
        return true;
    }

    public static boolean cancel(HTrigger t){
        for (Map.Entry trigger: activeTriggers.entrySet()){
            Set<HTrigger> triggerSet = (Set <HTrigger>) trigger.getValue();
            if (triggerSet.contains(t)){
                triggerSet.remove(t);
                activeTriggers.put((HTriggerKey)trigger.getKey(), triggerSet);
            }
        }
        return true;
    }

    public static Set<HTrigger> getTriggerByMeta(HTriggerKey tk){
        return activeTriggers.get(tk);
    }
    public static boolean containsTrigger(HTriggerKey tk){
        return activeTriggers.containsKey(tk);
    }
}
