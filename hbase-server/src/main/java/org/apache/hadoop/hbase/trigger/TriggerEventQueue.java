package org.apache.hadoop.hbase.trigger;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-3-2
 * Time: 下午9:32
 * To change this template use File | Settings | File Templates.
 */
public class TriggerEventQueue {
    private static ConcurrentLinkedQueue<HTriggerEvent> EventQueue =
            new ConcurrentLinkedQueue<HTriggerEvent>();

    public static void append(HTriggerEvent hte){
        EventQueue.add(hte);
    }

    public static HTriggerEvent poll(){
        return EventQueue.poll();
    }
}
