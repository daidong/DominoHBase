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

	private static Runnable consumer = null;
	
	public static void register(Runnable t){
		this.consumer = t;	
	}
    public static void append(HTriggerEvent hte){
        EventQueue.add(hte);
		t.Notify();
    }

    public static HTriggerEvent poll(){
		if (EventQueue.isEmpty()){
			t.wait();
			return null;
		}
        return EventQueue.poll();
    }
}
