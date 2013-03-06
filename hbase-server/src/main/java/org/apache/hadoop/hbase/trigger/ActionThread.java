package org.apache.hadoop.hbase.trigger;

import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-3-2
 * Time:
 * To change this template use File | Settings | File Templates.
 */
public class ActionThread implements Runnable {

    private ConcurrentLinkedQueue<HTriggerEvent> inputDS = null;
    private HTriggerAction actionClass = null;
    private HTrigger ht = null;

    /**
     * The work in run() is simple:
     * 1, init actionClass according to users submission;
     * 2, wait on the inputDS queue and once new element exist,
     * call actionClass.action(newElement)
     * 3, Report Status Periodically
     */
    @Override
    public void run() {
		//Init Actionclass
		HTriggerAction userAction = null;
		long lastTS = EnvironmentEdgeManager.currentTimeMillis();
		while (true){
			if (inputDS.isEmpty()){
				this.wait();
			} else {
				HTriggerEvent currEvent = inputDS.poll();
				if (userAction.filter(currEvent)){
					userAction.action(currEvent);
				}
			}
			long currTS = EnvironmentEdgeManager.currentTimeMillis();
			if (currTS - lastTS > 1000){
				report(ht);
				lastTS = currTS;
			}
		}
		
    }

    public void report(HTrigger ht){

    }
    public ActionThread(){
        inputDS = new ConcurrentLinkedQueue<HTriggerEvent>();
    }
    public ActionThread(HTrigger ht){
        this();
        this.ht = ht;
    }

    public void feed(HTriggerEvent hte){
        inputDS.add(hte);
		this.Notify();
    }

}
