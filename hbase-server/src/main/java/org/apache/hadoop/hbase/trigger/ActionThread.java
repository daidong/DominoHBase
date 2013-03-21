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
    private HTriggerAction action = null;
    private HTrigger ht = null;

    public ActionThread(HTriggerAction action){
      inputDS = new ConcurrentLinkedQueue<HTriggerEvent>();
      this.action = action;
    }
    /**
     * The work in run() is simple:
     * 1, init actionClass according to users submission;
     * 2, wait on the inputDS queue and once new element exist,
     * call actionClass.action(newElement)
     * 3, Report Status Periodically
     */
    @Override
    public void run() {
		long lastTS = EnvironmentEdgeManager.currentTimeMillis();
		while (true){
			if (inputDS.isEmpty()){
				try {
          this.wait();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
			} else {
				HTriggerEvent currEvent = inputDS.poll();
				if (action.filter(currEvent)){
				  action.action(currEvent);
				}
			}
			long currTS = EnvironmentEdgeManager.currentTimeMillis();
			if (currTS - lastTS > 1000){
				//report(ht);
				lastTS = currTS;
			}
		}
		
    }

    public void feed(HTriggerEvent hte){
        inputDS.add(hte);
        this.notify();
    }

}
