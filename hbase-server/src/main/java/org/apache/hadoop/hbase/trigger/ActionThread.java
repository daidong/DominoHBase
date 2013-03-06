package org.apache.hadoop.hbase.trigger;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-3-2
 * Time: 下午10:34
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
    }

}
