package org.apache.hadoop.hbase.trigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-3-2
 * To change this template use File | Settings | File Templates.
 */
public class ActionThreadManager implements Runnable{
  ConcurrentHashMap<HTrigger, ActionThread> actionThreads = null;
  private boolean registed;

  public ActionThreadManager(){
    actionThreads = new ConcurrentHashMap<HTrigger, ActionThread>();
    HTriggerEventQueue.register(this);
  }
  
  @Override
  public void run(){
    while (true){
      dispatch();
    }
  }
  public void dispatch(){
    HTriggerEvent hte = null;
    
    //This hte will wait until has an event
    try {
      hte = HTriggerEventQueue.poll();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    
    HTriggerKey currentFiredKey = hte.getEventTriggerKey();
    System.out.println("ActionThreadManager get a fired event: " + currentFiredKey.toString());
    
    ArrayList<HTrigger> waitOnTriggers = LocalTriggerManage.getTriggerByMeta(currentFiredKey);
    
    for (HTrigger ht : waitOnTriggers){
      if (actionThreads.containsKey(ht)){
        dispatch(actionThreads.get(ht), hte);
      } else {
        ActionThread curThread = new ActionThread(ht.getActionClass());
        Thread actionThread = new Thread(curThread);
        actionThread.start();
        
        actionThreads.put(ht, curThread);
        dispatch(curThread, hte);
      }
    }
  }

  private void dispatch(ActionThread actionThread, HTriggerEvent hte) {
    actionThread.feed(hte);
  }

  public void restart(HTrigger t){
  }
  public void kill(HTrigger t){
  }
}
