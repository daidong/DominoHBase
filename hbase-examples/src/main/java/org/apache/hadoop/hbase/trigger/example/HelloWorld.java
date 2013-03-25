package org.apache.hadoop.hbase.trigger.example;

import org.apache.hadoop.hbase.trigger.HTriggerAction;
import org.apache.hadoop.hbase.trigger.HTriggerEvent;
import org.apache.hadoop.hbase.trigger.Trigger;

public class HelloWorld extends HTriggerAction{

  @Override
  public void action(HTriggerEvent hte) {
    System.out.println("Hello Trigger Test:");
    //System.out.println(hte.getNewValue()+" at " + hte.getNewTS());
  }

  @Override
  public boolean filter(HTriggerEvent hte) {
    return true;
  }
  
  public static void main(String[] args) throws Exception{
    Trigger trigger = new Trigger();
    trigger.setTriggerName("HelloWorldTrigger");
    trigger.setTriggerOnTable("hello");
    trigger.setTriggerOnColumFamily("content");
    trigger.setTriggerOnColumn("zh");
    
    trigger.setActionClass(HelloWorld.class);
    trigger.setActionClassName("HelloWorld");
    trigger.setJarByClass(HelloWorld.class);
    
    trigger.submit();
  }

}
