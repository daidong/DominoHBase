package org.apache.hadoop.hbase.trigger;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Trigger {

  private TriggerConf conf;
  private TriggerClient triggerClient;
  
  public Trigger(){
    this.conf = new TriggerConf();
  }
  
  public Trigger(Configuration conf){
    this.conf = (TriggerConf) conf;
  }
  
  public Trigger(Configuration conf, String triggerName){
    this.conf = (TriggerConf) conf;
    setTriggerName(triggerName);
  }
  
  public void setTriggerOnTable(String tableName){
    conf.set("trigger.on.table", tableName);
  }
  public String getTriggerOnTable(){
    return conf.get("trigger.on.table");
  }
  
  public void setTriggerOnColumFamily(String columnFamily){
    conf.set("trigger.on.table.columnfamily", columnFamily);
  }
  public void setTriggerOnColumn(String column){
    conf.set("trigger.on.table.column", column);
  }
  public String getTriggerOnColumnFamily(){
    return conf.get("trigger.on.table.columnfamily");
  }
  public String getTriggerOnColumn(){
    return conf.get("trigger.on.table.column");
  }
  
  
  public void setTriggerName(String tn){
    conf.set("trigger.name", tn);
  }
  
  public TriggerClient getTriggerClient(){
    return this.triggerClient;
  }
  
  public void setWorkingDirectory(Path dir){
    conf.set("trigger.work.dir", dir.toString());
  }
  
  public void setActionClass(Class<? extends HTriggerAction> ta){
    conf.setClass("trigger.action.class", ta, HTriggerAction.class);
  }
  
  public void setActionClassName(String name){
    conf.set("trigger.action.class.name", name);
  }
  
  public Class getActionClass(){
    return conf.getClass("trigger.action.class", DefaultTriggerAction.class);
  }
  
  public void setTriggerId(int i){
    conf.set("trigger.id", String.valueOf(i));
  }
  
  public String getTriggerId(){
    return conf.get("trigger.id");
  }
  
  public void setJarByClass(Class<?> cls){
    conf.setJarByClass(cls);
  }
  
  public String getJar(){
    return conf.getJar();
  }

  public void submit() throws Exception {
    // connect to the HMaster and submit the job
    triggerClient = new TriggerClient(conf);
    int id  = triggerClient.submitJobInternal(conf);
    setTriggerId(id);
  }

  public boolean waitForCompletion() throws Exception{
    submit();
    return true;
  }
}
