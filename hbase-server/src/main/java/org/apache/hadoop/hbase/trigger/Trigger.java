package org.apache.hadoop.hbase.trigger;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Trigger {

  private TriggerConf conf;
  private TriggerClient triggerClient;
  private RunningTrigger info;
  
  public Trigger(){
    this(new Configuration());
  }
  
  public Trigger(Configuration configuration) {
    this(configuration, null);
  }
  
  public Trigger(Configuration conf, String triggerName){
    conf = (TriggerConf) conf;
    setTriggerName(triggerName);
  }
  
  public void setTriggerOnTable(byte[] tableName){
    conf.set("trigger.on.table", tableName.toString());
  }
  public String getTriggerOnTable(){
    return conf.get("trigger.on.table");
  }
  
  public void setTriggerOnColumFamily(byte[] columnFamily){
    conf.set("trigger.on.table.columnfamily", columnFamily.toString());
  }
  public void setTriggerOnColumn(byte[] column){
    conf.set("trigger.on.table.column", column.toString());
  }
  public String getTriggerOnColumnFamily(){
    return conf.get("trigger.on.table.columfamily");
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
    info = triggerClient.submitJobInternal(conf);
    setTriggerId(info.getID());
  }

  public boolean waitForCompletion() throws Exception{
    submit();
    info.waitForCompletion();
    return true;
  }
}
