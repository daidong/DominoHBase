package org.apache.hadoop.hbase.trigger;

import org.apache.hadoop.conf.Configuration;

public class TriggerConf extends Configuration{
  
  public TriggerConf(){
    super();
    addTriggerResources(this);
  }
  
  public static Configuration addTriggerResources(Configuration conf){
    conf.addResource("trigger-default.xml");
    return conf;
  }
  
  public static Configuration create(){
    Configuration conf = new Configuration();
    return addTriggerResources(conf);
  }

  public String getJar() {
    // TODO Auto-generated method stub
    return null;
  }

  public Object getJobName() {
    // TODO Auto-generated method stub
    return null;
  }

  public void setJobName(String name) {
    // TODO Auto-generated method stub
    
  }

  public void setJar(String string) {
    // TODO Auto-generated method stub
    
  }
  
  

}
