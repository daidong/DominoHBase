package org.apache.hadoop.hbase.trigger;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;

public class TriggerConf extends Configuration{
  
  public TriggerConf(){
    super();
    addTriggerResources(this);
  }
  
  public TriggerConf(Configuration conf){
    super(conf);
    addTriggerResources(this);
  }
  
  private void addTriggerResources(Configuration conf){
    //conf.addResource("core-site.xml");
    //conf.addResource("hdfs-site.xml");
    conf.addResource("trigger-default.xml");
  }

  /*
  public static Configuration create(){
    Configuration conf = new Configuration();
    return addTriggerResources(conf);
  }
  */
  
  public String getTriggerName() {
    return get("trigger.name");
  }
  public void setTriggerName(String name){
    set("trigger.name", name);
  }

  public void setJar(String jar) {
    set("trigger.jar", jar);
  }
  
  public String getJar(){
    return get("trigger.jar");
  }

  public void setJarByClass(Class<?> cls) {
    String jar = findContainingJar(cls);
    if (jar != null){
      setJar(jar);
    }
  }
  
  private static String findContainingJar(Class my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for(Enumeration itr = loader.getResources(class_file);
          itr.hasMoreElements();) {
        URL url = (URL) itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
  
  public String getTriggerOnColumnFamily(){
    return get("trigger.on.table.columnfamily");
  }
  /**
   * If users do not specify which column the trigger should monitor at, 
   * then we return the default "*". The default "*" will used in HTriggerKey comparator
   * @return trigger.on.table.column
   */
  public String getTriggerOnColumn(){
    return get("trigger.on.table.column", "*");
  }
  public String getTriggerOnTable(){
    return get("trigger.on.table");
  }
  public Class getActionClass(){
    return getClass("trigger.action.class", DefaultTriggerAction.class);
  }

  public String getActionClassName() {
    return get("trigger.action.class.name");
  }
  

}
