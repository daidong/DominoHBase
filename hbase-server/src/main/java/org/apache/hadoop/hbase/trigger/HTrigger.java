package org.apache.hadoop.hbase.trigger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.RunTrigger;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-1-15
 * Time:
 * To change this template use File | Settings | File Templates.
 */
public class HTrigger {
    private int triggerId;
    private HTriggerKey htk;
    private TriggerConf conf;
    private HTriggerAction action;
    
    public HTrigger(int triggerId, HTriggerKey htk, TriggerConf conf) throws Exception{
      this.triggerId = triggerId;
      this.htk = htk;
      this.conf = conf;
      initClass();
    }

    public TriggerConf getConf(){
      return this.conf;
    }
    
    public HTriggerKey getHTriggerKey(){
      return this.htk;
    }
    
    public void initClass() throws IOException, Exception, IllegalAccessException{
      /**
       * setup action class
       */
      File jarFile = new File("/tmp/hbase/triggerJar/" + String.valueOf(triggerId) + "/trigger.jar");
      
      File tmpDir = new File("/tmp/hbase/trigger/"+String.valueOf(triggerId)+"/");
      tmpDir.mkdirs();
      if (!tmpDir.isDirectory()){
        System.err.println("Mkdirs failed to create " + tmpDir);
        System.exit(-1);
      }
      final File workDir = File.createTempFile("trigger-unjar", "", tmpDir);
      workDir.delete();
      workDir.mkdirs();
      if (!workDir.isDirectory()){
        System.err.println("Mkdirs failed to create " + workDir);
        System.exit(-1);
      }
      
      Runtime.getRuntime().addShutdownHook(new Thread(){
        public void run(){
          try {
            FileUtil.fullyDelete(workDir);
          } catch (IOException e){
          }
        }
      });
      
      RunTrigger.unJar(jarFile, workDir);
      
      ArrayList<URL> classPath = new ArrayList<URL>();
      classPath.add(new File(workDir+"/").toURL());
      classPath.add(jarFile.toURL());
      classPath.add(new File(workDir, "classes/").toURL());
      File[] libs = new File(workDir, "lib").listFiles();
      if (libs != null){
        for (int i = 0; i < libs.length; i++){
          classPath.add(libs[i].toURL());
        }
      }
      
      ClassLoader loader = new URLClassLoader(classPath.toArray(new URL[0]));
      Thread.currentThread().setContextClassLoader(loader);
      String actionClassName = conf.getActionClassName();
      Class actionClassWithLoader = Class.forName(actionClassName, true, loader);
      this.action = (HTriggerAction) actionClassWithLoader.newInstance();
    }
    
    public void setAction(HTriggerAction naction){
      this.action = naction;
    }

    public long getTriggerId(){
        return this.triggerId;
    }
    
    public HTriggerAction getActionClass(){
      return this.action;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HTrigger hTrigger = (HTrigger) o;

        if (triggerId != hTrigger.triggerId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (triggerId ^ (triggerId >>> 32));
    }
}
