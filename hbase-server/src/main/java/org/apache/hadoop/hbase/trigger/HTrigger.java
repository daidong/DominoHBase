/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
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
    public int triggerId;
    private HTriggerKey htk;
    private TriggerConf conf;
    private HTriggerAction action;
    
    public HTrigger(int triggerId){
      this.triggerId = triggerId;
    }
    
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
      File jarFile = new File("/tmp/trigger/triggerJar/" + String.valueOf(triggerId) + "/trigger.jar");
      File libDir = new File("/tmp/trigger/triggerJar/"+String.valueOf(triggerId)+"/lib");
      
      File tmpDir = new File("/tmp/hbase/triggerJar/"+String.valueOf(triggerId)+"/");
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
      
      /*
      Runtime.getRuntime().addShutdownHook(new Thread(){
        public void run(){
          try {
            FileUtil.fullyDelete(workDir);
          } catch (IOException e){
          }
        }
      });
      */
      
      System.out.println("work dir: " + workDir.getPath());
      RunTrigger.unJar(jarFile, workDir);
      System.out.println("init class middle, unjar finishes");
      
      ArrayList<URL> classPath = new ArrayList<URL>();

      /**
       * add classpath added from "tmpjars"
       */
      if (libDir.exists()){
        File[] jarLibs = libDir.listFiles();
        if (jarLibs != null){
          for (int i = 0; i < jarLibs.length; i++){
            System.out.println("load jar libs..." + jarLibs[i].toURL().getPath());
            classPath.add(jarLibs[i].toURL());
          }
        }
      }
      
      classPath.add(new File(workDir+"/").toURL());
      classPath.add(jarFile.toURL());
      classPath.add(new File(workDir, "classes/").toURL());
      File[] libs = new File(workDir, "lib").listFiles();
      if (libs != null){
        for (int i = 0; i < libs.length; i++){
          System.out.println("load libs..." + libs[i].toURL().getPath());
          classPath.add(libs[i].toURL());
        }
      }
      
      ClassLoader loader = new URLClassLoader(classPath.toArray(new URL[0]));
      Thread.currentThread().setContextClassLoader(loader);
     
      String actionClassName = conf.getActionClassName();
      System.out.println("init class middle, actionClassName: " + actionClassName);
      Class actionClassWithLoader = Class.forName(actionClassName, true, loader);
      System.out.println("init class middle, 2");
      this.action = (HTriggerAction) actionClassWithLoader.getConstructor().newInstance();
      this.action.setHTrigger(this);
      System.out.println("init class finished, test: action class: " + this.action.TestAlive());
      
    }
    
    public void setAction(HTriggerAction naction){
      this.action = naction;
    }

    public int getTriggerId(){
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
        
        if (triggerId != hTrigger.triggerId) 
          return false;
        
        return true;
    }

    @Override
    public int hashCode() {
        return (int) (triggerId ^ (triggerId >>> 32));
    }
}
