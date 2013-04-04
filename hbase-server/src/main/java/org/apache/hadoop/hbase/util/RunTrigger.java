package org.apache.hadoop.hbase.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;

public class RunTrigger {

  public static void unJar(File jarFile, File toDir) throws IOException {
    JarFile jar = new JarFile(jarFile);
    try {
      Enumeration entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = (JarEntry)entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = jar.getInputStream(entry);
          try {
            File file = new File(toDir, entry.getName());
            if (!file.getParentFile().mkdirs()) {
              if (!file.getParentFile().isDirectory()) {
                throw new IOException("Mkdirs failed to create " + 
                                      file.getParentFile().toString());
              }
            }
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      jar.close();
    }
  }
  
  /**
   * @param args
   * @throws Throwable 
   */
  public static void main(String[] args) throws Throwable {
    String usage = "RunTrigger jarFile [mainClass] args...";
    
    if (args.length < 1){
      System.err.println(usage);
      System.exit(-1);
    }
    int firstArg = 0;
    String fileName = args[firstArg++];
    File file = new File(fileName);
    File tmpJarFile = new File("/tmp/hbase/triggerJar/trigger.jar");
    if (tmpJarFile.exists()){
      tmpJarFile.delete();
    }
    FileUtils.copyFile(file, tmpJarFile);
    
    String mainClassName = null;
    JarFile jarFile;
    try {
      jarFile = new JarFile(fileName);
    } catch (IOException e) {
      throw new IOException("Error opening trigger jar: " + fileName).initCause(e);
    }
    
    Manifest manifest = jarFile.getManifest();
    if (manifest != null){
      mainClassName = manifest.getMainAttributes().getValue("Main-Class");
    }
    jarFile.close();
    System.out.println("Manifest From Jar: " + mainClassName);
    
    if (mainClassName == null){
      if (args.length < 2){
        System.err.println(usage);
        System.exit(-1);
      }
      mainClassName = args[firstArg++];
    }
    System.out.println("Manifest class from argument: " + mainClassName);
    
    mainClassName = mainClassName.replace("/", ".");
    
    //'hbase.tmp.dir'
    File tmpDir = new File("/tmp/hbase/trigger/");
    
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
    
    unJar(file, workDir);
    
    ArrayList<URL> classPath = new ArrayList<URL>();
    classPath.add(new File(workDir+"/").toURL());
    classPath.add(file.toURL());
    classPath.add(new File(workDir, "classes/").toURL());
    File[] libs = new File(workDir, "lib").listFiles();
    if (libs != null){
      for (int i = 0; i < libs.length; i++){
        classPath.add(libs[i].toURL());
      }
    }
    
    ClassLoader loader = new URLClassLoader(classPath.toArray(new URL[0]));
    Thread.currentThread().setContextClassLoader(loader);
    Class<?> mainClass = Class.forName(mainClassName, true, loader);
    Method main = mainClass.getMethod("main", new Class[] {
        Array.newInstance(String.class, 0).getClass()
    });
    String[] newArgs = Arrays.asList(args).subList(firstArg, args.length).toArray(new String[0]);
    try {
      main.invoke(null, new Object[] { newArgs });
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
  }

}
