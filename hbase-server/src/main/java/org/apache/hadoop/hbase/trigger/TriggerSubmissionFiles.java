package org.apache.hadoop.hbase.trigger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;

import com.sun.org.apache.commons.logging.Log;

public class TriggerSubmissionFiles {

  final public static FsPermission TRIGGER_DIR_PERMISSION = 
    FsPermission.createImmutable((short) 0700);
  final public static FsPermission TRIGGER_FILE_PERMISSION = 
    FsPermission.createImmutable((short) 0644);
  
  /*
  public static Path getStagingDir(TriggerConf conf) throws IOException{
    Path stagingRoot = new Path(conf.get("trigger.staging.root.dir", "/tmp/trigger/staging"));
    //final FileSystem fs = stagingRoot.getFileSystem(conf);
    //return fs.makeQualified(new Path(stagingRoot, "/.staging"));
    return stagingRoot;
  }
  */
  
  public static Path getHDFSStagingDir(){
    Path hdfsRemote = new Path("hdfs://localhost:9000/tmp/hbase/trigger/staging");
    return hdfsRemote;
  }
  
  public static Path getJobConfPath(Path submitTriggerDir) {
    return new Path(submitTriggerDir, "trigger.xml");
  }

  public static Path getTriggerJar(Path submitTriggerDir) {
    return new Path(submitTriggerDir, "trigger.jar");
  }
  
  public static Path getTriggerLibJars(Path submitTriggerDir) {
    return new Path(submitTriggerDir, "libjars/");
  }

  public static void copyAndConfigureFiles(TriggerConf triggerCopy,
      Path submitTriggerDir) throws IOException, InterruptedException {
    
    FileSystem fs = submitTriggerDir.getFileSystem(triggerCopy);
    //submitTriggerDir = fs.makeQualified(submitTriggerDir);
    FsPermission triggerSysPerms = new FsPermission(TriggerSubmissionFiles.TRIGGER_FILE_PERMISSION);
    FileSystem.mkdirs(fs, submitTriggerDir, triggerSysPerms);
    
    String originalJarPath = "/tmp/hbase/triggerJar/trigger.jar";
    //String originalJarPath = triggerCopy.getJar();

    /**
     * copy jar file to HMaster's fs
     */
    if (originalJarPath != null) { 
      Path submitJarFile = TriggerSubmissionFiles.getTriggerJar(submitTriggerDir);
      triggerCopy.setJar(submitJarFile.toString());
      fs.copyFromLocalFile(new Path(originalJarPath), submitJarFile);
      fs.setPermission(submitJarFile, 
          new FsPermission(TriggerSubmissionFiles.TRIGGER_FILE_PERMISSION));
    } 
    
    /**
     * copy libjars to HMaster's fs
     */
    String libjars = triggerCopy.get("tmpjars");
    Path libjarsDir = TriggerSubmissionFiles.getTriggerLibJars(submitTriggerDir);
    
    if (libjars != null){
      System.out.println("We have LibJars, so copy them to hdfs, target: " + libjarsDir.toString());
      FileSystem.mkdirs(fs, libjarsDir, TriggerSubmissionFiles.TRIGGER_DIR_PERMISSION);
      String[] libjarsArr = libjars.split(",");
      for (String tmpJar : libjarsArr){
        Path localLibJar = new Path(tmpJar);
        Path hdfsLibJar = new Path(libjarsDir, localLibJar.getName());
        fs.copyFromLocalFile(localLibJar, hdfsLibJar);
        fs.setPermission(hdfsLibJar, new FsPermission(TriggerSubmissionFiles.TRIGGER_FILE_PERMISSION));
      }
    }
    
  }
}
