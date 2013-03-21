package org.apache.hadoop.hbase.trigger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;

public class TriggerSubmissionFiles {

  final public static FsPermission TRIGGER_DIR_PERMISSION = 
    FsPermission.createImmutable((short) 0700);
  final public static FsPermission TRIGGER_FILE_PERMISSION = 
    FsPermission.createImmutable((short) 0644);
  
  public static Path getStagingDir(TriggerConf conf) throws IOException{
    Path stagingRoot = new Path(conf.get("trigger.staging.root.dir", "/tmp/trigger/staging"));
    final FileSystem fs = stagingRoot.getFileSystem(conf);
    return fs.makeQualified(new Path(stagingRoot, "/.staging"));
  }

  public static Path getJobConfPath(Path submitTriggerDir) {
    return new Path(submitTriggerDir, "trigger.xml");
  }

  public static Path getTriggerJar(Path submitTriggerDir) {
    return new Path(submitTriggerDir, "trigger.jar");
  }

  public static void copyAndConfigureFiles(TriggerConf triggerCopy,
      Path submitTriggerDir) throws IOException, InterruptedException {
    
    FileSystem fs = submitTriggerDir.getFileSystem(triggerCopy);
    submitTriggerDir = fs.makeQualified(submitTriggerDir);
    FsPermission triggerSysPerms = new FsPermission(TriggerSubmissionFiles.TRIGGER_FILE_PERMISSION);
    FileSystem.mkdirs(fs, submitTriggerDir, triggerSysPerms);
    
    String originalJarPath = triggerCopy.getJar();

    if (originalJarPath != null) {           // copy jar to JobTracker's fs
      // use jar name if job is not named. 
      if ("".equals(triggerCopy.getTriggerName())){
        triggerCopy.setTriggerName(new Path(originalJarPath).getName());
      }
      Path submitJarFile = TriggerSubmissionFiles.getTriggerJar(submitTriggerDir);
      triggerCopy.setJar(submitJarFile.toString());
      fs.copyFromLocalFile(new Path(originalJarPath), submitJarFile);
      fs.setPermission(submitJarFile, 
          new FsPermission(TriggerSubmissionFiles.TRIGGER_FILE_PERMISSION));
    } 
  }

}
