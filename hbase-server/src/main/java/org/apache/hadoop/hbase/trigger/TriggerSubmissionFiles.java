package org.apache.hadoop.hbase.trigger;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class TriggerSubmissionFiles {

  final public static FsPermission TRIGGER_DIR_PERMISSION = 
    FsPermission.createImmutable((short) 0700);
  final public static FsPermission TRIGGER_FILE_PERMISSION = 
    FsPermission.createImmutable((short) 0644);
  
  public static Path getStagingDir(TriggerClient triggerClient,
      TriggerConf triggerCopy) throws IOException {
    Path stagingArea = triggerClient.getStagingAreaDir();
    FileSystem fs = stagingArea.getFileSystem(triggerCopy);
    fs.mkdirs(stagingArea, new FsPermission(TRIGGER_DIR_PERMISSION));
    return stagingArea;
  }

  public static Path getJobConfPath(Path submitTriggerDir) {
    // TODO Auto-generated method stub
    return null;
  }

  public static Path getJobJar(Path submitTriggerDir) {
    // TODO Auto-generated method stub
    return null;
  }

}
