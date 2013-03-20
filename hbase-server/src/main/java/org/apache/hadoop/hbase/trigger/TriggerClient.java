package org.apache.hadoop.hbase.trigger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;

public class TriggerClient {

  private Configuration conf;
  private HConnection connection;
  private Path stagingAreaDir = null;
  
  public TriggerClient(){
    
  }
  
  public void setConf(Configuration conf){
    this.conf = conf;
  }
  public TriggerClient(Configuration conf) throws ZooKeeperConnectionException{
    setConf(conf);
    init(conf);
  }
  public void init(Configuration conf) throws ZooKeeperConnectionException{
    this.connection = HConnectionManager.getConnection(conf);
  }
  
  public RunningTrigger submitJobInternal(final TriggerConf trigger) throws IOException, InterruptedException {
    TriggerConf triggerCopy = trigger;
    Path triggerStagingArea = TriggerSubmissionFiles.getStagingDir(TriggerClient.this, triggerCopy);
    int triggerId = this.connection.getNewTriggerId();
    Path submitTriggerDir = new Path(triggerStagingArea, String.valueOf(triggerId));
    triggerCopy.set("domino.trigger.dir", submitTriggerDir.toString());
    FileSystem fs = null;
    TriggerStatus status = null;
    try{
      copyAndConfigureFiles(triggerCopy, submitTriggerDir);
      Path submitTriggerFile = TriggerSubmissionFiles.getJobConfPath(submitTriggerDir);
      
      fs = submitTriggerDir.getFileSystem(triggerCopy);
      FSDataOutputStream out = FileSystem.create(fs, submitTriggerFile, new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
      triggerCopy.write(out);
      status = this.connection.submitTrigger(triggerId, submitTriggerDir.toString(), triggerCopy);
      if (status != null)
        return new RunningTrigger(status);
    } finally {
      if (status == null){
        if (fs != null && submitTriggerDir != null)
          fs.delete(submitTriggerDir, true);
      }
    }
    return null;
  }

  private void copyAndConfigureFiles(TriggerConf triggerCopy,
      Path submitTriggerDir) throws IOException, InterruptedException {
    short replication = (short)triggerCopy.getInt("trigger.submit.replication", 10);
    String files = triggerCopy.get("tmpfiles");
    String libjars = triggerCopy.get("tmpjars");
    String archives = triggerCopy.get("tmparchieves");
    
    FileSystem fs = submitTriggerDir.getFileSystem(triggerCopy);
    submitTriggerDir = fs.makeQualified(submitTriggerDir);
    FsPermission triggerSysPerms = new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    FileSystem.mkdirs(fs, submitTriggerDir, triggerSysPerms);
    Path filesDir = JobSubmissionFiles.getJobDistCacheFiles(submitTriggerDir);
    Path archivesDir = JobSubmissionFiles.getJobDistCacheArchives(submitTriggerDir);
    Path libjarsDir = JobSubmissionFiles.getJobDistCacheLibjars(submitTriggerDir);
    // add all the command line files/ jars and archive
    // first copy them to jobtrackers filesystem 
    
    if (files != null) {
      FileSystem.mkdirs(fs, filesDir, triggerSysPerms);
      String[] fileArr = files.split(",");
      for (String tmpFile: fileArr) {
        URI tmpURI;
        try {
          tmpURI = new URI(tmpFile);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
        Path tmp = new Path(tmpURI);
        Path newPath = copyRemoteFiles(fs,filesDir, tmp, triggerCopy, replication);
        try {
          URI pathURI = getPathURI(newPath, tmpURI.getFragment());
        } catch(URISyntaxException ue) { 
          throw new IOException("Failed to create uri for " + tmpFile, ue);
        }
        //DistributedCache.createSymlink(job);
      }
    }
    
    if (libjars != null) {
      FileSystem.mkdirs(fs, libjarsDir, triggerSysPerms);
      String[] libjarsArr = libjars.split(",");
      for (String tmpjars: libjarsArr) {
        Path tmp = new Path(tmpjars);
        Path newPath = copyRemoteFiles(fs, libjarsDir, tmp, triggerCopy, replication);
        //DistributedCache.addArchiveToClassPath
        //  (new Path(newPath.toUri().getPath()), triggerCopy, fs);
      }
    }
    
    
    if (archives != null) {
     FileSystem.mkdirs(fs, archivesDir, triggerSysPerms); 
     String[] archivesArr = archives.split(",");
     for (String tmpArchives: archivesArr) {
       URI tmpURI;
       try {
         tmpURI = new URI(tmpArchives);
       } catch (URISyntaxException e) {
         throw new IllegalArgumentException(e);
       }
       Path tmp = new Path(tmpURI);
       Path newPath = copyRemoteFiles(fs, archivesDir, tmp, triggerCopy, replication);
       try {
         URI pathURI = getPathURI(newPath, tmpURI.getFragment());
         //DistributedCache.addCacheArchive(pathURI, job);
       } catch(URISyntaxException ue) {
         //should not throw an uri excpetion
         throw new IOException("Failed to create uri for " + tmpArchives, ue);
       }
       //DistributedCache.createSymlink(job);
     }
    }
    
    String originalJarPath = triggerCopy.getJar();

    if (originalJarPath != null) {           // copy jar to JobTracker's fs
      // use jar name if job is not named. 
      if ("".equals(triggerCopy.getJobName())){
        triggerCopy.setJobName(new Path(originalJarPath).getName());
      }
      Path submitJarFile = TriggerSubmissionFiles.getJobJar(submitTriggerDir);
      triggerCopy.setJar(submitJarFile.toString());
      fs.copyFromLocalFile(new Path(originalJarPath), submitJarFile);
      fs.setReplication(submitJarFile, replication);
      fs.setPermission(submitJarFile, 
          new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
    } 
  }

  private Path copyRemoteFiles(FileSystem jtFs, Path parentDir, 
      final Path originalPath, final TriggerConf trigger, short replication) 
  throws IOException, InterruptedException {
    
    FileSystem remoteFs = null;
    remoteFs = originalPath.getFileSystem(trigger);
   
    // this might have name collisions. copy will throw an exception
    //parse the original path to create new path
    Path newPath = new Path(parentDir, originalPath.getName());
    FileUtil.copy(remoteFs, originalPath, jtFs, newPath, false, trigger);
    jtFs.setReplication(newPath, replication);
    return newPath;
  }
  
  private URI getPathURI(Path destPath, String fragment)
      throws URISyntaxException {
    URI pathURI = destPath.toUri();
    if (pathURI.getFragment() == null) {
      if (fragment == null) {
        pathURI = new URI(pathURI.toString() + "#" + destPath.getName());
      } else {
        pathURI = new URI(pathURI.toString() + "#" + fragment);
      }
    }
    return pathURI;
  }

  public Path getStagingAreaDir() throws IOException {
    if (stagingAreaDir == null){
      stagingAreaDir = new Path(this.connection.getStagingAreaDir());
    }
    return stagingAreaDir;
  }
}
