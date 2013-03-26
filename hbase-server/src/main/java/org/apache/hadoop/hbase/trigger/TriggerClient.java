package org.apache.hadoop.hbase.trigger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ClientProtocol;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RSTriggerRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RSTriggerResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.SubmitTriggerRequest;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;

import com.google.protobuf.ServiceException;

public class TriggerClient {

  private TriggerConf conf;
  private HConnection connection;
  private Path stagingAreaDir = null;
  
  public TriggerClient(){
  }
  
  public TriggerClient(TriggerConf conf) throws IOException{
    this.conf = conf;
    init();
  }
  
  public void init() throws ZooKeeperConnectionException{
    this.connection = HConnectionManager.getConnection(conf);
  }
  
  public int submitJobInternal(final TriggerConf trigger) throws Exception {
    TriggerConf triggerCopy = trigger;
    Path triggerStagingArea = TriggerSubmissionFiles.getHDFSStagingDir();
    int triggerId = this.connection.getNewTriggerId(true);
    
    Path submitTriggerDir = new Path(triggerStagingArea, String.valueOf(triggerId));
    //System.out.println("in submitJobInternal: submitTriggerDir: " + submitTriggerDir.toString());
    
    triggerCopy.set("trigger.dir", submitTriggerDir.toString());
    FileSystem fs = null;
    TriggerStatus status = null;
    try{
      TriggerSubmissionFiles.copyAndConfigureFiles(triggerCopy, submitTriggerDir);
      Path submitTriggerFile = TriggerSubmissionFiles.getJobConfPath(submitTriggerDir);
      //System.out.println("in submitJobInternal: submitTriggerFile: " + submitTriggerFile.toString());
      
      /**
       * Write trigger configuration file into HDFS /trigger/id/
       */
      fs = submitTriggerDir.getFileSystem(triggerCopy);
      FSDataOutputStream out = FileSystem.create(fs, submitTriggerFile, new FsPermission(TriggerSubmissionFiles.TRIGGER_FILE_PERMISSION));
      triggerCopy.writeXml(out);
      out.close();
      
      /**
       * really submit this trigger. @TODO, should remove submitTriggerDir.toString() and triggerCopy
       * as they can be rebuilt by reading /trigger/id/jobconf.xml
       */
      this.connection.submitTrigger(triggerId);
      
      /**
       * really submit the trigger to all the relevant region servers
       */
      this.connection.submitTriggerToRS(conf.getTriggerOnTable(), triggerId);
      
    } finally {
      /*
      if (status == null){
        if (fs != null && submitTriggerDir != null)
          fs.delete(submitTriggerDir, true);
      }
      */
    }
    return triggerId;
  }
}
