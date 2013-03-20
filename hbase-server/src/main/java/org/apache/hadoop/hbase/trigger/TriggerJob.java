package org.apache.hadoop.hbase.trigger;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

public class TriggerJob {

  public static enum TriggerState {DEFINE, RUNNING};
  private TriggerState state = TriggerState.DEFINE;
  private TriggerClient triggerClient;
  private RunningTrigger info;
  private int triggerId;
  private Configuration conf;
  
  public Configuration getConfiguration(){
    return this.conf;
  }
  private void connect() throws IOException{
      triggerClient = new TriggerClient((TriggerConf)getConfiguration());
  }
  
  public void submit() throws IOException, InterruptedException{
    //connect to the HMaster and submit the job
    connect();
    info = triggerClient.submitJobInternal((TriggerConf)getConfiguration());
    this.triggerId = info.getID();
    this.state = TriggerState.RUNNING;
    
  }
  public boolean waitForCompletion() throws IOException, InterruptedException{
    submit();
    info.waitForCompletion();
    return isSuccessful();
  }

  private boolean isSuccessful() {
    // TODO Auto-generated method stub
    return false;
  }
}
