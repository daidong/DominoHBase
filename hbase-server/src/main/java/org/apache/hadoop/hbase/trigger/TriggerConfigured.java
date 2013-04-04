package org.apache.hadoop.hbase.trigger;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public class TriggerConfigured implements Configurable{

  private TriggerConf conf;
  
  /** Construct a TriggerConfigured. */
  public TriggerConfigured() {
    this(null);
  }
  
  /** Construct a TriggerConfigured. */
  public TriggerConfigured(Configuration conf) {
    setConf(conf);
  }
  
  @Override
  public Configuration getConf() {
    // TODO Auto-generated method stub
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = (TriggerConf) conf;
  }

}
