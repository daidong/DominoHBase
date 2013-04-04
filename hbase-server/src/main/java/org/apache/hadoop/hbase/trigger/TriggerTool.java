package org.apache.hadoop.hbase.trigger;

import org.apache.hadoop.conf.Configurable;

public interface TriggerTool extends Configurable{
  int run(String[] args) throws Exception;
}
