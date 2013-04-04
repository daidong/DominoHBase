package org.apache.hadoop.hbase.trigger;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * In our trigger system, we allow this options:
 * "libjars"  ->  will set "tmpjars" in triggerconf;
 * "files"    ->  will set "tmpfiles" in triggerconf;
 * "archives" ->  will set "tmparchives" in triggerconf;
 * 
 * @author daidong
 *
 */
public class TriggerRunner {

  public static int run(TriggerTool tool, String[] args) throws Exception{
    return run(new TriggerConf(), tool, args);
  }
  
  
  public static int run(Configuration conf, TriggerTool tool, String[] args) throws Exception{
    if (conf == null){
      conf = new TriggerConf();
    }
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    tool.setConf(conf);
    
    String[] toolArgs = parser.getRemainingArgs();
    return tool.run(toolArgs);
  }
  
}
