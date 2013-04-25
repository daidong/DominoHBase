/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
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
