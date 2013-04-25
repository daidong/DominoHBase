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
package org.apache.hadoop.hbase.trigger.example;

import org.apache.hadoop.hbase.trigger.Trigger;
import org.apache.hadoop.hbase.trigger.TriggerConf;
import org.apache.hadoop.hbase.trigger.TriggerConfigured;
import org.apache.hadoop.hbase.trigger.TriggerRunner;
import org.apache.hadoop.hbase.trigger.TriggerTool;

public class PageRank extends TriggerConfigured implements TriggerTool {

  public int run(String[] args) throws Exception{
    TriggerConf tmp = (TriggerConf)this.getConf();
    System.out.println(tmp.getActionClassName());
    
    Trigger tg1 = new Trigger(tmp, "PageRankDist");
    tg1.setTriggerOnTable("wbpages");
    tg1.setTriggerOnColumFamily("prvalues");
    tg1.setTriggerOnColumn("pr");
    tg1.setActionClassName("org.apache.hadoop.hbase.trigger.example.PageRankDist");
    tg1.submit();
    
    Trigger tg2 = new Trigger(tmp, "PageRankSum");
    tg2.setTriggerOnTable("PageRankAcc");
    tg2.setTriggerOnColumFamily("nodes");
    tg2.setTriggerOnColumn("*");
    tg2.setActionClassName("org.apache.hadoop.hbase.trigger.example.PageRankSum");
    tg2.submit();
   
    return 0;
  }

  public static void main(String[] args) throws Exception {
    TriggerRunner.run(new TriggerConf(), new PageRank(), args);
  }
}
