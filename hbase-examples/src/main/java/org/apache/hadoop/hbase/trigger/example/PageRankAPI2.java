package org.apache.hadoop.hbase.trigger.example;

import org.apache.hadoop.hbase.trigger.Trigger;
import org.apache.hadoop.hbase.trigger.TriggerConf;
import org.apache.hadoop.hbase.trigger.TriggerConfigured;
import org.apache.hadoop.hbase.trigger.TriggerRunner;
import org.apache.hadoop.hbase.trigger.TriggerTool;

public class PageRankAPI2 extends TriggerConfigured implements TriggerTool {

  public int run(String[] args) throws Exception{
    TriggerConf tmp = (TriggerConf)this.getConf();
    
    Trigger tg1 = new Trigger(tmp, "PageRankDistAPI2", "wbpages" ,"prvalues", "pr" ,
        "org.apache.hadoop.hbase.trigger.example.PageRankDistAPI2", "INITIALWITHCONVERGE");
    tg1.submit();
    
    Trigger tg2 = new Trigger(tmp, "PageRankSumAPI2", "PageRankAcc", "nodes", "*", 
        "org.apache.hadoop.hbase.trigger.example.PageRankSumAPI2");
    tg2.submit();
   
    return 0;
  }

  public static void main(String[] args) throws Exception {
    TriggerRunner.run(new TriggerConf(), new PageRankAPI2(), args);
  }
}
