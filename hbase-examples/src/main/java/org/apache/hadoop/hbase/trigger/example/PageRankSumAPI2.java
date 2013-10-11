package org.apache.hadoop.hbase.trigger.example;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.trigger.AccHTriggerAction;
import org.apache.hadoop.hbase.trigger.HTriggerEvent;
import org.apache.hadoop.hbase.trigger.PartialResult;
import org.apache.hadoop.hbase.trigger.WritePrepared;
import org.apache.hadoop.hbase.trigger.WriteUnit;

public class PageRankSumAPI2 extends AccHTriggerAction{

  private static final Log LOG = LogFactory.getLog(PageRankSumAPI2.class);
  
  /*
  public void incr(HTriggerEvent hte, PartialResult r){
    byte[] oldSum = r.getValue();
    float dOldSum = Float.parseFloat(new String(oldSum));
    float newvalue = Float.parseFloat(new String(hte.getNewValue()));
    float oldvalue = Float.parseFloat(new String(hte.getOldValue()));
    
    float newSum = dOldSum + (newvalue - oldvalue);
    String snewSum = String.valueOf(newSum);

    //System.out.println("PageRankSumAPI2...get newSum: " + snewSum + " from oldSum: " + dOldSum + "" + " by compare new: " + newvalue + " with old: " + oldvalue);
    
    WriteUnit write = new WriteUnit(this, "wbpages".getBytes(), hte.getRowKey(), 
        "prvalues".getBytes(), "pr".getBytes(), snewSum.getBytes());
    
    WritePrepared.append(this, write);
    WritePrepared.flush(this);
    
  }
  */
  
  @Override
  public void action(HTriggerEvent hte) {

    //LOG.info("Current Action Round is: " + this.getCurrentRound());

    Result r = this.getReader().GetValues();
    byte[] pageId = hte.getRowKey();

    //LOG.info("Action on " + new String(pageId) + " with value: " + new String(hte.getNewValue()) + " at " + this.getCurrentRound());
    
    float sum = 0F;
    Map<byte[], byte[]> nodes  = r.getFamilyMap("nodes".getBytes());
    if (nodes != null){
      for (byte[] weight:nodes.values()){
        String sw = new String(weight);
        float fw = Float.parseFloat(sw);
        sum += fw;
        //LOG.info("In key: " + hte.getEventTriggerKey() + " we accumulate for round " + this.getCurrentRound());
      }
    }
    
    System.out.println("PageRankSum ========>" + "Current on " + new String(hte.getRowKey()) + 
        " with value " + new String(hte.getNewValue()) + " at " + this.getCurrentRound());  
      
    sum = 0.85F * sum + 0.15F;
    String ssum = String.valueOf(sum);
    WriteUnit write = new WriteUnit(this, "wbpages".getBytes(), pageId, "prvalues".getBytes(), "pr".getBytes(), ssum.getBytes());
    //LOG.info("Append Write Unit:" + write);
    WritePrepared.append(this, write);
    WritePrepared.flush(this);
  }

  @Override
  public boolean filter(HTriggerEvent hte) {
    return true;
  }

}
