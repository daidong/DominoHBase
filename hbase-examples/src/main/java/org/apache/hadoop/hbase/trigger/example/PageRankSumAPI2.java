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
  public void NicePrint(String rowKey, String value, long round){
    System.out.print("PageRankSum ===>");
    for (long i = 0; i < round - 1 ; i++){
      System.out.print("  ");
    }
    System.out.print("|--");
    System.out.println("("+rowKey + ":" + value+")");
  }



  @Override
  public void action(HTriggerEvent hte) {

    NicePrint(new String(hte.getRowKey()), new String(hte.getNewValue()), this.getCurrentRound());
    
    String loginfo = "";
    
    //LOG.info("Inside PageRankSumAPI Action");
    Map<byte[], byte[]> nodes = this.getReader().GetMapValues();
    byte[] pageId = hte.getRowKey();
    loginfo = loginfo + "Sum: " + new String(pageId);
    float sum = 0F;
    if (nodes != null){
      for (byte[] weight:nodes.values()){
        String sw = new String(weight);
        loginfo += (sw + ":");
        float fw = Float.parseFloat(sw);
        sum += fw;
      }
    }
    //LOG.info(loginfo);
    sum = 0.85F * sum + 0.15F;
    String ssum = String.valueOf(sum);
    WriteUnit write = new WriteUnit(this, "wbpages".getBytes(), pageId, "prvalues".getBytes(), "pr".getBytes(), ssum.getBytes());
    WritePrepared.append(this, write);
    WritePrepared.flush(this);
  }

  @Override
  public boolean filter(HTriggerEvent hte) {
    LOG.info("Inside PageRankSumAPI Filter");
    return true;
  }

}
