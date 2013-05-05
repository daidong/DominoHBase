package org.apache.hadoop.hbase.trigger.example;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.trigger.AccHTriggerAction;
import org.apache.hadoop.hbase.trigger.HTriggerEvent;
import org.apache.hadoop.hbase.trigger.WritePrepared;
import org.apache.hadoop.hbase.trigger.WriteUnit;

public class PageRankSumAPI2 extends AccHTriggerAction{

  private static final Log LOG = LogFactory.getLog(PageRankSumAPI2.class);
  @Override
  public void action(HTriggerEvent hte) {

    //LOG.info("Current Action Round is: " + this.getCurrentRound());
    
    Result r = this.getReader().GetValues();
    byte[] pageId = hte.getRowKey();

    float sum = 0F;
    Map<byte[], byte[]> nodes  = r.getFamilyMap("nodes".getBytes());
    if (nodes != null){
      for (byte[] weight:nodes.values()){
        String sw = new String(weight);
        float fw = Float.parseFloat(sw);
        sum += fw;
      }
    }
    
    String ssum = String.valueOf(sum);
    WriteUnit write = new WriteUnit(this, "wbpages".getBytes(), pageId, "prvalues".getBytes(), "pr".getBytes(), ssum.getBytes());
    LOG.info("Append Write Unit:" + write);
    WritePrepared.append(this, write);
    WritePrepared.flush(this);
  }

  @Override
  public boolean filter(HTriggerEvent hte) {
    return true;
  }

}
