package org.apache.hadoop.hbase.trigger.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.trigger.HTriggerAction;
import org.apache.hadoop.hbase.trigger.HTriggerEvent;
import org.apache.hadoop.hbase.trigger.WritePrepared;
import org.apache.hadoop.hbase.trigger.WriteUnit;

public class PageRankDistAPI2 extends HTriggerAction{

  private static final Log LOG = LogFactory.getLog(PageRankDistAPI2.class);
  
  private HTable myTable;
  
  public PageRankDistAPI2(){
    try {
      Configuration conf = HBaseConfiguration.create();
      this.myTable = new HTable(conf, "wbpages".getBytes());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void NicePrint(String rowKey, String value, long round){
    System.out.print("PageRankDist===>");
    for (long i = 0; i < round - 1 ; i++){
      System.out.print("  ");
    }
    System.out.print("|--");
    System.out.println("("+rowKey + ":" + value+")");
  }

  @Override
  public void action(HTriggerEvent hte) {    
    NicePrint(new String(hte.getRowKey()), new String(hte.getNewValue()), this.getCurrentRound());
	  
    byte[] currentPageId = hte.getRowKey();
    byte[] values = hte.getNewValue();
    float fvalue = Float.parseFloat(new String(values));
    
    Get g = new Get(currentPageId);
    g.addFamily("outlinks".getBytes());
    
    try {
      Result r = myTable.get(g);
      NavigableMap<byte[],byte[]> outlinks = r.getFamilyMap("outlinks".getBytes());
      int n = 0;
      if (outlinks != null){
        n = outlinks.size();
      }
      float weight = 0;
      if (n != 0)
        weight = fvalue / n;
      String sweight = String.valueOf(weight);

      for (byte[] link: outlinks.values()){
        WriteUnit write = new WriteUnit(this, "PageRankAcc".getBytes(), 
                                        link, "nodes".getBytes(), currentPageId, sweight.getBytes(), true);
        WritePrepared.append(this, write);
      }
      
      WritePrepared.flush(this);
      
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public boolean filter(HTriggerEvent hte) {
    byte[] nvalue = hte.getNewValue();
    byte[] oldValue = hte.getOldValue();
    float fnv = Float.parseFloat(new String(nvalue));
    float fov = Float.parseFloat(new String(oldValue));
    if (Math.abs((fnv - fov)) < 0.001){
      System.out.println("Converge at " + new String(hte.getRowKey()));
      return false;
    }
    return true;
  }

}
