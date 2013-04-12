package org.apache.hadoop.hbase.trigger.example;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.trigger.HTriggerAction;
import org.apache.hadoop.hbase.trigger.HTriggerEvent;

/**
 * PageRankDist
 * @author daidong
 * This trigger will monitor on table 'wbpages', column-family: 'prvalues', column: 'pr'
 * besides each web page also contains the information of its out links in column-family: 
 * 'outlinks', column:'links' and 'number'.
 * 
 * There is also an 'aggragate pattern' table named 'PageRankAcc', containing column-family: 'nodes' and 
 * column: 'node-id'. 
 * 
 * Basically this trigger finishes two things:
 * 1) monitor on wbpages-prvalues-pr
 * 2) write all page
 */

public class PageRankSum extends HTriggerAction{

  private HTable remoteTable;
  private HTable myTable;
  
  public PageRankSum(){
    try {
      Configuration conf = HBaseConfiguration.create();
      this.remoteTable = new HTable(conf, "wbpages".getBytes());
      this.myTable = new HTable(conf, "PageRankAcc".getBytes());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  @Override
  public void action(HTriggerEvent hte) {
    byte[] pageId = hte.getRowKey();
    System.out.println("PageRankSum proceses: " + new String(pageId));
    Get g = new Get(pageId);
    g.addFamily("nodes".getBytes());
    try {
      Result r = this.myTable.get(g);
      //System.out.println("PageRankSum get result on pageId: " + new String(pageId) + " size " + r.size());
      float sum = 0F;
      Map<byte[], byte[]> nodes  = r.getFamilyMap("nodes".getBytes());
      if (nodes != null){
        //System.out.println("PageRankSum get nodes not null on pageId: " + new String(pageId));
        for (byte[] weight:nodes.values()){
          String sw = new String(weight);
          float fw = Float.parseFloat(sw);
          sum += fw;
        }
      }
      Put p = new Put(pageId);
      String ssum = String.valueOf(sum);
      p.add("prvalues".getBytes(), "pr".getBytes(), ssum.getBytes());
      System.out.println("PageRankSum Start to write wbpages table: " + new String(pageId));
      this.remoteTable.put(p);
      System.out.println("PageRankSum End to  write wbpages table: " + new String(pageId));
      
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public boolean filter(HTriggerEvent hte) {
    return true;
  }

}
