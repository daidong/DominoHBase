package org.apache.hadoop.hbase.trigger.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;

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
 * column: 'weight'. 
 * 
 * Basically this trigger finishes two things:
 * 1) monitor on wbpages-prvalues-pr
 * 2) write all page
 */

public class PageRankDist extends HTriggerAction{

  private HTable remoteTable;
  private HTable myTable;
  
  public PageRankDist(){
    byte[] tableName = "PageRankAcc".getBytes();
    try {
      Configuration conf = HBaseConfiguration.create();
      this.remoteTable = new HTable(conf, tableName);
      this.myTable = new HTable(conf, "wbpages".getBytes());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void action(HTriggerEvent hte) {
    byte[] currentPageId = hte.getRowKey();
    System.out.println("PageRankDist processes: " + new String(currentPageId));
    byte[] values = hte.getNewValue();
    String svalue = new String(values);
    float fvalue = Float.parseFloat(svalue);
    
    Get g = new Get(currentPageId);
    g.addFamily("outlinks".getBytes());
    ArrayList<Put> puts = new ArrayList<Put>();
    
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
        Put p = new Put(link);
        p.add("nodes".getBytes(), currentPageId, sweight.getBytes());
        puts.add(p);
      }
      
      this.remoteTable.put(puts);
      
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
    System.out.println("Inside PageRankDist: " + fnv + " : " + fov);
    if (Math.abs((fnv - fov)) < 0.001){
      return false;
    }
    return true;
  }
}
