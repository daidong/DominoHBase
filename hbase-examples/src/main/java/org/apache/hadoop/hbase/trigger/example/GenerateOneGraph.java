package org.apache.hadoop.hbase.trigger.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class GenerateOneGraph {

  HBaseAdmin admin;
	HTable webpage;
	HTable PageRankAcc;
	String pagePrefix = "pageid";
	long ts = 0L;

	public GenerateOneGraph() throws IOException{
		Configuration conf = HBaseConfiguration.create();
		admin = new HBaseAdmin(conf);
		if (admin.tableExists("wbpages")){
		  admin.disableTable("wbpages");
		  admin.deleteTable("wbpages");
		}
		if (admin.tableExists("PageRankAcc")){
		  admin.disableTable("PageRankAcc");
		  admin.deleteTable("PageRankAcc");
		}
		HTableDescriptor wb = new HTableDescriptor("wbpages");
		wb.addFamily(new HColumnDescriptor("prvalues"));
		wb.addFamily(new HColumnDescriptor("outlinks"));
    
		HTableDescriptor pr = new HTableDescriptor("PageRankAcc");
    pr.addFamily(new HColumnDescriptor("nodes"));
    
		admin.createTable(wb);
		admin.createTable(pr);
		webpage = new HTable(conf, "wbpages".getBytes());
		PageRankAcc = new HTable(conf, "PageRankAcc".getBytes());
  }

	public void createWb() throws IOException{
	  /* Page0 */
	  byte[] rowKey = (pagePrefix+String.valueOf(0)).getBytes();
	  Put p = new Put(rowKey);  
	  p.add("prvalues".getBytes(), "pr".getBytes(), ts, String.valueOf(1).getBytes());
    p.add("outlinks".getBytes(), (pagePrefix+String.valueOf(1)).getBytes(), ts, (pagePrefix+String.valueOf(1)).getBytes());
    p.add("outlinks".getBytes(), (pagePrefix+String.valueOf(2)).getBytes(), ts, (pagePrefix+String.valueOf(2)).getBytes());
    p.add("outlinks".getBytes(), (pagePrefix+String.valueOf(3)).getBytes(), ts, (pagePrefix+String.valueOf(3)).getBytes());
    webpage.put(p);
    webpage.flushCommits();
    
    /* Page1 */
    rowKey = (pagePrefix+String.valueOf(1)).getBytes();
    p = new Put(rowKey);  
    p.add("prvalues".getBytes(), "pr".getBytes(), ts, String.valueOf(1).getBytes());
    p.add("outlinks".getBytes(), (pagePrefix+String.valueOf(0)).getBytes(), ts, (pagePrefix+String.valueOf(0)).getBytes());
    p.add("outlinks".getBytes(), (pagePrefix+String.valueOf(2)).getBytes(), ts, (pagePrefix+String.valueOf(2)).getBytes());
    webpage.put(p);
    webpage.flushCommits();
    
    /* Page2 */
    rowKey = (pagePrefix+String.valueOf(2)).getBytes();
    p = new Put(rowKey);  
    p.add("prvalues".getBytes(), "pr".getBytes(), ts, String.valueOf(1).getBytes());
    p.add("outlinks".getBytes(), (pagePrefix+String.valueOf(3)).getBytes(), ts, (pagePrefix+String.valueOf(3)).getBytes());
    webpage.put(p);
    webpage.flushCommits();
    
    /* page3 */
    rowKey = (pagePrefix+String.valueOf(3)).getBytes();
    p = new Put(rowKey);  
    p.add("prvalues".getBytes(), "pr".getBytes(), ts, String.valueOf(1).getBytes());
    p.add("outlinks".getBytes(), (pagePrefix+String.valueOf(0)).getBytes(), ts, (pagePrefix+String.valueOf(0)).getBytes());
    p.add("outlinks".getBytes(), (pagePrefix+String.valueOf(1)).getBytes(), ts, (pagePrefix+String.valueOf(1)).getBytes());
    webpage.put(p);
    webpage.flushCommits();
    
    
	}
	
	public void createPR() throws IOException{
    /* Page0 */
    byte[] rowKey = (pagePrefix+String.valueOf(0)).getBytes();
    Put p = new Put(rowKey);  
    p.add("nodes".getBytes(), (pagePrefix+String.valueOf(1)).getBytes(), ts, (String.valueOf(0.5)).getBytes());
    p.add("nodes".getBytes(), (pagePrefix+String.valueOf(3)).getBytes(), ts, (String.valueOf(0.5)).getBytes());
    PageRankAcc.put(p);
    PageRankAcc.flushCommits();
    
    /* Page1 */
    rowKey = (pagePrefix+String.valueOf(1)).getBytes();
    p = new Put(rowKey);  
    p.add("nodes".getBytes(), (pagePrefix+String.valueOf(0)).getBytes(), ts, (String.valueOf(0.3333333334)).getBytes());
    p.add("nodes".getBytes(), (pagePrefix+String.valueOf(3)).getBytes(), ts, (String.valueOf(0.5)).getBytes());
    PageRankAcc.put(p);
    PageRankAcc.flushCommits();
    
    /* Page2 */
    rowKey = (pagePrefix+String.valueOf(2)).getBytes();
    p = new Put(rowKey);  
    p.add("nodes".getBytes(), (pagePrefix+String.valueOf(0)).getBytes(), ts, (String.valueOf(0.3333333334)).getBytes());
    p.add("nodes".getBytes(), (pagePrefix+String.valueOf(1)).getBytes(), ts, (String.valueOf(0.5)).getBytes());
    PageRankAcc.put(p);
    PageRankAcc.flushCommits();
    
    /* page3 */
    rowKey = (pagePrefix+String.valueOf(3)).getBytes();
    p = new Put(rowKey);  
    p.add("nodes".getBytes(), (pagePrefix+String.valueOf(0)).getBytes(), ts, (String.valueOf(0.333333334)).getBytes());
    p.add("nodes".getBytes(), (pagePrefix+String.valueOf(2)).getBytes(), ts, (String.valueOf(1)).getBytes());
    PageRankAcc.put(p);
    PageRankAcc.flushCommits();
    
    
  }
  
  public static void main(String[] args) throws IOException {
    GenerateOneGraph go = new GenerateOneGraph();
    go.createWb();
    go.createPR();
  }

}
