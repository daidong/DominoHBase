package org.apache.hadoop.hbase.trigger.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class GenerateNIncomingGraph {

  	HBaseAdmin admin;
	HTable webpage;
	HTable PageRankAcc;
	int LINK_IN_NUMBER = 10;
	int PAGES_NUMBER = 40;
	Random rand = null;

	String pagePrefix = "pageid";

	HashMap<Long, ArrayList<Long>> reverseMap = new HashMap<Long, ArrayList<Long>>();

	public GenerateNIncomingGraph()  throws IOException{
		rand = new Random(System.currentTimeMillis());
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
    wb.addFamily(new HColumnDescriptor("contents"));
    
    HTableDescriptor pr = new HTableDescriptor("PageRankAcc");
    pr.addFamily(new HColumnDescriptor("nodes"));
    
    admin.createTable(wb);
    admin.createTable(pr);
		webpage = new HTable(conf, "wbpages".getBytes());
		PageRankAcc = new HTable(conf, "PageRankAcc".getBytes());
	}

	public void createWebGraph() throws IOException{

		HashMap<Long, ArrayList<Long>> webs = new HashMap<Long, ArrayList<Long>>(PAGES_NUMBER);
		
		for (long i = 0; i < PAGES_NUMBER; i++){
			
			ArrayList<Long> ins = new ArrayList<Long>();
			int j = 0; 
			
			while (j < LINK_IN_NUMBER){
				long inlink = Math.abs(rand.nextLong())%PAGES_NUMBER;
				if (inlink == i) {
					continue;
				}
				j++;
				ArrayList<Long> outs = null;
				if (!webs.containsKey(inlink)){
					outs = new ArrayList<Long>();
					outs.add(i); 
					webs.put(inlink, outs);
				} else {
					outs = webs.get(inlink);
					outs.add(i);
				}
			}	
					
		}
		
		long ts = 0L;
		String content = "Hello, World!";
        for (int ki = 0; ki < 100; ki++)
            content = content + " " + content;
		
		for (long i = 0; i < PAGES_NUMBER; i++){
			byte[] rowKey = (pagePrefix+String.valueOf(i)).getBytes();
			Put p = new Put(rowKey);  
			p.add("prvalues".getBytes(), "pr".getBytes(), ts, String.valueOf(1).getBytes());
			p.add("contents".getBytes(), "en".getBytes(), ts, content.getBytes());

			ArrayList<Put> AccPuts = new ArrayList<Put>();
			
			for (Long link:webs.get(i)){
				String vs = pagePrefix+String.valueOf(link);
				p.add("outlinks".getBytes(), vs.getBytes(), ts, vs.getBytes());
				
				double avg = 1.0 / (double) (webs.get(i).size());
				byte[] row = (pagePrefix+String.valueOf(link)).getBytes();
				Put AccPut = new Put(row);
				AccPut.add("nodes".getBytes(), rowKey, ts, String.valueOf(avg).getBytes());
				AccPuts.add(AccPut);
				
			}
			
			webpage.put(p);
		    webpage.flushCommits();
		    PageRankAcc.put(AccPuts);
		    PageRankAcc.flushCommits();   
			
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		GenerateNIncomingGraph gen = new GenerateNIncomingGraph();
		gen.createWebGraph();
	}

}
