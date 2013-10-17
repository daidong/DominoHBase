package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

public class PageRankSumCoprocessor extends BaseRegionObserver{

	private HTable myTable = null;
	private HTable reTable;
	
	public PageRankSumCoprocessor(){
	    try {
	      Configuration conf = HBaseConfiguration.create();
	      this.myTable = new HTable(conf, "wbpages".getBytes());
	      this.reTable = new HTable(conf, "PageRankAcc".getBytes());
	    } catch (IOException e) {
	      e.printStackTrace();
	    }
	  }
	
	@Override
	public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
			Put put, org.apache.hadoop.hbase.regionserver.wal.WALEdit edit,
			boolean writeToWAL) throws IOException{

		System.out.println("Enter PageRankSumCoprocessor");
		byte[] pageId = put.getRow();
		Get g = new Get(pageId);
		g.addFamily("nodes".getBytes()).setMaxVersions(1);
		Result r = reTable.get(g);
		Map<byte[], byte[]> nodes = r.getFamilyMap("nodes".getBytes());
		long round = put.getTimeStamp();
		
		float sum = 0F;

		if (nodes != null){
			for (byte[] weight:nodes.values()){
				String sw = new String(weight);
				float fw = Float.parseFloat(sw);
				sum += fw;
			}
		}

		sum = 0.85F * sum + 0.15F;
		String ssum = String.valueOf(sum);

		Put p = new Put(pageId);
		p.add("prvalues".getBytes(), "pr".getBytes(), (round + 1), ssum.getBytes());

		myTable.put(p);
	}
	
}
