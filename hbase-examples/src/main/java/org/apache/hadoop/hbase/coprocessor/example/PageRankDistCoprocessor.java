package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NavigableMap;

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

public class PageRankDistCoprocessor extends BaseRegionObserver{

	private HTable myTable;
	private HTable reTable;
	private boolean stop = false;

	public PageRankDistCoprocessor(){
		try {
			Configuration conf = HBaseConfiguration.create();
			this.myTable = new HTable(conf, "wbpages".getBytes());
			this.reTable = new HTable(conf, "PageRankAcc".getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> c,
			Put put, org.apache.hadoop.hbase.regionserver.wal.WALEdit edit,
			boolean writeToWAL) throws IOException{
		byte[] currentPageId = put.getRow();
		Get g = new Get(currentPageId);
		g.addColumn("prvalues".getBytes(), "pr".getBytes()).setMaxVersions(1);
		Result r = myTable.get(g);
		KeyValue kv = r.getColumn("prvalues".getBytes(), "pr".getBytes()).get(0);
		byte[] oldValue = kv.getValue();
		
		ArrayList<KeyValue> values = (ArrayList<KeyValue>) edit.getKeyValues();
		byte[] nvalue = values.get(0).getValue();
		
		float fnv = Float.parseFloat(new String(nvalue));
	    float fov = Float.parseFloat(new String(oldValue));
	    
	    if (Math.abs((fnv - fov)) < 0.001){
	    	this.stop = true;
	    }
		
	}
	
	@Override
	public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c,
			Put put, org.apache.hadoop.hbase.regionserver.wal.WALEdit edit,
			boolean writeToWAL) throws IOException{
		
		
		System.out.println("enter PageRankDistCoprocessor");
		if (this.stop)
			return;
		
		byte[] currentPageId = put.getRow();
		ArrayList<KeyValue> values = (ArrayList<KeyValue>) edit.getKeyValues();
		byte[] value = values.get(0).getValue();
		float fvalue = Float.parseFloat(new String(value));
		long round = put.getTimeStamp();
		
		Get g = new Get(currentPageId);
		g.addFamily("outlinks".getBytes()).setMaxVersions(1);
		
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

		ArrayList<Put> ps = new ArrayList<Put>();
		for (byte[] link: outlinks.values()){
			Put p = new Put(link);
			p.add("nodes".getBytes(), currentPageId, (round + 1), sweight.getBytes());
			ps.add(p);
		}
		reTable.put(ps);
	}
	
}
