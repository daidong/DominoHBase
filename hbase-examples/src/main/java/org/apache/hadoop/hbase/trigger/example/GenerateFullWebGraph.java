/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

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

public class GenerateFullWebGraph {

	HBaseAdmin admin;
	HTable webpage;
	HTable PageRankAcc;
	int LARGEST_OUT_LINKS = 80;
	int PAGES_NUMBER = 1000;
	Random rand = null;

	String pagePrefix = "pageid";

	//HashMap<Long, ArrayList<Long>> reverseMap = new HashMap<Long, ArrayList<Long>>();

	public GenerateFullWebGraph()  throws IOException{
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
	    //wb.setMemStoreFlushSize(memstoreFlushSize);
	    wb.addFamily(new HColumnDescriptor("prvalues").setInMemory(true));
	    wb.addFamily(new HColumnDescriptor("outlinks").setInMemory(true));
	    wb.addFamily(new HColumnDescriptor("contents"));
	    
	    HTableDescriptor pr = new HTableDescriptor("PageRankAcc");
	    pr.addFamily(new HColumnDescriptor("nodes").setInMemory(true));
	    
	    admin.createTable(wb);
	    admin.createTable(pr);
	    webpage = new HTable(conf, "wbpages".getBytes());
	    PageRankAcc = new HTable(conf, "PageRankAcc".getBytes());
	}


	public void createWebGraph() throws IOException{
		for (long i = 0; i < PAGES_NUMBER; i++){
			createPage(i);
		}
		webpage.close();
		PageRankAcc.close();
	}

	public void formPageRankAcc(long pageId) throws IOException{

	}
	public void createPage(long pageId) throws IOException{

		int outlinks = rand.nextInt(LARGEST_OUT_LINKS) + 1;

		ArrayList<Long> ols = new ArrayList<Long>();

		while (ols.size() < outlinks){
			long outlink = Math.abs(rand.nextLong())%PAGES_NUMBER;
			if (outlink == pageId) {
				continue;
			}
			ols.add(outlink);
		}

		long ts = 0L;          
		String content = "Hello, World!";
        for (int ki = 0; ki < 10; ki++)
            content = content + " " + content;
        
		byte[] rowKey = (pagePrefix+String.valueOf(pageId)).getBytes();
		Put p = new Put(rowKey);   
		ArrayList<Put> AccPuts = new ArrayList<Put>();
		p.add("prvalues".getBytes(), "pr".getBytes(), ts, String.valueOf(1).getBytes());
		p.add("contents".getBytes(), "en".getBytes(), ts, content.getBytes());
		
		for (long link:ols){
			String vs = pagePrefix+String.valueOf(link);
			p.add("outlinks".getBytes(), vs.getBytes(), ts, vs.getBytes());

			double avg = 1.0 / (double) (outlinks);
			Put AccPut = new Put(vs.getBytes());
			AccPut.add("nodes".getBytes(), rowKey, ts, String.valueOf(avg).getBytes());
			AccPuts.add(AccPut);
		}    
		
		webpage.put(p);
		webpage.flushCommits();
		PageRankAcc.put(AccPuts);
		PageRankAcc.flushCommits();
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		GenerateFullWebGraph gen = new GenerateFullWebGraph();
		gen.createWebGraph();
	}

}
