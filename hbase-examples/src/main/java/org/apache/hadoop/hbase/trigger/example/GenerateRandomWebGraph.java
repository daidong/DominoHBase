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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class GenerateRandomWebGraph {


	HBaseAdmin admin;
	HTable webpage;
	HTable PageRankAcc;

	int LARGEST_OUT_LINKS = 2;
	long LARGEST_PAGE_ID = 3;
	int PAGES_NUMBER = 3;

	ArrayList<Long> allPages = new ArrayList<Long>();
	ArrayList<Long> waitForCreate = new ArrayList<Long>();

	String pagePrefix = "pageid";
	Random rand = null;

	public GenerateRandomWebGraph()  throws IOException{
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
		waitForCreate.add(Math.abs(rand.nextLong()) % LARGEST_PAGE_ID);
		while (!waitForCreate.isEmpty()){
			long id = waitForCreate.get(0);
			waitForCreate.remove(0);
			//waitForCreate.remove(id);
			createPage(id);
		}
		webpage.close();    
	}


	public void createPage(long pageId) throws IOException{
		if (allPages.contains(pageId))
			return;
		allPages.add(pageId);

		int outlinks = rand.nextInt(LARGEST_OUT_LINKS)+1;

		ArrayList<Long> ols = new ArrayList<Long>();

		if (allPages.size() > PAGES_NUMBER){
			while (ols.size() < outlinks){
				int randomIndex = rand.nextInt(allPages.size());
				long reverseLink = allPages.get(randomIndex);
				ols.add(reverseLink);
			}
		} else {
			while (ols.size() < outlinks){
				long outlink = Math.abs(rand.nextLong())%LARGEST_PAGE_ID;
				ols.add(outlink);
				if (!allPages.contains(outlink))
					waitForCreate.add(outlink);
			}
		}

		long ts = 0L;
		String content = "Hello, World!";
        for (int ki = 0; ki < 100; ki++)
            content = content + " " + content;
        
		byte[] rowKey = (pagePrefix+String.valueOf(pageId)).getBytes();
		Put p = new Put(rowKey);
		p.add("prvalues".getBytes(), "pr".getBytes(), ts, String.valueOf(0.5).getBytes());
		p.add("contents".getBytes(), "en".getBytes(), ts, content.getBytes());
		
		for (long link:ols){
			String vs = pagePrefix+String.valueOf(link);
			p.add("outlinks".getBytes(), vs.getBytes(), ts, vs.getBytes());
		}    
		webpage.put(p);
		webpage.flushCommits();
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		GenerateRandomWebGraph gen = new GenerateRandomWebGraph();
		gen.createWebGraph();
	}

}
