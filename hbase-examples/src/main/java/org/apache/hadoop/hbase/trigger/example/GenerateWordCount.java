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
import org.apache.hadoop.hbase.util.Bytes;

public class GenerateWordCount {


	HBaseAdmin admin;
	HTable wbcontent;
	HTable wordcount;

	Random rand = null;

	public GenerateWordCount()  throws IOException{
		rand = new Random(System.currentTimeMillis());
		Configuration conf = HBaseConfiguration.create();
		admin = new HBaseAdmin(conf);
		if (admin.tableExists("wbcontent")){
			admin.disableTable("wbcontent");
			admin.deleteTable("wbcontent");
		}
		if (admin.tableExists("wordcount")){
			admin.disableTable("wordcount");
			admin.deleteTable("wordcount");
		}
		HTableDescriptor wb = new HTableDescriptor("wbcontent");
		wb.addFamily(new HColumnDescriptor("content"));

		HTableDescriptor pr = new HTableDescriptor("wordcount");
		pr.addFamily(new HColumnDescriptor("count"));

		admin.createTable(wb);
		admin.createTable(pr);
		wbcontent = new HTable(conf, "wbcontent".getBytes());
		wordcount = new HTable(conf, "wordcount".getBytes());
	}


	public void createPages(int rows) throws IOException{
		long ts = 0L;
		String content = "It has been more and more common for applications to obtain the ability of processing incremental inputs in cloud today. However, does not like programming with MapReduce or Dryad, it is still hard for developers to write such incremental applications under large-scale distributed environment. It is because the incremental algorithms are usually more complex, at the same time, developers are lack of efficient programming abstractions and runtime supports to program them. In this study, we proposed an incremental computing framework in cloud, namely Domino, which abstracts applications into a serial of triggers. Domino includes a rich set programming abstractions and runtime supports based on this trigger-based programming model. Besides, from a careful design, implementation and many novel optimizations, Domino is able to achieve much better performance comparing with current popular MapReduce framework and other incremental models under incremental scenario. Use cases and extensive evaluation results also confirm these advantages, which show a large range of applications can be implemented in Domino in a straightforward way, and their performance easily beats MapReduce for more than ten folders in the best case.";
    
		for (int i = 0; i < rows; i++){
		  byte[] rowkey = Bytes.toBytes(i);
		  Put p = new Put(rowkey);
		  p.add("content".getBytes(), "en".getBytes(), ts, content.getBytes());
		  wbcontent.put(p);
	    wbcontent.flushCommits();
		}
        
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		GenerateWordCount gen = new GenerateWordCount();
		int rows = 20000000;
		gen.createPages(rows);
	}

}
