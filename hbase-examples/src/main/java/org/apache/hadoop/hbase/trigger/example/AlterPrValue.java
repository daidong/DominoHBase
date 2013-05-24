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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public class AlterPrValue {

  
  HTable webpage;
  HTable PageRankAcc;
  int LARGEST_OUT_LINKS = 30;
  int PAGES_NUMBER = 10000;
  Random rand = null;
  
  String pagePrefix = "pageid";
    
  public AlterPrValue()  throws IOException{
    rand = new Random(System.currentTimeMillis());
    Configuration conf = HBaseConfiguration.create();
    webpage = new HTable(conf, "wbpages".getBytes());
  }
  
  
  public void alterWebGraph() throws IOException{
    long startTs = System.currentTimeMillis();
    for (long i = 0; i < PAGES_NUMBER; i++){
      //if (rand.nextDouble() < 1)
        alterPrValue(i);
    }
    webpage.close();    
    long endTs = System.currentTimeMillis();
    System.out.println("Start at: " + startTs + " end at: " + endTs + " lasts: " + (endTs - startTs));
  }
  
  public void alterPrValue(long pageId) throws IOException{
    byte[] rowKey = (pagePrefix+String.valueOf(pageId)).getBytes();
    Put p = new Put(rowKey);
    long ts = 0L;
    p.add("prvalues".getBytes(), "pr".getBytes(), ts, String.valueOf(1).getBytes());
    webpage.put(p);
    webpage.flushCommits();
  }
  
  public void testGetNotExistColumn() throws IOException{
    Get g = new Get("pageid10".getBytes());
    g.addColumn("outlinks".getBytes(), "_partial_result_".getBytes());
    Result r = webpage.get(g);
    if (r.isEmpty()){
      System.out.println("get NULL");
    } else {
      System.out.println(r);
    }
  }
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    AlterPrValue gen = new AlterPrValue();
    gen.testGetNotExistColumn();
    //gen.alterWebGraph();
  }

}
