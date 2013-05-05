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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class GenerateFullWebGraph {

  
  HTable webpage;
  HTable PageRankAcc;
  int LARGEST_OUT_LINKS = 30;
  int PAGES_NUMBER = 10000;
  Random rand = null;
  
  String pagePrefix = "pageid";
  
  HashMap<Long, ArrayList<Long>> reverseMap = new HashMap<Long, ArrayList<Long>>();
    
  public GenerateFullWebGraph()  throws IOException{
    rand = new Random(System.currentTimeMillis());
    Configuration conf = HBaseConfiguration.create();
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
          
    int outlinks = rand.nextInt(LARGEST_OUT_LINKS) + 5;
    
    ArrayList<Long> ols = new ArrayList<Long>();

    for (int i = 0; i < outlinks; i++){
      long outlink = Math.abs(rand.nextLong())%PAGES_NUMBER;
      if (outlink == pageId) continue;
      ols.add(outlink);
    }
    
    long ts = 0L;          
    byte[] rowKey = (pagePrefix+String.valueOf(pageId)).getBytes();
    Put p = new Put(rowKey);   
    ArrayList<Put> AccPuts = new ArrayList<Put>();
    
    p.add("prvalues".getBytes(), "pr".getBytes(), ts, String.valueOf(1).getBytes());
    for (long link:ols){
      String vs = pagePrefix+String.valueOf(link);
      p.add("outlinks".getBytes(), vs.getBytes(), ts, vs.getBytes());
      
      double avg = 1.0 / (double) outlinks;
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
