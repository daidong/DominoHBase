/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class PageRank {

  public static class PageRankDistMapper extends TableMapper<ImmutableBytesWritable, Text> {
    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException{
      byte[] prvalue = value.getValue("prvalues".getBytes(), "pr".getBytes());
      double d = Double.parseDouble(new String(prvalue));
      BigDecimal pr = new BigDecimal(new String(prvalue));
      
      Map<byte[], byte[]> alllinks = value.getFamilyMap("outlinks".getBytes());
      int k = alllinks.keySet().size();
      if (k == 0) k = 1;
      BigDecimal w = pr.divide(new BigDecimal(k), MathContext.DECIMAL128);
      for (byte[] page:alllinks.keySet()){
        context.write(new ImmutableBytesWritable(page), new Text(w.toPlainString()));
      }
    }
  }
  
  public static class PageRankReducer extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable>{
    public void reduce(ImmutableBytesWritable key, Iterable<Text> value, Context context) throws InterruptedException, IOException{
      BigDecimal sum = new BigDecimal(0);
      for (Text v : value){
        String plainString = new String(v.getBytes());
        BigDecimal t = new BigDecimal(plainString);
        sum = sum.add(t);
      }
      sum.multiply(new BigDecimal(0.85)).add(new BigDecimal(0.15));
      Put p = new Put(key.get());
      String newPr = sum.toPlainString();
      p.add("prvalues".getBytes(), "pr".getBytes(), newPr.getBytes());
      context.write(key, p);
    }
  }
  
  /**
   * @param args
   * @throws IOException 
   * @throws ClassNotFoundException 
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	long start = System.currentTimeMillis();

	for (int i = 0; i < 16; i++){
    Configuration config = HBaseConfiguration.create();
    Job job = new Job(config,"PageRankHBase"+i);
    job.setJarByClass(PageRank.class);    

    Scan scan = new Scan();
    scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
    scan.setCacheBlocks(false);  // don't set to true for MR jobs
    // set other scan attrs

    TableMapReduceUtil.initTableMapperJob(
      "wbpages",      // input table
      scan,           // Scan instance to control CF and attribute selection
      PageRankDistMapper.class,   // mapper class
      ImmutableBytesWritable.class,           // mapper output key
      Text.class,           // mapper output value
      job);
    
    TableMapReduceUtil.initTableReducerJob(
      "wbpages",      // output table
      PageRankReducer.class,             // reducer class
      job);

    boolean b = job.waitForCompletion(true);
    if (!b) {
        throw new IOException("error with job!");
    }
    }
    
    long end = System.currentTimeMillis();
    System.out.println("One Round Time: " + (end - start));
  }

}
