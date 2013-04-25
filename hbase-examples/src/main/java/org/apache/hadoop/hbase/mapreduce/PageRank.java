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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.SampleUploader.Uploader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class PageRank {

  public static class PageRankDistMapper extends TableMapper<ImmutableBytesWritable, Text> {
    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException{
      byte[] prvalue = value.getValue("prvalues".getBytes(), "pr".getBytes());
      double d = Double.parseDouble(new String(prvalue));
      Map<byte[], byte[]> alllinks = value.getFamilyMap("outlinks".getBytes());
      int k = alllinks.keySet().size();
      if (k == 0) k = 1;
      double weight = d / k;
      for (byte[] page:alllinks.keySet()){
        context.write(new ImmutableBytesWritable(page), new Text(String.valueOf(weight)));
      }
    }
  }
  
  public static class PageRankReducer extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable>{
    public void reduce(ImmutableBytesWritable key, Iterable<Text> value, Context context) throws InterruptedException, IOException{
      double sum = 0;
      for (Text v : value){
        double t = Double.parseDouble(new String(v.getBytes()));
        sum += t;
      }
      Put p = new Put(key.get());
      String newPr = String.valueOf(sum);
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
    Configuration config = HBaseConfiguration.create();
    Job job = new Job(config,"PageRankHBase");
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

}
