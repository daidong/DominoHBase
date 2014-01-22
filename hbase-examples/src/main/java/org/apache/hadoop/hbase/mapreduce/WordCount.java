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

/**
 * There are two tables in WordCount MapReduce: 'wbcontent', 'wordcount'.
 * 'wbcontent' contains 'content:en' column
 * 'wordcount' contains 'count:number' column
 * 
 * @author daidong
 *
 */
public class WordCount {

  public static class WordCountMapper extends TableMapper<ImmutableBytesWritable, Text> {
    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException{
      byte[] content = value.getValue("content".getBytes(), "en".getBytes());
      String c = new String(content);
      String[] splitc = c.split(" ");
      for (String word:splitc){
        byte[] w = word.getBytes();
        String v = "1";
        context.write(new ImmutableBytesWritable(w), new Text(v));
      }
    }
  }
  
  public static class WordCountReducer extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable>{
    public void reduce(ImmutableBytesWritable key, Iterable<Text> value, Context context) throws InterruptedException, IOException{
      int w = 0;
      for (Text v : value){
        w++;
      }
      Put p = new Put(key.get());
      String newPr = String.valueOf(w);
      p.add("count".getBytes(), "number".getBytes(), newPr.getBytes());
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
    job.setJarByClass(WordCount.class);    

    Scan scan = new Scan();
    scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
    scan.setCacheBlocks(false);  // don't set to true for MR jobs
    // set other scan attrs

    TableMapReduceUtil.initTableMapperJob(
      "wbcontent",      // input table
      scan,           // Scan instance to control CF and attribute selection
      WordCountMapper.class,   // mapper class
      ImmutableBytesWritable.class,           // mapper output key
      Text.class,           // mapper output value
      job);
    
    TableMapReduceUtil.initTableReducerJob(
      "wordcount",      // output table
      WordCountReducer.class,             // reducer class
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
