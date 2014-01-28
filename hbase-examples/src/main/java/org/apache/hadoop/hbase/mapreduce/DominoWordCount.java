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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * There are two tables in WordCount MapReduce: 'wbcontent', 'wordcount'.
 * 'wbcontent' contains 'content:en' column
 * 'wordcount' contains 'count:number' column
 * 
 * @author daidong
 *
 */
public class DominoWordCount {

  public static class DominoWordCountMapper extends TableMapper<ImmutableBytesWritable, Text> {
    
    //private HTable wordcount;
    HashMap<String, Long> localFreq = null;

    @Override
    public void setup(Context context) {
      localFreq = new HashMap<String, Long>();
    }
    
    @Override
    public void cleanup(Context context){
      /*
      Configuration conf = HBaseConfiguration.create();
      HTable wordcount = null;
      try {
        wordcount = new HTable(conf, "wordcount".getBytes());
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      */
      for (String word:localFreq.keySet()){
        byte[] w = word.getBytes();
        try {
          Thread.sleep(1L);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        /*
        try {
          wordcount.incrementColumnValue(w, "count".getBytes(), "number".getBytes(), localFreq.get(word), false);
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } 
        */
      }
    }
    
    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException{
      byte[] content = value.getValue("content".getBytes(), "en".getBytes());
      String c = new String(content);
      String[] splitc = c.split(" ");
      for (String word:splitc){
        if (!localFreq.containsKey(word)){
          localFreq.put(word, 1L);
        } else {
          localFreq.put(word, localFreq.get(word)+1);
        }
        
      }
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

	for (int i = 0; i < 3; i++){
    Configuration config = HBaseConfiguration.create();
    Job job = new Job(config,"DominoWordCount"+i);
    job.setJarByClass(DominoWordCount.class);    

    Scan scan = new Scan();
    scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
    scan.setCacheBlocks(false);  // don't set to true for MR jobs
    // set other scan attrs

    TableMapReduceUtil.initTableMapperJob(
      "wbcontent",      // input table
      scan,           // Scan instance to control CF and attribute selection
      DominoWordCountMapper.class,   // mapper class
      null,           // mapper output key
      null,           // mapper output value
      job);
    
    //job.setOutputFormatClass(cls);
    job.setOutputFormatClass(NullOutputFormat.class);
    
    boolean b = job.waitForCompletion(true);
    if (!b) {
        throw new IOException("error with job!");
    }
    }
    
    long end = System.currentTimeMillis();
    System.out.println("One Round Time: " + (end - start));
  }

}
