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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
* There are two tables in WordCount MapReduce: 'items', 'cluster'.
* 'items' contains 'vec:value(K1:K2:..)' column
* 'central' contains 'central:k1-k50' column
* 
* @author daidong
*
*/
public class KMeans {

 public static class KMeansMapper extends TableMapper<ImmutableBytesWritable, Text> {   
   public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException{
     byte[] vector = value.getValue("vec".getBytes(), "value".getBytes());
     String[] v = Bytes.toString(vector).split(":");
     
     Configuration conf = HBaseConfiguration.create();
     HTable centrals = new HTable(conf, "central".getBytes());
     double least = Double.MAX_VALUE;
     int belongto = -1;
     
     for (int i = 0; i < 120; i++){
       Get g = new Get(Bytes.toBytes(i));
       Result t = centrals.get(g);
       byte[] r = t.getValue("central".getBytes(), "value".getBytes());
       String[] rs = Bytes.toString(r).split(":");
       
       double distance = 0.0;
       for (int j = 0; j < v.length; j++){
         double v1 = Double.parseDouble(v[j]);
         double v2 = Double.parseDouble(rs[j]);
         distance += Math.abs(v1 - v2); 
       }
       
       if (distance < least){
         least = distance;
         belongto = i;
       }
     }
     
     byte[] clustering = Bytes.toBytes(belongto);
     context.write(new ImmutableBytesWritable(clustering), new Text(vector));
     
   }
 }
 
 public static class KMeansReducer extends TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable>{
   public void reduce(ImmutableBytesWritable key, Iterable<Text> value, Context context) throws InterruptedException, IOException{
     double[] avg = new double[50];
     for (int i = 0; i < 50; i++){
       avg[i] = 0.0;
     }
     int num = 0;
     for (Text v : value){
       num ++;
       byte[] vec = v.getBytes();
       int i = 0;
       for (String vi : Bytes.toString(vec).split(":")){
         avg[i++] += Double.parseDouble(vi);
       }
     }
     for (int i = 0; i < 50; i++){
       avg[i] = avg[i]/num;
     }
     
     String central = "";
     for (int i = 0; i < 49; i++){
       central = central + avg[i] + ":";
     }
     central = central + avg[49];
     
     Put p = new Put(key.get());
     
     p.add("central".getBytes(), "value".getBytes(), central.getBytes());
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

 for (int i = 0; i < 1; i++){
   Configuration config = HBaseConfiguration.create();
   Job job = new Job(config,"KMeans"+i);
   job.setJarByClass(WordCount.class);    

   Scan scan = new Scan();
   scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
   scan.setCacheBlocks(false);  // don't set to true for MR jobs
   // set other scan attrs

   TableMapReduceUtil.initTableMapperJob(
     "items",      // input table
     scan,           // Scan instance to control CF and attribute selection
     KMeansMapper.class,   // mapper class
     ImmutableBytesWritable.class,           // mapper output key
     Text.class,           // mapper output value
     job);
   
   TableMapReduceUtil.initTableReducerJob(
     "cluster",      // output table
     KMeansReducer.class,             // reducer class
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
