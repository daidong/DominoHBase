package org.apache.hadoop.hbase.trigger.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSTest {

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    String remoteFile = "hdfs://localhost:9000/hbase/tmp/trigger/staging/1/trigger.xml";
    String localFile = "/tmp/trigger/staging/1/trigger.xml";
    
    Configuration conf = new Configuration();
    conf.addResource(new Path("/Users/daidong/Documents/research/hadoop-1.1.2/conf/core-site.xml"));
    conf.addResource(new Path("/Users/daidong/Documents/research/hadoop-1.1.2/conf/hdfs-site.xml"));
    
    FileSystem fs = FileSystem.get(conf);
    
    /*
    FSDataInputStream in = fs.open(new Path(remoteFile));
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    String line = "";
    while ( (line = br.readLine()) != null)
      System.out.println(line);
    */
    
    fs.copyToLocalFile(new Path(remoteFile), new Path(localFile));
    
  }

}
