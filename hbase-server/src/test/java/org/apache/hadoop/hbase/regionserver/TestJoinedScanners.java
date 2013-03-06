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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.Test;
import org.junit.experimental.categories.Category;



/**
 * Test performance improvement of joined scanners optimization:
 * https://issues.apache.org/jira/browse/HBASE-5416
 */
@Category(LargeTests.class)
public class TestJoinedScanners {
  static final Log LOG = LogFactory.getLog(TestJoinedScanners.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String DIR = TEST_UTIL.getDataTestDir("TestJoinedScanners").toString();

  private static final byte[] tableName = Bytes.toBytes("testTable");
  private static final byte[] cf_essential = Bytes.toBytes("essential");
  private static final byte[] cf_joined = Bytes.toBytes("joined");
  private static final byte[] col_name = Bytes.toBytes("a");
  private static final byte[] flag_yes = Bytes.toBytes("Y");
  private static final byte[] flag_no  = Bytes.toBytes("N");

  @Test
  public void testJoinedScanners() throws Exception {
    String dataNodeHosts[] = new String[] { "host1", "host2", "host3" };
    int regionServersCount = 3;

    HBaseTestingUtility htu = new HBaseTestingUtility();

    final int DEFAULT_BLOCK_SIZE = 1024*1024;
    htu.getConfiguration().setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    htu.getConfiguration().setInt("dfs.replication", 1);
    htu.getConfiguration().setLong("hbase.hregion.max.filesize", 322122547200L);
    MiniHBaseCluster cluster = null;

    try {
      cluster = htu.startMiniCluster(1, regionServersCount, dataNodeHosts);
      byte [][] families = {cf_essential, cf_joined};

      HTable ht = htu.createTable(
        Bytes.toBytes(this.getClass().getSimpleName()), families);

      long rows_to_insert = 10000;
      int insert_batch = 20;
      int flag_percent = 1;
      int large_bytes = 128 * 1024;
      long time = System.nanoTime();

      LOG.info("Make " + Long.toString(rows_to_insert) + " rows, total size = "
        + Float.toString(rows_to_insert * large_bytes / 1024 / 1024) + " MB");

      byte [] val_large = new byte[large_bytes];

      List<Put> puts = new ArrayList<Put>();

      for (long i = 0; i < rows_to_insert; i++) {
        Put put = new Put(Bytes.toBytes(Long.toString (i)));
        if (i % 100 <= flag_percent) {
          put.add(cf_essential, col_name, flag_yes);
        }
        else {
          put.add(cf_essential, col_name, flag_no);
        }
        put.add(cf_joined, col_name, val_large);
        puts.add(put);
        if (puts.size() >= insert_batch) {
          ht.put(puts);
          puts.clear();
        }
      }
      if (puts.size() >= 0) {
        ht.put(puts);
        puts.clear();
      }

      LOG.info("Data generated in "
        + Double.toString((System.nanoTime() - time) / 1000000000.0) + " seconds");

      boolean slow = true;
      for (int i = 0; i < 20; ++i) {
        runScanner(ht, slow);
        slow = !slow;
      }

      ht.close();
    } finally {
      if (cluster != null) {
        htu.shutdownMiniCluster();
      }
    }
  }

  private void runScanner(HTable table, boolean slow) throws Exception {
    long time = System.nanoTime();
    Scan scan = new Scan();
    scan.addColumn(cf_essential, col_name);
    scan.addColumn(cf_joined, col_name);

    SingleColumnValueFilter filter = new SingleColumnValueFilter(
        cf_essential, col_name, CompareFilter.CompareOp.EQUAL, flag_yes);
    filter.setFilterIfMissing(true);
    scan.setFilter(filter);
    scan.setLoadColumnFamiliesOnDemand(!slow);

    ResultScanner result_scanner = table.getScanner(scan);
    Result res;
    long rows_count = 0;
    while ((res = result_scanner.next()) != null) {
      rows_count++;
    }

    double timeSec = (System.nanoTime() - time) / 1000000000.0;
    result_scanner.close();
    LOG.info((slow ? "Slow" : "Joined") + " scanner finished in " + Double.toString(timeSec)
      + " seconds, got " + Long.toString(rows_count/2) + " rows");
  }

  private static HRegion initHRegion(byte[] tableName, byte[] startKey, byte[] stopKey,
      String callingMethod, Configuration conf, byte[]... families)
      throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(htd.getName(), startKey, stopKey, false);
    Path path = new Path(DIR + callingMethod);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("Failed delete of " + path);
      }
    }
    return HRegion.createHRegion(info, path, conf, htd);
  }
}