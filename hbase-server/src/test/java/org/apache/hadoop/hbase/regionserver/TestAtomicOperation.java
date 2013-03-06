/**
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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.junit.experimental.categories.Category;


/**
 * Testing of HRegion.incrementColumnValue, HRegion.increment,
 * and HRegion.append
 */
@Category(MediumTests.class) // Starts 100 threads
public class TestAtomicOperation extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestAtomicOperation.class);

  HRegion region = null;
  private HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final String DIR = TEST_UTIL.getDataTestDir("TestAtomicOperation").toString();


  // Test names
  static final byte[] tableName = Bytes.toBytes("testtable");;
  static final byte[] qual1 = Bytes.toBytes("qual1");
  static final byte[] qual2 = Bytes.toBytes("qual2");
  static final byte[] qual3 = Bytes.toBytes("qual3");
  static final byte[] value1 = Bytes.toBytes("value1");
  static final byte[] value2 = Bytes.toBytes("value2");
  static final byte [] row = Bytes.toBytes("rowA");
  static final byte [] row2 = Bytes.toBytes("rowB");

  /**
   * @see org.apache.hadoop.hbase.HBaseTestCase#setUp()
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    EnvironmentEdgeManagerTestHelper.reset();
  }

  //////////////////////////////////////////////////////////////////////////////
  // New tests that doesn't spin up a mini cluster but rather just test the
  // individual code pieces in the HRegion. 
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Test basic append operation.
   * More tests in
   * @see org.apache.hadoop.hbase.client.TestFromClientSide#testAppend()
   */
  public void testAppend() throws IOException {
    initHRegion(tableName, getName(), fam1);
    String v1 = "Ultimate Answer to the Ultimate Question of Life,"+
    " The Universe, and Everything";
    String v2 = " is... 42.";
    Append a = new Append(row);
    a.setReturnResults(false);
    a.add(fam1, qual1, Bytes.toBytes(v1));
    a.add(fam1, qual2, Bytes.toBytes(v2));
    assertNull(region.append(a, null, true));
    a = new Append(row);
    a.add(fam1, qual1, Bytes.toBytes(v2));
    a.add(fam1, qual2, Bytes.toBytes(v1));
    Result result = region.append(a, null, true);
    assertEquals(0, Bytes.compareTo(Bytes.toBytes(v1+v2), result.getValue(fam1, qual1)));
    assertEquals(0, Bytes.compareTo(Bytes.toBytes(v2+v1), result.getValue(fam1, qual2)));
  }

  /**
   * Test multi-threaded increments.
   */
  public void testIncrementMultiThreads() throws IOException {

    LOG.info("Starting test testIncrementMultiThreads");
    // run a with mixed column families (1 and 3 versions)
    initHRegion(tableName, getName(), new int[] {1,3}, fam1, fam2);

    // create 100 threads, each will increment by its own quantity
    int numThreads = 100;
    int incrementsPerThread = 1000;
    Incrementer[] all = new Incrementer[numThreads];
    int expectedTotal = 0;

    // create all threads
    for (int i = 0; i < numThreads; i++) {
      all[i] = new Incrementer(region, i, i, incrementsPerThread);
      expectedTotal += (i * incrementsPerThread);
    }

    // run all threads
    for (int i = 0; i < numThreads; i++) {
      all[i].start();
    }

    // wait for all threads to finish
    for (int i = 0; i < numThreads; i++) {
      try {
        all[i].join();
      } catch (InterruptedException e) {
      }
    }
    assertICV(row, fam1, qual1, expectedTotal);
    assertICV(row, fam1, qual2, expectedTotal*2);
    assertICV(row, fam2, qual3, expectedTotal*3);
    LOG.info("testIncrementMultiThreads successfully verified that total is " +
             expectedTotal);
  }


  private void assertICV(byte [] row,
                         byte [] familiy,
                         byte[] qualifier,
                         long amount) throws IOException {
    // run a get and see?
    Get get = new Get(row);
    get.addColumn(familiy, qualifier);
    Result result = region.get(get, null);
    assertEquals(1, result.size());

    KeyValue kv = result.raw()[0];
    long r = Bytes.toLong(kv.getValue());
    assertEquals(amount, r);
  }

  private void initHRegion (byte [] tableName, String callingMethod,
      byte[] ... families)
    throws IOException {
    initHRegion(tableName, callingMethod, null, families);
  }

  private void initHRegion (byte [] tableName, String callingMethod, int [] maxVersions,
    byte[] ... families)
  throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    int i=0;
    for(byte [] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      hcd.setMaxVersions(maxVersions != null ? maxVersions[i++] : 1);
      htd.addFamily(hcd);
    }
    HRegionInfo info = new HRegionInfo(htd.getName(), null, null, false);
    Path path = new Path(DIR + callingMethod);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("Failed delete of " + path);
      }
    }
    region = HRegion.createHRegion(info, path, HBaseConfiguration.create(), htd);
  }

  /**
   * A thread that makes a few increment calls
   */
  public static class Incrementer extends Thread {

    private final HRegion region;
    private final int numIncrements;
    private final int amount;


    public Incrementer(HRegion region,
        int threadNumber, int amount, int numIncrements) {
      this.region = region;
      this.numIncrements = numIncrements;
      this.amount = amount;
      setDaemon(true);
    }

    @Override
    public void run() {
      for (int i=0; i<numIncrements; i++) {
        try {
          Increment inc = new Increment(row);
          inc.addColumn(fam1, qual1, amount);
          inc.addColumn(fam1, qual2, amount*2);
          inc.addColumn(fam2, qual3, amount*3);
          region.increment(inc, null, true);

          // verify: Make sure we only see completed increments
          Get g = new Get(row);
          Result result = region.get(g, null);
          assertEquals(Bytes.toLong(result.getValue(fam1, qual1))*2, Bytes.toLong(result.getValue(fam1, qual2))); 
          assertEquals(Bytes.toLong(result.getValue(fam1, qual1))*3, Bytes.toLong(result.getValue(fam2, qual3)));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void testAppendMultiThreads() throws IOException {
    LOG.info("Starting test testAppendMultiThreads");
    // run a with mixed column families (1 and 3 versions)
    initHRegion(tableName, getName(), new int[] {1,3}, fam1, fam2);

    int numThreads = 100;
    int opsPerThread = 100;
    AtomicOperation[] all = new AtomicOperation[numThreads];
    final byte[] val = new byte[]{1};

    AtomicInteger failures = new AtomicInteger(0);
    // create all threads
    for (int i = 0; i < numThreads; i++) {
      all[i] = new AtomicOperation(region, opsPerThread, null, failures) {
        @Override
        public void run() {
          for (int i=0; i<numOps; i++) {
            try {
              Append a = new Append(row);
              a.add(fam1, qual1, val);
              a.add(fam1, qual2, val);
              a.add(fam2, qual3, val);
              region.append(a, null, true);

              Get g = new Get(row);
              Result result = region.get(g, null);
              assertEquals(result.getValue(fam1, qual1).length, result.getValue(fam1, qual2).length); 
              assertEquals(result.getValue(fam1, qual1).length, result.getValue(fam2, qual3).length); 
            } catch (IOException e) {
              e.printStackTrace();
              failures.incrementAndGet();
              fail();
            }
          }
        }
      };
    }

    // run all threads
    for (int i = 0; i < numThreads; i++) {
      all[i].start();
    }

    // wait for all threads to finish
    for (int i = 0; i < numThreads; i++) {
      try {
        all[i].join();
      } catch (InterruptedException e) {
      }
    }
    assertEquals(0, failures.get());
    Get g = new Get(row);
    Result result = region.get(g, null);
    assertEquals(result.getValue(fam1, qual1).length, 10000);
    assertEquals(result.getValue(fam1, qual2).length, 10000);
    assertEquals(result.getValue(fam2, qual3).length, 10000);
  }
  /**
   * Test multi-threaded row mutations.
   */
  public void testRowMutationMultiThreads() throws IOException {

    LOG.info("Starting test testRowMutationMultiThreads");
    initHRegion(tableName, getName(), fam1);

    // create 10 threads, each will alternate between adding and
    // removing a column
    int numThreads = 10;
    int opsPerThread = 500;
    AtomicOperation[] all = new AtomicOperation[numThreads];

    AtomicLong timeStamps = new AtomicLong(0);
    AtomicInteger failures = new AtomicInteger(0);
    // create all threads
    for (int i = 0; i < numThreads; i++) {
      all[i] = new AtomicOperation(region, opsPerThread, timeStamps, failures) {
        @Override
        public void run() {
          boolean op = true;
          for (int i=0; i<numOps; i++) {
            try {
              // throw in some flushes
              if (i%10==0) {
                synchronized(region) {
                  LOG.debug("flushing");
                  region.flushcache();
                  if (i%100==0) {
                    region.compactStores();
                  }
                }
              }
              long ts = timeStamps.incrementAndGet();
              RowMutations rm = new RowMutations(row);
              if (op) {
                Put p = new Put(row, ts);
                p.add(fam1, qual1, value1);
                rm.add(p);
                Delete d = new Delete(row);
                d.deleteColumns(fam1, qual2, ts);
                rm.add(d);
              } else {
                Delete d = new Delete(row);
                d.deleteColumns(fam1, qual1, ts);
                rm.add(d);
                Put p = new Put(row, ts);
                p.add(fam1, qual2, value2);
                rm.add(p);
              }
              region.mutateRow(rm);
              op ^= true;
              // check: should always see exactly one column
              Get g = new Get(row);
              Result r = region.get(g, null);
              if (r.size() != 1) {
                LOG.debug(r);
                failures.incrementAndGet();
                fail();
              }
            } catch (IOException e) {
              e.printStackTrace();
              failures.incrementAndGet();
              fail();
            }
          }
        }
      };
    }

    // run all threads
    for (int i = 0; i < numThreads; i++) {
      all[i].start();
    }

    // wait for all threads to finish
    for (int i = 0; i < numThreads; i++) {
      try {
        all[i].join();
      } catch (InterruptedException e) {
      }
    }
    assertEquals(0, failures.get());
  }


  /**
   * Test multi-threaded region mutations.
   */
  public void testMultiRowMutationMultiThreads() throws IOException {

    LOG.info("Starting test testMultiRowMutationMultiThreads");
    initHRegion(tableName, getName(), fam1);

    // create 10 threads, each will alternate between adding and
    // removing a column
    int numThreads = 10;
    int opsPerThread = 500;
    AtomicOperation[] all = new AtomicOperation[numThreads];

    AtomicLong timeStamps = new AtomicLong(0);
    AtomicInteger failures = new AtomicInteger(0);
    final List<byte[]> rowsToLock = Arrays.asList(row, row2);
    // create all threads
    for (int i = 0; i < numThreads; i++) {
      all[i] = new AtomicOperation(region, opsPerThread, timeStamps, failures) {
        @Override
        public void run() {
          boolean op = true;
          for (int i=0; i<numOps; i++) {
            try {
              // throw in some flushes
              if (i%10==0) {
                synchronized(region) {
                  LOG.debug("flushing");
                  region.flushcache();
                  if (i%100==0) {
                    region.compactStores();
                  }
                }
              }
              long ts = timeStamps.incrementAndGet();
              List<Mutation> mrm = new ArrayList<Mutation>();
              if (op) {
                Put p = new Put(row2, ts);
                p.add(fam1, qual1, value1);
                mrm.add(p);
                Delete d = new Delete(row);
                d.deleteColumns(fam1, qual1, ts);
                mrm.add(d);
              } else {
                Delete d = new Delete(row2);
                d.deleteColumns(fam1, qual1, ts);
                mrm.add(d);
                Put p = new Put(row, ts);
                p.add(fam1, qual1, value2);
                mrm.add(p);
              }
              region.mutateRowsWithLocks(mrm, rowsToLock);
              op ^= true;
              // check: should always see exactly one column
              Scan s = new Scan(row);
              RegionScanner rs = region.getScanner(s);
              List<KeyValue> r = new ArrayList<KeyValue>();
              while(rs.next(r));
              rs.close();
              if (r.size() != 1) {
                LOG.debug(r);
                failures.incrementAndGet();
                fail();
              }
            } catch (IOException e) {
              e.printStackTrace();
              failures.incrementAndGet();
              fail();
            }
          }
        }
      };
    }

    // run all threads
    for (int i = 0; i < numThreads; i++) {
      all[i].start();
    }

    // wait for all threads to finish
    for (int i = 0; i < numThreads; i++) {
      try {
        all[i].join();
      } catch (InterruptedException e) {
      }
    }
    assertEquals(0, failures.get());
  }

  public static class AtomicOperation extends Thread {
    protected final HRegion region;
    protected final int numOps;
    protected final AtomicLong timeStamps;
    protected final AtomicInteger failures;
    protected final Random r = new Random();

    public AtomicOperation(HRegion region, int numOps, AtomicLong timeStamps,
        AtomicInteger failures) {
      this.region = region;
      this.numOps = numOps;
      this.timeStamps = timeStamps;
      this.failures = failures;
    }
  }

}

