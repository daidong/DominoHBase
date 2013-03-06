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
package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.RegionCoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.LockRowRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.LockRowResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.UnlockRowRequest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.CompareType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * <p>Used to communicate with a single HBase table.
 *
 * <p>This class is not thread safe for reads nor write.
 *
 * <p>In case of writes (Put, Delete), the underlying write buffer can
 * be corrupted if multiple threads contend over a single HTable instance.
 *
 * <p>In case of reads, some fields used by a Scan are shared among all threads.
 * The HTable implementation can either not contract to be safe in case of a Get
 *
 * <p>To access a table in a multi threaded environment, please consider
 * using the {@link HTablePool} class to create your HTable instances.
 *
 * <p>Instances of HTable passed the same {@link Configuration} instance will
 * share connections to servers out on the cluster and to the zookeeper ensemble
 * as well as caches of region locations.  This is usually a *good* thing and it
 * is recommended to reuse the same configuration object for all your tables.
 * This happens because they will all share the same underlying
 * {@link HConnection} instance. See {@link HConnectionManager} for more on
 * how this mechanism works.
 *
 * <p>{@link HConnection} will read most of the
 * configuration it needs from the passed {@link Configuration} on initial
 * construction.  Thereafter, for settings such as
 * <code>hbase.client.pause</code>, <code>hbase.client.retries.number</code>,
 * and <code>hbase.client.rpc.maxattempts</code> updating their values in the
 * passed {@link Configuration} subsequent to {@link HConnection} construction
 * will go unnoticed.  To run with changed values, make a new
 * {@link HTable} passing a new {@link Configuration} instance that has the
 * new configuration.
 *
 * <p>Note that this class implements the {@link Closeable} interface. When a
 * HTable instance is no longer required, it *should* be closed in order to ensure
 * that the underlying resources are promptly released. Please note that the close
 * method can throw java.io.IOException that must be handled.
 *
 * @see HBaseAdmin for create, drop, list, enable and disable of tables.
 * @see HConnection
 * @see HConnectionManager
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HTable implements HTableInterface {
  private static final Log LOG = LogFactory.getLog(HTable.class);
  private HConnection connection;
  private final byte [] tableName;
  private volatile Configuration configuration;
  private final ArrayList<Put> writeBuffer = new ArrayList<Put>();
  private long writeBufferSize;
  private boolean clearBufferOnFail;
  private boolean autoFlush;
  private long currentWriteBufferSize;
  protected int scannerCaching;
  private int maxKeyValueSize;
  private ExecutorService pool;  // For Multi
  private boolean closed;
  private int operationTimeout;
  private static final int DOPUT_WB_CHECK = 10;    // i.e., doPut checks the writebuffer every X Puts.
  private final boolean cleanupPoolOnClose; // shutdown the pool in close()
  private final boolean cleanupConnectionOnClose; // close the connection in close()

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.  Recommended.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final String tableName)
  throws IOException {
    this(conf, Bytes.toBytes(tableName));
  }


  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.  Recommended.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final byte [] tableName)
  throws IOException {
    this.tableName = tableName;
    this.cleanupPoolOnClose = this.cleanupConnectionOnClose = true;
    if (conf == null) {
      this.connection = null;
      return;
    }
    this.connection = HConnectionManager.getConnection(conf);
    this.configuration = conf;

    int maxThreads = conf.getInt("hbase.htable.threads.max", Integer.MAX_VALUE);
    if (maxThreads == 0) {
      maxThreads = 1; // is there a better default?
    }
    long keepAliveTime = conf.getLong("hbase.htable.threads.keepalivetime", 60);

    // Using the "direct handoff" approach, new threads will only be created
    // if it is necessary and will grow unbounded. This could be bad but in HCM
    // we only create as many Runnables as there are region servers. It means
    // it also scales when new region servers are added.
    this.pool = new ThreadPoolExecutor(1, maxThreads, keepAliveTime, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("hbase-table"));
    ((ThreadPoolExecutor) this.pool).allowCoreThreadTimeOut(true);

    this.finishSetup();
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>conf</code> instance.  Uses already-populated
   * region cache if one is available, populated by any other HTable instances
   * sharing this <code>conf</code> instance.
   * Use this constructor when the ExecutorService is externally managed.
   * @param conf Configuration object to use.
   * @param tableName Name of the table.
   * @param pool ExecutorService to be used.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(Configuration conf, final byte[] tableName, final ExecutorService pool)
      throws IOException {
    this.connection = HConnectionManager.getConnection(conf);
    this.configuration = conf;
    this.pool = pool;
    this.tableName = tableName;
    this.cleanupPoolOnClose = false;
    this.cleanupConnectionOnClose = true;

    this.finishSetup();
  }

  /**
   * Creates an object to access a HBase table.
   * Shares zookeeper connection and other resources with other HTable instances
   * created with the same <code>connection</code> instance.
   * Use this constructor when the ExecutorService and HConnection instance are
   * externally managed.
   * @param tableName Name of the table.
   * @param connection HConnection to be used.
   * @param pool ExecutorService to be used.
   * @throws IOException if a remote or network exception occurs
   */
  public HTable(final byte[] tableName, final HConnection connection,
      final ExecutorService pool) throws IOException {
    if (pool == null || pool.isShutdown()) {
      throw new IllegalArgumentException("Pool is null or shut down.");
    }
    if (connection == null || connection.isClosed()) {
      throw new IllegalArgumentException("Connection is null or closed.");
    }
    this.tableName = tableName;
    this.cleanupPoolOnClose = this.cleanupConnectionOnClose = false;
    this.connection = connection;
    this.configuration = connection.getConfiguration();
    this.pool = pool;

    this.finishSetup();
  }

  /**
   * setup this HTable's parameter based on the passed configuration
   */
  private void finishSetup() throws IOException {
    this.connection.locateRegion(tableName, HConstants.EMPTY_START_ROW);
    this.operationTimeout = HTableDescriptor.isMetaTable(tableName) ? HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT
        : this.configuration.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
            HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    this.writeBufferSize = this.configuration.getLong(
        "hbase.client.write.buffer", 2097152);
    this.clearBufferOnFail = true;
    this.autoFlush = true;
    this.currentWriteBufferSize = 0;
    this.scannerCaching = this.configuration.getInt(
        HConstants.HBASE_CLIENT_SCANNER_CACHING,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);

    this.maxKeyValueSize = this.configuration.getInt(
        "hbase.client.keyvalue.maxsize", -1);
    this.closed = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Tells whether or not a table is enabled or not. This method creates a
   * new HBase configuration, so it might make your unit tests fail due to
   * incorrect ZK client port.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
	* @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(Bytes.toBytes(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not. This method creates a
   * new HBase configuration, so it might make your unit tests fail due to
   * incorrect ZK client port.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(byte[] tableName) throws IOException {
    return isTableEnabled(HBaseConfiguration.create(), tableName);
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
	 * @deprecated use {@link HBaseAdmin#isTableEnabled(byte[])}
   */
  @Deprecated
  public static boolean isTableEnabled(Configuration conf, String tableName)
  throws IOException {
    return isTableEnabled(conf, Bytes.toBytes(tableName));
  }

  /**
   * Tells whether or not a table is enabled or not.
   * @param conf The Configuration object to use.
   * @param tableName Name of table to check.
   * @return {@code true} if table is online.
   * @throws IOException if a remote or network exception occurs
   * @deprecated use {@link HBaseAdmin#isTableEnabled(byte[] tableName)}
   */
  @Deprecated
  public static boolean isTableEnabled(Configuration conf,
      final byte[] tableName) throws IOException {
    return HConnectionManager.execute(new HConnectable<Boolean>(conf) {
      @Override
      public Boolean connect(HConnection connection) throws IOException {
        return connection.isTableEnabled(tableName);
      }
    });
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return The location of the given row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final String row)
  throws IOException {
    return connection.getRegionLocation(tableName, Bytes.toBytes(row), false);
  }

  /**
   * Finds the region on which the given row is being served. Does not reload the cache.
   * @param row Row to find.
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final byte [] row)
  throws IOException {
    return connection.getRegionLocation(tableName, row, false);
  }

  /**
   * Finds the region on which the given row is being served.
   * @param row Row to find.
   * @param reload true to reload information or false to use cached information
   * @return Location of the row.
   * @throws IOException if a remote or network exception occurs
   */
  public HRegionLocation getRegionLocation(final byte [] row, boolean reload)
  throws IOException {
    return connection.getRegionLocation(tableName, row, reload);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte [] getTableName() {
    return this.tableName;
  }

  /**
   * <em>INTERNAL</em> Used by unit tests and tools to do low-level
   * manipulations.
   * @return An HConnection instance.
   * @deprecated This method will be changed from public to package protected.
   */
  // TODO(tsuna): Remove this.  Unit tests shouldn't require public helpers.
  @Deprecated
  public HConnection getConnection() {
    return this.connection;
  }

  /**
   * Gets the number of rows that a scanner will fetch at once.
   * <p>
   * The default value comes from {@code hbase.client.scanner.caching}.
   * @deprecated Use {@link Scan#setCaching(int)} and {@link Scan#getCaching()}
   */
  @Deprecated
  public int getScannerCaching() {
    return scannerCaching;
  }

  /**
   * Sets the number of rows that a scanner will fetch at once.
   * <p>
   * This will override the value specified by
   * {@code hbase.client.scanner.caching}.
   * Increasing this value will reduce the amount of work needed each time
   * {@code next()} is called on a scanner, at the expense of memory use
   * (since more rows will need to be maintained in memory by the scanners).
   * @param scannerCaching the number of rows a scanner will fetch at once.
   * @deprecated Use {@link Scan#setCaching(int)}
   */
  @Deprecated
  public void setScannerCaching(int scannerCaching) {
    this.scannerCaching = scannerCaching;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return new UnmodifyableHTableDescriptor(
      this.connection.getHTableDescriptor(this.tableName));
  }

  /**
   * Gets the starting row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region starting row keys
   * @throws IOException if a remote or network exception occurs
   */
  public byte [][] getStartKeys() throws IOException {
    return getStartEndKeys().getFirst();
  }

  /**
   * Gets the ending row key for every region in the currently open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Array of region ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  public byte[][] getEndKeys() throws IOException {
    return getStartEndKeys().getSecond();
  }

  /**
   * Gets the starting and ending row keys for every region in the currently
   * open table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return Pair of arrays of region starting and ending row keys
   * @throws IOException if a remote or network exception occurs
   */
  public Pair<byte[][],byte[][]> getStartEndKeys() throws IOException {
    NavigableMap<HRegionInfo, ServerName> regions = getRegionLocations();
    final List<byte[]> startKeyList = new ArrayList<byte[]>(regions.size());
    final List<byte[]> endKeyList = new ArrayList<byte[]>(regions.size());

    for (HRegionInfo region : regions.keySet()) {
      startKeyList.add(region.getStartKey());
      endKeyList.add(region.getEndKey());
    }

    return new Pair<byte [][], byte [][]>(
      startKeyList.toArray(new byte[startKeyList.size()][]),
      endKeyList.toArray(new byte[endKeyList.size()][]));
  }

  /**
   * Gets all the regions and their address for this table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   * @return A map of HRegionInfo with it's server address
   * @throws IOException if a remote or network exception occurs
   */
  public NavigableMap<HRegionInfo, ServerName> getRegionLocations() throws IOException {
    // TODO: Odd that this returns a Map of HRI to SN whereas getRegionLocation, singular, returns an HRegionLocation.
    return MetaScanner.allTableRegions(getConfiguration(), getTableName(), false);
  }

  /**
   * Get the corresponding regions for an arbitrary range of keys.
   * <p>
   * @param startKey Starting row in range, inclusive
   * @param endKey Ending row in range, exclusive
   * @return A list of HRegionLocations corresponding to the regions that
   * contain the specified range
   * @throws IOException if a remote or network exception occurs
   */
  public List<HRegionLocation> getRegionsInRange(final byte [] startKey,
    final byte [] endKey) throws IOException {
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey,
                                                    HConstants.EMPTY_END_ROW);
    if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
      throw new IllegalArgumentException(
        "Invalid range: " + Bytes.toStringBinary(startKey) +
        " > " + Bytes.toStringBinary(endKey));
    }
    final List<HRegionLocation> regionList = new ArrayList<HRegionLocation>();
    byte [] currentKey = startKey;
    do {
      HRegionLocation regionLocation = getRegionLocation(currentKey, false);
      regionList.add(regionLocation);
      currentKey = regionLocation.getRegionInfo().getEndKey();
    } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW) &&
             (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));
    return regionList;
  }

  /**
   * {@inheritDoc}
   */
   @Override
   public Result getRowOrBefore(final byte[] row, final byte[] family)
   throws IOException {
     return new ServerCallable<Result>(connection, tableName, row, operationTimeout) {
       public Result call() throws IOException {
         return ProtobufUtil.getRowOrBefore(server,
           location.getRegionInfo().getRegionName(), row, family);
       }
     }.withRetries();
   }

   /**
    * {@inheritDoc}
    */
  @Override
  public ResultScanner getScanner(final Scan scan) throws IOException {
    if (scan.getCaching() <= 0) {
      scan.setCaching(getScannerCaching());
    }
    return new ClientScanner(getConfiguration(), scan, getTableName(),
        this.connection);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte [] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte [] family, byte [] qualifier)
  throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result get(final Get get) throws IOException {
    return new ServerCallable<Result>(connection, tableName, get.getRow(), operationTimeout) {
          public Result call() throws IOException {
            return ProtobufUtil.get(server,
              location.getRegionInfo().getRegionName(), get);
          }
        }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result[] get(List<Get> gets) throws IOException {
    try {
      Object [] r1 = batch((List)gets);

      // translate.
      Result [] results = new Result[r1.length];
      int i=0;
      for (Object o : r1) {
        // batch ensures if there is a failure we get an exception instead
        results[i++] = (Result) o;
      }

      return results;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void batch(final List<?extends Row> actions, final Object[] results)
      throws InterruptedException, IOException {
    connection.processBatchCallback(actions, tableName, pool, results, null);
  }

  @Override
  public Object[] batch(final List<? extends Row> actions)
     throws InterruptedException, IOException {
    Object[] results = new Object[actions.size()];
    connection.processBatchCallback(actions, tableName, pool, results, null);
    return results;
  }

  @Override
  public <R> void batchCallback(
    final List<? extends Row> actions, final Object[] results, final Batch.Callback<R> callback)
    throws IOException, InterruptedException {
    connection.processBatchCallback(actions, tableName, pool, results, callback);
  }

  @Override
  public <R> Object[] batchCallback(
    final List<? extends Row> actions, final Batch.Callback<R> callback) throws IOException,
      InterruptedException {
    Object[] results = new Object[actions.size()];
    connection.processBatchCallback(actions, tableName, pool, results, callback);
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final Delete delete)
  throws IOException {
    new ServerCallable<Boolean>(connection, tableName, delete.getRow(), operationTimeout) {
          public Boolean call() throws IOException {
            try {
              MutateRequest request = RequestConverter.buildMutateRequest(
                location.getRegionInfo().getRegionName(), delete);
              MutateResponse response = server.mutate(null, request);
              return Boolean.valueOf(response.getProcessed());
            } catch (ServiceException se) {
              throw ProtobufUtil.getRemoteException(se);
            }
          }
        }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final List<Delete> deletes)
  throws IOException {
    Object[] results = new Object[deletes.size()];
    try {
      connection.processBatch((List) deletes, tableName, pool, results);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      // mutate list so that it is empty for complete success, or contains only failed records
      // results are returned in the same order as the requests in list
      // walk the list backwards, so we can remove from list without impacting the indexes of earlier members
      for (int i = results.length - 1; i>=0; i--) {
        // if result is not null, it succeeded
        if (results[i] instanceof Result) {
          deletes.remove(i);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final Put put) throws IOException {
    doPut(Arrays.asList(put));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final List<Put> puts) throws IOException {
    doPut(puts);
  }

  private void doPut(final List<Put> puts) throws IOException {
    int n = 0;
    for (Put put : puts) {
      validatePut(put);
      writeBuffer.add(put);
      currentWriteBufferSize += put.heapSize();

      // we need to periodically see if the writebuffer is full instead of waiting until the end of the List
      n++;
      if (n % DOPUT_WB_CHECK == 0 && currentWriteBufferSize > writeBufferSize) {
        flushCommits();
      }
    }
    if (autoFlush || currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mutateRow(final RowMutations rm) throws IOException {
    new ServerCallable<Void>(connection, tableName, rm.getRow(),
        operationTimeout) {
      public Void call() throws IOException {
        try {
          MultiRequest request = RequestConverter.buildMultiRequest(
            location.getRegionInfo().getRegionName(), rm);
          server.multi(null, request);
        } catch (ServiceException se) {
          throw ProtobufUtil.getRemoteException(se);
        }
        return null;
      }
    }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result append(final Append append) throws IOException {
    if (append.numFamilies() == 0) {
      throw new IOException(
          "Invalid arguments to append, no columns specified");
    }
    return new ServerCallable<Result>(connection, tableName, append.getRow(), operationTimeout) {
          public Result call() throws IOException {
            try {
              MutateRequest request = RequestConverter.buildMutateRequest(
                location.getRegionInfo().getRegionName(), append);
              MutateResponse response = server.mutate(null, request);
              if (!response.hasResult()) return null;
              return ProtobufUtil.toResult(response.getResult());
            } catch (ServiceException se) {
              throw ProtobufUtil.getRemoteException(se);
            }
          }
        }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result increment(final Increment increment) throws IOException {
    if (!increment.hasFamilies()) {
      throw new IOException(
          "Invalid arguments to increment, no columns specified");
    }
    return new ServerCallable<Result>(connection, tableName, increment.getRow(), operationTimeout) {
          public Result call() throws IOException {
            try {
              MutateRequest request = RequestConverter.buildMutateRequest(
                location.getRegionInfo().getRegionName(), increment);
              MutateResponse response = server.mutate(null, request);
              return ProtobufUtil.toResult(response.getResult());
            } catch (ServiceException se) {
              throw ProtobufUtil.getRemoteException(se);
            }
          }
        }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount)
  throws IOException {
    return incrementColumnValue(row, family, qualifier, amount, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(final byte [] row, final byte [] family,
      final byte [] qualifier, final long amount, final boolean writeToWAL)
  throws IOException {
    NullPointerException npe = null;
    if (row == null) {
      npe = new NullPointerException("row is null");
    } else if (family == null) {
      npe = new NullPointerException("family is null");
    } else if (qualifier == null) {
      npe = new NullPointerException("qualifier is null");
    }
    if (npe != null) {
      throw new IOException(
          "Invalid arguments to incrementColumnValue", npe);
    }
    return new ServerCallable<Long>(connection, tableName, row, operationTimeout) {
          public Long call() throws IOException {
            try {
              MutateRequest request = RequestConverter.buildMutateRequest(
                location.getRegionInfo().getRegionName(), row, family,
                qualifier, amount, writeToWAL);
              MutateResponse response = server.mutate(null, request);
              Result result = ProtobufUtil.toResult(response.getResult());
              return Long.valueOf(Bytes.toLong(result.getValue(family, qualifier)));
            } catch (ServiceException se) {
              throw ProtobufUtil.getRemoteException(se);
            }
          }
        }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndPut(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put)
  throws IOException {
    return new ServerCallable<Boolean>(connection, tableName, row, operationTimeout) {
          public Boolean call() throws IOException {
            try {
              MutateRequest request = RequestConverter.buildMutateRequest(
                location.getRegionInfo().getRegionName(), row, family, qualifier,
                new BinaryComparator(value), CompareType.EQUAL, put);
              MutateResponse response = server.mutate(null, request);
              return Boolean.valueOf(response.getProcessed());
            } catch (ServiceException se) {
              throw ProtobufUtil.getRemoteException(se);
            }
          }
        }.withRetries();
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndDelete(final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Delete delete)
  throws IOException {
    return new ServerCallable<Boolean>(connection, tableName, row, operationTimeout) {
          public Boolean call() throws IOException {
            try {
              MutateRequest request = RequestConverter.buildMutateRequest(
                location.getRegionInfo().getRegionName(), row, family, qualifier,
                new BinaryComparator(value), CompareType.EQUAL, delete);
              MutateResponse response = server.mutate(null, request);
              return Boolean.valueOf(response.getProcessed());
            } catch (ServiceException se) {
              throw ProtobufUtil.getRemoteException(se);
            }
          }
        }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean exists(final Get get) throws IOException {
    return new ServerCallable<Boolean>(connection, tableName, get.getRow(), operationTimeout) {
          public Boolean call() throws IOException {
            try {
              GetRequest request = RequestConverter.buildGetRequest(
                  location.getRegionInfo().getRegionName(), get, true);
              GetResponse response = server.get(null, request);
              return response.getExists();
            } catch (ServiceException se) {
              throw ProtobufUtil.getRemoteException(se);
            }
          }
        }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flushCommits() throws IOException {
    Object[] results = new Object[writeBuffer.size()];
    boolean success = false;
    try {
      this.connection.processBatch(writeBuffer, tableName, pool, results);
      success = true;
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.getMessage());
    } finally {
      // mutate list so that it is empty for complete success, or contains
      // only failed records. Results are returned in the same order as the
      // requests in list. Walk the list backwards, so we can remove from list
      // without impacting the indexes of earlier members
      currentWriteBufferSize = 0;
      if (success || clearBufferOnFail) {
        writeBuffer.clear();
      } else {
        for (int i = results.length - 1; i >= 0; i--) {
          if (results[i] instanceof Result) {
            writeBuffer.remove(i);
          } else {
            currentWriteBufferSize += writeBuffer.get(i).heapSize();
          }
        }
      }
    }
  }

  /**
   * Process a mixed batch of Get, Put and Delete actions. All actions for a
   * RegionServer are forwarded in one RPC call. Queries are executed in parallel.
   *
   * @param list The collection of actions.
   * @param results An empty array, same size as list. If an exception is thrown,
   * you can test here for partial results, and to determine which actions
   * processed successfully.
   * @throws IOException if there are problems talking to META. Per-item
   * exceptions are stored in the results array.
   */
  public <R> void processBatchCallback(
    final List<? extends Row> list, final Object[] results, final Batch.Callback<R> callback)
    throws IOException, InterruptedException {
    connection.processBatchCallback(list, tableName, pool, results, callback);
  }


  /**
   * Parameterized batch processing, allowing varying return types for different
   * {@link Row} implementations.
   */
  public void processBatch(final List<? extends Row> list, final Object[] results)
    throws IOException, InterruptedException {

    this.processBatchCallback(list, results, null);
  }


  @Override
  public void close() throws IOException {
    if (this.closed) {
      return;
    }
    flushCommits();
    if (cleanupPoolOnClose) {
      this.pool.shutdown();
    }
    if (cleanupConnectionOnClose) {
      if (this.connection != null) {
        this.connection.close();
      }
    }
    this.closed = true;
  }

  // validate for well-formedness
  public void validatePut(final Put put) throws IllegalArgumentException{
    if (put.isEmpty()) {
      throw new IllegalArgumentException("No columns to insert");
    }
    if (maxKeyValueSize > 0) {
      for (List<KeyValue> list : put.getFamilyMap().values()) {
        for (KeyValue kv : list) {
          if (kv.getLength() > maxKeyValueSize) {
            throw new IllegalArgumentException("KeyValue size too large");
          }
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowLock lockRow(final byte [] row)
  throws IOException {
    return new ServerCallable<RowLock>(connection, tableName, row, operationTimeout) {
        public RowLock call() throws IOException {
          try {
            LockRowRequest request = RequestConverter.buildLockRowRequest(
              location.getRegionInfo().getRegionName(), row);
            LockRowResponse response = server.lockRow(null, request);
            return new RowLock(row, response.getLockId());
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        }
      }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void unlockRow(final RowLock rl)
  throws IOException {
    new ServerCallable<Boolean>(connection, tableName, rl.getRow(), operationTimeout) {
        public Boolean call() throws IOException {
          try {
            UnlockRowRequest request = RequestConverter.buildUnlockRowRequest(
              location.getRegionInfo().getRegionName(), rl.getLockId());
            server.unlockRow(null, request);
            return Boolean.TRUE;
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          }
        }
      }.withRetries();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAutoFlush() {
    return autoFlush;
  }

  /**
   * See {@link #setAutoFlush(boolean, boolean)}
   *
   * @param autoFlush
   *          Whether or not to enable 'auto-flush'.
   */
  public void setAutoFlush(boolean autoFlush) {
    setAutoFlush(autoFlush, autoFlush);
  }

  /**
   * Turns 'auto-flush' on or off.
   * <p>
   * When enabled (default), {@link Put} operations don't get buffered/delayed
   * and are immediately executed. Failed operations are not retried. This is
   * slower but safer.
   * <p>
   * Turning off {@link #autoFlush} means that multiple {@link Put}s will be
   * accepted before any RPC is actually sent to do the write operations. If the
   * application dies before pending writes get flushed to HBase, data will be
   * lost.
   * <p>
   * When you turn {@link #autoFlush} off, you should also consider the
   * {@link #clearBufferOnFail} option. By default, asynchronous {@link Put}
   * requests will be retried on failure until successful. However, this can
   * pollute the writeBuffer and slow down batching performance. Additionally,
   * you may want to issue a number of Put requests and call
   * {@link #flushCommits()} as a barrier. In both use cases, consider setting
   * clearBufferOnFail to true to erase the buffer after {@link #flushCommits()}
   * has been called, regardless of success.
   *
   * @param autoFlush
   *          Whether or not to enable 'auto-flush'.
   * @param clearBufferOnFail
   *          Whether to keep Put failures in the writeBuffer
   * @see #flushCommits
   */
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    this.autoFlush = autoFlush;
    this.clearBufferOnFail = autoFlush || clearBufferOnFail;
  }

  /**
   * Returns the maximum size in bytes of the write buffer for this HTable.
   * <p>
   * The default value comes from the configuration parameter
   * {@code hbase.client.write.buffer}.
   * @return The size of the write buffer in bytes.
   */
  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * Sets the size of the buffer in bytes.
   * <p>
   * If the new size is less than the current amount of data in the
   * write buffer, the buffer gets flushed.
   * @param writeBufferSize The new write buffer size, in bytes.
   * @throws IOException if a remote or network exception occurs.
   */
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    this.writeBufferSize = writeBufferSize;
    if(currentWriteBufferSize > writeBufferSize) {
      flushCommits();
    }
  }

  /**
   * Returns the write buffer.
   * @return The current write buffer.
   */
  public ArrayList<Put> getWriteBuffer() {
    return writeBuffer;
  }

  /**
   * The pool is used for mutli requests for this HTable
   * @return the pool used for mutli
   */
  ExecutorService getPool() {
    return this.pool;
  }

  /**
   * Enable or disable region cache prefetch for the table. It will be
   * applied for the given table's all HTable instances who share the same
   * connection. By default, the cache prefetch is enabled.
   * @param tableName name of table to configure.
   * @param enable Set to true to enable region cache prefetch. Or set to
   * false to disable it.
   * @throws IOException
   */
  public static void setRegionCachePrefetch(final byte[] tableName,
      final boolean enable) throws IOException {
    HConnectionManager.execute(new HConnectable<Void>(HBaseConfiguration
        .create()) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        connection.setRegionCachePrefetch(tableName, enable);
        return null;
      }
    });
  }

  /**
   * Enable or disable region cache prefetch for the table. It will be
   * applied for the given table's all HTable instances who share the same
   * connection. By default, the cache prefetch is enabled.
   * @param conf The Configuration object to use.
   * @param tableName name of table to configure.
   * @param enable Set to true to enable region cache prefetch. Or set to
   * false to disable it.
   * @throws IOException
   */
  public static void setRegionCachePrefetch(final Configuration conf,
      final byte[] tableName, final boolean enable) throws IOException {
    HConnectionManager.execute(new HConnectable<Void>(conf) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        connection.setRegionCachePrefetch(tableName, enable);
        return null;
      }
    });
  }

  /**
   * Check whether region cache prefetch is enabled or not for the table.
   * @param conf The Configuration object to use.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   * @throws IOException
   */
  public static boolean getRegionCachePrefetch(final Configuration conf,
      final byte[] tableName) throws IOException {
    return HConnectionManager.execute(new HConnectable<Boolean>(conf) {
      @Override
      public Boolean connect(HConnection connection) throws IOException {
        return connection.getRegionCachePrefetch(tableName);
      }
    });
  }

  /**
   * Check whether region cache prefetch is enabled or not for the table.
   * @param tableName name of table to check
   * @return true if table's region cache prefecth is enabled. Otherwise
   * it is disabled.
   * @throws IOException
   */
  public static boolean getRegionCachePrefetch(final byte[] tableName) throws IOException {
    return HConnectionManager.execute(new HConnectable<Boolean>(
        HBaseConfiguration.create()) {
      @Override
      public Boolean connect(HConnection connection) throws IOException {
        return connection.getRegionCachePrefetch(tableName);
      }
    });
 }

  /**
   * Explicitly clears the region cache to fetch the latest value from META.
   * This is a power user function: avoid unless you know the ramifications.
   */
  public void clearRegionCache() {
    this.connection.clearRegionCache();
  }

  /**
   * {@inheritDoc}
   */
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    return new RegionCoprocessorRpcChannel(connection, tableName, row);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends Service, R> Map<byte[],R> coprocessorService(final Class<T> service,
      byte[] startKey, byte[] endKey, final Batch.Call<T,R> callable)
      throws ServiceException, Throwable {
    final Map<byte[],R> results =  Collections.synchronizedMap(
        new TreeMap<byte[], R>(Bytes.BYTES_COMPARATOR));
    coprocessorService(service, startKey, endKey, callable, new Batch.Callback<R>() {
      public void update(byte[] region, byte[] row, R value) {
        results.put(region, value);
      }
    });
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends Service, R> void coprocessorService(final Class<T> service,
      byte[] startKey, byte[] endKey, final Batch.Call<T,R> callable,
      final Batch.Callback<R> callback) throws ServiceException, Throwable {

    // get regions covered by the row range
    List<byte[]> keys = getStartKeysInRange(startKey, endKey);

    Map<byte[],Future<R>> futures =
        new TreeMap<byte[],Future<R>>(Bytes.BYTES_COMPARATOR);
    for (final byte[] r : keys) {
      final RegionCoprocessorRpcChannel channel =
          new RegionCoprocessorRpcChannel(connection, tableName, r);
      Future<R> future = pool.submit(
          new Callable<R>() {
            public R call() throws Exception {
              T instance = ProtobufUtil.newServiceStub(service, channel);
              R result = callable.call(instance);
              byte[] region = channel.getLastRegion();
              if (callback != null) {
                callback.update(region, r, result);
              }
              return result;
            }
          });
      futures.put(r, future);
    }
    for (Map.Entry<byte[],Future<R>> e : futures.entrySet()) {
      try {
        e.getValue().get();
      } catch (ExecutionException ee) {
        LOG.warn("Error calling coprocessor service " + service.getName() + " for row "
            + Bytes.toStringBinary(e.getKey()), ee);
        throw ee.getCause();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException("Interrupted calling coprocessor service " + service.getName()
            + " for row " + Bytes.toStringBinary(e.getKey()))
            .initCause(ie);
      }
    }
  }

  private List<byte[]> getStartKeysInRange(byte[] start, byte[] end)
  throws IOException {
    Pair<byte[][],byte[][]> startEndKeys = getStartEndKeys();
    byte[][] startKeys = startEndKeys.getFirst();
    byte[][] endKeys = startEndKeys.getSecond();

    if (start == null) {
      start = HConstants.EMPTY_START_ROW;
    }
    if (end == null) {
      end = HConstants.EMPTY_END_ROW;
    }

    List<byte[]> rangeKeys = new ArrayList<byte[]>();
    for (int i=0; i<startKeys.length; i++) {
      if (Bytes.compareTo(start, startKeys[i]) >= 0 ) {
        if (Bytes.equals(endKeys[i], HConstants.EMPTY_END_ROW) ||
            Bytes.compareTo(start, endKeys[i]) < 0) {
          rangeKeys.add(start);
        }
      } else if (Bytes.equals(end, HConstants.EMPTY_END_ROW) ||
          Bytes.compareTo(startKeys[i], end) <= 0) {
        rangeKeys.add(startKeys[i]);
      } else {
        break; // past stop
      }
    }

    return rangeKeys;
  }

  public void setOperationTimeout(int operationTimeout) {
    this.operationTimeout = operationTimeout;
  }

  public int getOperationTimeout() {
    return operationTimeout;
  }

}
