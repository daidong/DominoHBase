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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.handler.SplitRegionHandler;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ServiceException;

/**
 * Like {@link TestSplitTransaction} in that we're testing {@link SplitTransaction}
 * only the below tests are against a running cluster where {@link TestSplitTransaction}
 * is tests against a bare {@link HRegion}.
 */
@Category(LargeTests.class)
public class TestSplitTransactionOnCluster {
  private static final Log LOG =
    LogFactory.getLog(TestSplitTransactionOnCluster.class);
  private HBaseAdmin admin = null;
  private MiniHBaseCluster cluster = null;
  private static final int NB_SERVERS = 2;
  private static CountDownLatch latch = new CountDownLatch(1);
  private static boolean secondSplit = false;
  private static boolean callRollBack = false;
  private static boolean firstSplitCompleted = false;

  private static final HBaseTestingUtility TESTING_UTIL =
    new HBaseTestingUtility();

  @BeforeClass public static void before() throws Exception {
    TESTING_UTIL.getConfiguration().setInt("hbase.balancer.period", 60000);
    // Needed because some tests have splits happening on RS that are killed
    // We don't want to wait 3min for the master to figure it out
    TESTING_UTIL.getConfiguration().setInt(
        "hbase.master.assignment.timeoutmonitor.timeout", 4000);
    TESTING_UTIL.startMiniCluster(NB_SERVERS);
  }

  @AfterClass public static void after() throws Exception {
    TESTING_UTIL.shutdownMiniCluster();
  }

  @Before public void setup() throws IOException {
    TESTING_UTIL.ensureSomeNonStoppedRegionServersAvailable(NB_SERVERS);
    this.admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());
    this.cluster = TESTING_UTIL.getMiniHBaseCluster();
  }

  private HRegionInfo getAndCheckSingleTableRegion(final List<HRegion> regions) {
    assertEquals(1, regions.size());
    return regions.get(0).getRegionInfo();
  }

  /**
   * A test that intentionally has master fail the processing of the split message.
   * Tests that the regionserver split ephemeral node gets cleaned up if it
   * crashes and that after we process server shutdown, the daughters are up on
   * line.
   * @throws IOException
   * @throws InterruptedException
   * @throws NodeExistsException
   * @throws KeeperException
   * @throws DeserializationException 
   */
  @Test (timeout = 300000) public void testRSSplitEphemeralsDisappearButDaughtersAreOnlinedAfterShutdownHandling()
  throws IOException, InterruptedException, NodeExistsException, KeeperException, DeserializationException, ServiceException {
    final byte [] tableName =
      Bytes.toBytes("ephemeral");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = ProtobufUtil.getOnlineRegions(server).size();
      // Now, before we split, set special flag in master, a flag that has
      // it FAIL the processing of split.
      SplitRegionHandler.TEST_SKIP = true;
      // Now try splitting and it should work.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = checkAndGetDaughters(tableName);
      // Assert the ephemeral node is up in zk.
      String path = ZKAssign.getNodeName(TESTING_UTIL.getZooKeeperWatcher(),
        hri.getEncodedName());
      Stat stats =
        TESTING_UTIL.getZooKeeperWatcher().getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats=" + stats);
      RegionTransition rt =
        RegionTransition.parseFrom(ZKAssign.getData(TESTING_UTIL.getZooKeeperWatcher(),
          hri.getEncodedName()));
      // State could be SPLIT or SPLITTING.
      assertTrue(rt.getEventType().equals(EventType.RS_ZK_REGION_SPLIT) ||
        rt.getEventType().equals(EventType.RS_ZK_REGION_SPLITTING));
      // Now crash the server
      cluster.abortRegionServer(tableRegionIndex);
      waitUntilRegionServerDead();
      awaitDaughters(tableName, daughters.size());

      // Assert daughters are online.
      regions = cluster.getRegions(tableName);
      for (HRegion r: regions) {
        assertTrue(daughters.contains(r));
      }
      // Finally assert that the ephemeral SPLIT znode was cleaned up.
      for (int i=0; i<100; i++) {
        // wait a bit (10s max) for the node to disappear
        stats = TESTING_UTIL.getZooKeeperWatcher().getRecoverableZooKeeper().exists(path, false);
        if (stats == null) break;
        Thread.sleep(100);
      }
      LOG.info("EPHEMERAL NODE AFTER SERVER ABORT, path=" + path + ", stats=" + stats);
      assertTrue(stats == null);
    } finally {
      // Set this flag back.
      SplitRegionHandler.TEST_SKIP = false;
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  @Test (timeout = 300000) public void testExistingZnodeBlocksSplitAndWeRollback()
  throws IOException, InterruptedException, NodeExistsException, KeeperException, ServiceException {
    final byte [] tableName =
      Bytes.toBytes("testExistingZnodeBlocksSplitAndWeRollback");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = ProtobufUtil.getOnlineRegions(server).size();
      // Insert into zk a blocking znode, a znode of same name as region
      // so it gets in way of our splitting.
      ZKAssign.createNodeClosing(TESTING_UTIL.getZooKeeperWatcher(),
        hri, new ServerName("any.old.server", 1234, -1));
      // Now try splitting.... should fail.  And each should successfully
      // rollback.
      this.admin.split(hri.getRegionNameAsString());
      this.admin.split(hri.getRegionNameAsString());
      this.admin.split(hri.getRegionNameAsString());
      // Wait around a while and assert count of regions remains constant.
      for (int i = 0; i < 10; i++) {
        Thread.sleep(100);
        assertEquals(regionCount, ProtobufUtil.getOnlineRegions(server).size());
      }
      // Now clear the zknode
      ZKAssign.deleteClosingNode(TESTING_UTIL.getZooKeeperWatcher(), hri);
      // Now try splitting and it should work.
      split(hri, server, regionCount);
      // Get daughters
      checkAndGetDaughters(tableName);
      // OK, so split happened after we cleared the blocking node.
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  /**
   * Messy test that simulates case where SplitTransactions fails to add one
   * of the daughters up into the .META. table before crash.  We're testing
   * fact that the shutdown handler will fixup the missing daughter region
   * adding it back into .META.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout = 300000) public void testShutdownSimpleFixup()
  throws IOException, InterruptedException, ServiceException {
    final byte [] tableName = Bytes.toBytes("testShutdownSimpleFixup");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = ProtobufUtil.getOnlineRegions(server).size();
      // Now split.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = checkAndGetDaughters(tableName);
      // Remove one of the daughters from .META. to simulate failed insert of
      // daughter region up into .META.
      removeDaughterFromMeta(daughters.get(0).getRegionName());
      // Now crash the server
      cluster.abortRegionServer(tableRegionIndex);
      waitUntilRegionServerDead();
      awaitDaughters(tableName, daughters.size());
      // Assert daughters are online.
      regions = cluster.getRegions(tableName);
      for (HRegion r: regions) {
        assertTrue(daughters.contains(r));
      }
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  /**
   * Test that if daughter split on us, we won't do the shutdown handler fixup
   * just because we can't find the immediate daughter of an offlined parent.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=300000) public void testShutdownFixupWhenDaughterHasSplit()
  throws IOException, InterruptedException, ServiceException {
    final byte [] tableName =
      Bytes.toBytes("testShutdownFixupWhenDaughterHasSplit");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = ProtobufUtil.getOnlineRegions(server).size();
      // Now split.
      split(hri, server, regionCount);
      // Get daughters
      List<HRegion> daughters = checkAndGetDaughters(tableName);
      // Now split one of the daughters.
      regionCount = ProtobufUtil.getOnlineRegions(server).size();
      HRegionInfo daughter = daughters.get(0).getRegionInfo();
      // Compact first to ensure we have cleaned up references -- else the split
      // will fail.
      this.admin.compact(daughter.getRegionName());
      daughters = cluster.getRegions(tableName);
      HRegion daughterRegion = null;
      for (HRegion r: daughters) {
        if (r.getRegionInfo().equals(daughter)) daughterRegion = r;
      }
      assertTrue(daughterRegion != null);
      for (int i=0; i<100; i++) {
        if (!daughterRegion.hasReferences()) break;
        Threads.sleep(100);
      }
      assertFalse("Waiting for refereces to be compacted", daughterRegion.hasReferences());
      split(daughter, server, regionCount);
      // Get list of daughters
      daughters = cluster.getRegions(tableName);
      // Now crash the server
      cluster.abortRegionServer(tableRegionIndex);
      waitUntilRegionServerDead();
      awaitDaughters(tableName, daughters.size());
      // Assert daughters are online and ONLY the original daughters -- that
      // fixup didn't insert one during server shutdown recover.
      regions = cluster.getRegions(tableName);
      assertEquals(daughters.size(), regions.size());
      for (HRegion r: regions) {
        assertTrue(daughters.contains(r));
      }
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }
  
  /**
   * Verifies HBASE-5806.  When splitting is partially done and the master goes down
   * when the SPLIT node is in either SPLIT or SPLITTING state.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws NodeExistsException
   * @throws KeeperException
   * @throws DeserializationException 
   */
  @Test(timeout = 300000)
  public void testMasterRestartWhenSplittingIsPartial()
      throws IOException, InterruptedException, NodeExistsException,
      KeeperException, DeserializationException, ServiceException {
    final byte[] tableName = Bytes.toBytes("testMasterRestartWhenSplittingIsPartial");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      // Now, before we split, set special flag in master, a flag that has
      // it FAIL the processing of split.
      SplitRegionHandler.TEST_SKIP = true;
      // Now try splitting and it should work.
      
      this.admin.split(hri.getRegionNameAsString());
      checkAndGetDaughters(tableName);
      // Assert the ephemeral node is up in zk.
      String path = ZKAssign.getNodeName(t.getConnection()
          .getZooKeeperWatcher(), hri.getEncodedName());
      Stat stats = t.getConnection().getZooKeeperWatcher()
          .getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats="
          + stats);
      byte[] bytes = ZKAssign.getData(t.getConnection()
          .getZooKeeperWatcher(), hri.getEncodedName());
      RegionTransition rtd = RegionTransition.parseFrom(bytes);
      // State could be SPLIT or SPLITTING.
      assertTrue(rtd.getEventType().equals(EventType.RS_ZK_REGION_SPLIT)
          || rtd.getEventType().equals(EventType.RS_ZK_REGION_SPLITTING));

      // abort and wait for new master.
      MockMasterWithoutCatalogJanitor master = abortAndWaitForMaster();

      this.admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());

      // update the hri to be offlined and splitted. 
      hri.setOffline(true);
      hri.setSplit(true);
      ServerName regionServerOfRegion = master.getAssignmentManager()
        .getRegionStates().getRegionServerOfRegion(hri);
      assertTrue(regionServerOfRegion != null);

    } finally {
      // Set this flag back.
      SplitRegionHandler.TEST_SKIP = false;
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  /**
   * Verifies HBASE-5806.  Here the case is that splitting is completed but before the
   * CJ could remove the parent region the master is killed and restarted.
   * @throws IOException
   * @throws InterruptedException
   * @throws NodeExistsException
   * @throws KeeperException
   */
  @Test (timeout = 300000)
  public void testMasterRestartAtRegionSplitPendingCatalogJanitor()
      throws IOException, InterruptedException, NodeExistsException,
      KeeperException, ServiceException {
    final byte[] tableName = Bytes.toBytes("testMasterRestartAtRegionSplitPendingCatalogJanitor");

    // Create table then get the single region for our new table.
    HTable t = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);

    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      
      this.admin.split(hri.getRegionNameAsString());
      checkAndGetDaughters(tableName);
      // Assert the ephemeral node is up in zk.
      String path = ZKAssign.getNodeName(t.getConnection()
          .getZooKeeperWatcher(), hri.getEncodedName());
      Stat stats = t.getConnection().getZooKeeperWatcher()
          .getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats="
          + stats);
      String node = ZKAssign.getNodeName(t.getConnection()
          .getZooKeeperWatcher(), hri.getEncodedName());
      Stat stat = new Stat();
      byte[] data = ZKUtil.getDataNoWatch(t.getConnection()
          .getZooKeeperWatcher(), node, stat);
      // ZKUtil.create
      for (int i=0; data != null && i<60; i++) {
        Thread.sleep(1000);
        data = ZKUtil.getDataNoWatch(t.getConnection().getZooKeeperWatcher(),
            node, stat);

      }
      assertNull("Waited too long for ZK node to be removed: "+node, data);

      MockMasterWithoutCatalogJanitor master = abortAndWaitForMaster();

      this.admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());

      hri.setOffline(true);
      hri.setSplit(true);
      ServerName regionServerOfRegion = master.getAssignmentManager()
        .getRegionStates().getRegionServerOfRegion(hri);
      assertTrue(regionServerOfRegion == null);
    } finally {
      // Set this flag back.
      SplitRegionHandler.TEST_SKIP = false;
      this.admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  /**
   * 
   * While transitioning node from RS_ZK_REGION_SPLITTING to
   * RS_ZK_REGION_SPLITTING during region split,if zookeper went down split always
   * fails for the region. HBASE-6088 fixes this scenario. 
   * This test case is to test the znode is deleted(if created) or not in roll back.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  @Test
  public void testSplitBeforeSettingSplittingInZK() throws Exception,
      InterruptedException, KeeperException {
    testSplitBeforeSettingSplittingInZKInternals();
  }
  
  @Test(timeout = 20000)
  public void testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack() throws Exception {
    final byte[] tableName = Bytes
        .toBytes("testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack");
    HBaseAdmin admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());
    try {
      // Create table then get the single region for our new table.
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(htd);
      HTable t = new HTable(cluster.getConfiguration(), tableName);
      // wait for up to 10s
      for (int i=0; cluster.getRegions(tableName).size() != 1 && i<100; i++) {
        Thread.sleep(100);
      }
      assertTrue("waited too long for table to get online",
          cluster.getRegions(tableName).size() == 1);
      final List<HRegion> regions = cluster.getRegions(tableName);
      HRegionInfo hri = getAndCheckSingleTableRegion(regions);
      int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionName());
      final HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
      insertData(tableName, admin, t);
      // Turn off balancer so it doesn't cut in and mess up our placements.
      this.admin.setBalancerRunning(false, false);
      // Turn off the meta scanner so it don't remove parent on us.
      cluster.getMaster().setCatalogJanitorEnabled(false);

      new Thread() {
        public void run() {
          SplitTransaction st = null;
          st = new MockedSplitTransaction(regions.get(0), Bytes.toBytes("row2"));
          try {
            st.prepare();
            st.execute(regionServer, regionServer);
          } catch (IOException e) {

          }
        }
      }.start();
      for (int i=0; !callRollBack && i<100; i++) {
        Thread.sleep(100);
      }
      assertTrue("Waited too long for rollback", callRollBack);
      SplitTransaction st = null;
      st = new MockedSplitTransaction(regions.get(0), Bytes.toBytes("row2"));
      try {
        secondSplit = true;
        st.prepare();
        st.execute(regionServer, regionServer);
      } catch (IOException e) {
        LOG.debug("Rollback started :"+ e.getMessage());
        st.rollback(regionServer, regionServer);
      }
      for (int i=0; !firstSplitCompleted && i<100; i++) {
        Thread.sleep(100);
      }
      assertTrue("fist split did not complete", firstSplitCompleted);

      RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();
      Map<String, RegionState> rit = regionStates.getRegionsInTransition();
      
      for (int i=0; rit.containsKey(hri.getTableNameAsString()) && i<100; i++) {
        Thread.sleep(100);
      }
      assertFalse("region still in transition", rit.containsKey(rit.containsKey(hri.getTableNameAsString())));

      List<HRegion> onlineRegions = regionServer.getOnlineRegions(tableName);
      // Region server side split is successful.
      assertEquals("The parent region should be splitted", 2, onlineRegions.size());
      //Should be present in RIT
      List<HRegionInfo> regionsOfTable = cluster.getMaster().getAssignmentManager()
          .getRegionStates().getRegionsOfTable(tableName);
      // Master side should also reflect the same
      assertEquals("No of regions in master", 2, regionsOfTable.size());
    } finally {
      admin.setBalancerRunning(true, false);
      secondSplit = false;
      firstSplitCompleted = false;
      callRollBack = false;
      cluster.getMaster().setCatalogJanitorEnabled(true);
      if (admin.isTableAvailable(tableName) && admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        admin.close();
      }
    }
  }

  @Test(timeout = 20000)
  public void testTableExistsIfTheSpecifiedTableRegionIsSplitParent() throws Exception {
    final byte[] tableName = 
        Bytes.toBytes("testTableExistsIfTheSpecifiedTableRegionIsSplitParent");
    HRegionServer regionServer = null;
    List<HRegion> regions = null;
    HBaseAdmin admin = new HBaseAdmin(TESTING_UTIL.getConfiguration());
    try {
      // Create table then get the single region for our new table.
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(htd);
      HTable t = new HTable(cluster.getConfiguration(), tableName);
      regions = cluster.getRegions(tableName);
      int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionName());
      regionServer = cluster.getRegionServer(regionServerIndex);
      insertData(tableName, admin, t);
      // Turn off balancer so it doesn't cut in and mess up our placements.
      admin.setBalancerRunning(false, false);
      // Turn off the meta scanner so it don't remove parent on us.
      cluster.getMaster().setCatalogJanitorEnabled(false);
      boolean tableExists = MetaReader.tableExists(regionServer.getCatalogTracker(),
          Bytes.toString(tableName));
      assertEquals("The specified table should present.", true, tableExists);
      SplitTransaction st = new SplitTransaction(regions.get(0), Bytes.toBytes("row2"));
      try {
        st.prepare();
        st.createDaughters(regionServer, regionServer);
      } catch (IOException e) {

      }
      tableExists = MetaReader.tableExists(regionServer.getCatalogTracker(),
          Bytes.toString(tableName));
      assertEquals("The specified table should present.", true, tableExists);
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      admin.close();
    }
  }

  private void insertData(final byte[] tableName, HBaseAdmin admin, HTable t) throws IOException,
      InterruptedException {
    Put p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("1"));
    t.put(p);
    p = new Put(Bytes.toBytes("row2"));
    p.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("2"));
    t.put(p);
    p = new Put(Bytes.toBytes("row3"));
    p.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("3"));
    t.put(p);
    p = new Put(Bytes.toBytes("row4"));
    p.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("4"));
    t.put(p);
    admin.flush(tableName);
  }
  
  
  @Test
  public void testShouldThrowIOExceptionIfStoreFileSizeIsEmptyAndSHouldSuccessfullyExecuteRollback()
      throws Exception {
    final byte[] tableName = Bytes.toBytes("testRollBackShudBeSuccessfulIfStoreFileIsEmpty");
    // Create table then get the single region for our new table.
    TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);
    List<HRegion> regions = cluster.getRegions(tableName);
    HRegionInfo hri = getAndCheckSingleTableRegion(regions);
    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);
    int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionName());
    HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.setBalancerRunning(false, false);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      // Now split.
      SplitTransaction st = null;
      st = new MockedSplitTransaction(regions.get(0), null);
      try {
        st.execute(regionServer, regionServer);
      } catch (IOException e) {
        List<HRegion> daughters = cluster.getRegions(tableName);
        assertTrue(daughters.size() == 1);

        String node = ZKAssign.getNodeName(regionServer.getZooKeeper(), regions.get(0)
            .getRegionInfo().getEncodedName());
        assertFalse(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
        assertTrue(st.rollback(regionServer, regionServer));
        assertTrue(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
      }
    } finally {
      admin.setBalancerRunning(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }

  }

  private void testSplitBeforeSettingSplittingInZKInternals() throws Exception {
    final byte[] tableName = Bytes.toBytes("testSplitBeforeSettingSplittingInZK");
    HBaseAdmin admin = TESTING_UTIL.getHBaseAdmin();
    try {
      // Create table then get the single region for our new table.
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(htd);

      List<HRegion> regions = null;
      for (int i=0; i<100; i++) {
        regions = cluster.getRegions(tableName);
        if (regions.size() > 0) break;
        Thread.sleep(100);
      }
      int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
      SplitTransaction st = null;
      {
        st = new MockedSplitTransaction(regions.get(0), null) {
          @Override
          int createNodeSplitting(ZooKeeperWatcher zkw, HRegionInfo region, ServerName serverName)
              throws KeeperException, IOException {
            throw new IOException();
          }
        };
      }
      try {
        st.execute(regionServer, regionServer);
      } catch (IOException e) {
        String node = ZKAssign.getNodeName(regionServer.getZooKeeper(), regions.get(0)
            .getRegionInfo().getEncodedName());
        {
          assertTrue(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
        }
        assertTrue(st.rollback(regionServer, regionServer));
        assertTrue(ZKUtil.checkExists(regionServer.getZooKeeper(), node) == -1);
      }
    } finally {
      if (admin.isTableAvailable(tableName) && admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }
    }
  }
  
  public static class MockedSplitTransaction extends SplitTransaction {

    private HRegion currentRegion;
    public MockedSplitTransaction(HRegion r, byte[] splitrow) {
      super(r, splitrow);
      this.currentRegion = r;
    }
    
    @Override
    void transitionZKNode(Server server, RegionServerServices services, HRegion a, HRegion b)
        throws IOException {
      if (this.currentRegion.getRegionInfo().getTableNameAsString()
          .equals("testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack")) {
        try {
          if (!secondSplit){
            callRollBack = true;
            latch.await();
          }
        } catch (InterruptedException e) {
        }
       
      }
      super.transitionZKNode(server, services, a, b);
      if (this.currentRegion.getRegionInfo().getTableNameAsString()
          .equals("testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack")) {
        firstSplitCompleted = true;
      }
    }
    @Override
    public boolean rollback(Server server, RegionServerServices services) throws IOException {
      if (this.currentRegion.getRegionInfo().getTableNameAsString()
          .equals("testShouldFailSplitIfZNodeDoesNotExistDueToPrevRollBack")) {
        if(secondSplit){
          super.rollback(server, services);
          latch.countDown();
          return true;
        }
      }
      return super.rollback(server, services);
    }

  }

  private List<HRegion> checkAndGetDaughters(byte[] tableName)
      throws InterruptedException {    
    List<HRegion> daughters = null;
    // try up to 10s
    for (int i=0; i<100; i++) {
      daughters = cluster.getRegions(tableName);
      if (daughters.size() >= 2) break;
      Thread.sleep(100);
    }
    assertTrue(daughters.size() >= 2);
    return daughters;
  }

  private MockMasterWithoutCatalogJanitor abortAndWaitForMaster() 
  throws IOException, InterruptedException {
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    cluster.getConfiguration().setClass(HConstants.MASTER_IMPL, 
    		MockMasterWithoutCatalogJanitor.class, HMaster.class);
    MockMasterWithoutCatalogJanitor master = null;
    master = (MockMasterWithoutCatalogJanitor) cluster.startMaster().getMaster();
    cluster.waitForActiveAndReadyMaster();
    return master;
  }

  private void split(final HRegionInfo hri, final HRegionServer server,
      final int regionCount)
  throws IOException, InterruptedException {
    this.admin.split(hri.getRegionNameAsString());
    for (int i = 0; ProtobufUtil.getOnlineRegions(server).size() <= regionCount && i < 100; i++) {
      LOG.debug("Waiting on region to split");
      Thread.sleep(100);
    }
    assertFalse("Waited too long for split",
        ProtobufUtil.getOnlineRegions(server).size() <= regionCount);
  }

  private void removeDaughterFromMeta(final byte [] regionName) throws IOException {
    HTable metaTable =
      new HTable(TESTING_UTIL.getConfiguration(), HConstants.META_TABLE_NAME);
    Delete d = new Delete(regionName);
    LOG.info("Deleted " + Bytes.toString(regionName));
    metaTable.delete(d);
  }

  /**
   * Ensure single table region is not on same server as the single .META. table
   * region.
   * @param admin
   * @param hri
   * @return Index of the server hosting the single table region
   * @throws UnknownRegionException
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   * @throws InterruptedException
   */
  private int ensureTableRegionNotOnSameServerAsMeta(final HBaseAdmin admin,
      final HRegionInfo hri)
  throws UnknownRegionException, MasterNotRunningException,
  ZooKeeperConnectionException, InterruptedException {
    MiniHBaseCluster cluster = TESTING_UTIL.getMiniHBaseCluster();
    // Now make sure that the table region is not on same server as that hosting
    // .META.  We don't want .META. replay polluting our test when we later crash
    // the table region serving server.
    int metaServerIndex = cluster.getServerWithMeta();
    assertTrue(metaServerIndex != -1);
    HRegionServer metaRegionServer = cluster.getRegionServer(metaServerIndex);
    int tableRegionIndex = cluster.getServerWith(hri.getRegionName());
    assertTrue(tableRegionIndex != -1);
    HRegionServer tableRegionServer = cluster.getRegionServer(tableRegionIndex);
    if (metaRegionServer.getServerName().equals(tableRegionServer.getServerName())) {
      HRegionServer hrs = getOtherRegionServer(cluster, metaRegionServer);
      assertNotNull(hrs);
      assertNotNull(hri);
      LOG.
        info("Moving " + hri.getRegionNameAsString() + " to " +
        hrs.getServerName() + "; metaServerIndex=" + metaServerIndex);
      admin.move(hri.getEncodedNameAsBytes(),
        Bytes.toBytes(hrs.getServerName().toString()));
    }
    // Wait till table region is up on the server that is NOT carrying .META..
    for (int i=0; i<100; i++) {
      tableRegionIndex = cluster.getServerWith(hri.getRegionName());
      if (tableRegionIndex != -1 && tableRegionIndex != metaServerIndex) break;
      LOG.debug("Waiting on region move off the .META. server; current index " +
        tableRegionIndex + " and metaServerIndex=" + metaServerIndex);
      Thread.sleep(100);
    }
    assertTrue("Region not moved off .META. server", tableRegionIndex != -1
        && tableRegionIndex != metaServerIndex);
    // Verify for sure table region is not on same server as .META.
    tableRegionIndex = cluster.getServerWith(hri.getRegionName());
    assertTrue(tableRegionIndex != -1);
    assertNotSame(metaServerIndex, tableRegionIndex);
    return tableRegionIndex;
  }

  /**
   * Find regionserver other than the one passed.
   * Can't rely on indexes into list of regionservers since crashed servers
   * occupy an index.
   * @param cluster
   * @param notThisOne
   * @return A regionserver that is not <code>notThisOne</code> or null if none
   * found
   */
  private HRegionServer getOtherRegionServer(final MiniHBaseCluster cluster,
      final HRegionServer notThisOne) {
    for (RegionServerThread rst: cluster.getRegionServerThreads()) {
      HRegionServer hrs = rst.getRegionServer();
      if (hrs.getServerName().equals(notThisOne.getServerName())) continue;
      if (hrs.isStopping() || hrs.isStopped()) continue;
      return hrs;
    }
    return null;
  }

  private void printOutRegions(final HRegionServer hrs, final String prefix)
      throws IOException {
    List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs);
    for (HRegionInfo region: regions) {
      LOG.info(prefix + region.getRegionNameAsString());
    }
  }

  private void waitUntilRegionServerDead() throws InterruptedException {
    // Wait until the master processes the RS shutdown
    for (int i=0; cluster.getMaster().getClusterStatus().
        getServers().size() == NB_SERVERS && i<100; i++) {
      LOG.info("Waiting on server to go down");
      Thread.sleep(100);
    }
    assertFalse("Waited too long for RS to die", cluster.getMaster().getClusterStatus().
        getServers().size() == NB_SERVERS);
  }

  private void awaitDaughters(byte[] tableName, int numDaughters) throws InterruptedException {
    // Wait till regions are back on line again.
    for (int i=0; cluster.getRegions(tableName).size() < numDaughters && i<60; i++) {
      LOG.info("Waiting for repair to happen");
      Thread.sleep(1000);
    }
    if (cluster.getRegions(tableName).size() < numDaughters) {
      fail("Waiting too long for daughter regions");
    }
  }
  
  public static class MockMasterWithoutCatalogJanitor extends HMaster {

    public MockMasterWithoutCatalogJanitor(Configuration conf) throws IOException, KeeperException,
        InterruptedException {
      super(conf);
    }

    protected void startCatalogJanitorChore() {
      LOG.debug("Customised master executed.");
    }
  }

}

