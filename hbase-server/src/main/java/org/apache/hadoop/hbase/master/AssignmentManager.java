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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.LinkedHashMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.handler.ClosedRegionHandler;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.master.handler.OpenedRegionHandler;
import org.apache.hadoop.hbase.master.handler.SplitRegionHandler;
import org.apache.hadoop.hbase.regionserver.RegionAlreadyInTransitionException;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

/**
 * Manages and performs region assignment.
 * <p>
 * Monitors ZooKeeper for events related to regions in transition.
 * <p>
 * Handles existing regions in transition during master failover.
 */
@InterfaceAudience.Private
public class AssignmentManager extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(AssignmentManager.class);

  public static final ServerName HBCK_CODE_SERVERNAME = new ServerName(HConstants.HBCK_CODE_NAME,
      -1, -1L);

  protected final Server server;

  private ServerManager serverManager;

  private CatalogTracker catalogTracker;

  final TimeoutMonitor timeoutMonitor;

  private TimerUpdater timerUpdater;

  private LoadBalancer balancer;

  final private KeyLocker<String> locker = new KeyLocker<String>();

  /**
   * Map of regions to reopen after the schema of a table is changed. Key -
   * encoded region name, value - HRegionInfo
   */
  private final Map <String, HRegionInfo> regionsToReopen;

  /*
   * Maximum times we recurse an assignment/unassignment.
   * See below in {@link #assign()} and {@link #unassign()}.
   */
  private final int maximumAttempts;

  /** Plans for region movement. Key is the encoded version of a region name*/
  // TODO: When do plans get cleaned out?  Ever? In server open and in server
  // shutdown processing -- St.Ack
  // All access to this Map must be synchronized.
  final NavigableMap<String, RegionPlan> regionPlans =
    new TreeMap<String, RegionPlan>();

  private final ZKTable zkTable;

  /**
   * Contains the server which need to update timer, these servers will be
   * handled by {@link TimerUpdater}
   */
  private final ConcurrentSkipListSet<ServerName> serversInUpdatingTimer =
    new ConcurrentSkipListSet<ServerName>();

  private final ExecutorService executorService;

  //Thread pool executor service for timeout monitor
  private java.util.concurrent.ExecutorService threadPoolExecutorService;

  // A bunch of ZK events workers. Each is a single thread executor service
  private final java.util.concurrent.ExecutorService zkEventWorkers;

  private List<EventType> ignoreStatesRSOffline = Arrays.asList(
      EventType.RS_ZK_REGION_FAILED_OPEN, EventType.RS_ZK_REGION_CLOSED);

  // metrics instance to send metrics for RITs
  MetricsMaster metricsMaster;

  private final RegionStates regionStates;

  /**
   * Indicator that AssignmentManager has recovered the region states so
   * that ServerShutdownHandler can be fully enabled and re-assign regions
   * of dead servers. So that when re-assignment happens, AssignmentManager
   * has proper region states.
   *
   * Protected to ease testing.
   */
  protected final AtomicBoolean failoverCleanupDone = new AtomicBoolean(false);

  /**
   * Constructs a new assignment manager.
   *
   * @param server
   * @param serverManager
   * @param catalogTracker
   * @param service
   * @throws KeeperException
   * @throws IOException
   */
  public AssignmentManager(Server server, ServerManager serverManager,
      CatalogTracker catalogTracker, final LoadBalancer balancer,
      final ExecutorService service, MetricsMaster metricsMaster) throws KeeperException, IOException {
    super(server.getZooKeeper());
    this.server = server;
    this.serverManager = serverManager;
    this.catalogTracker = catalogTracker;
    this.executorService = service;
    this.regionsToReopen = Collections.synchronizedMap
                           (new HashMap<String, HRegionInfo> ());
    Configuration conf = server.getConfiguration();
    this.timeoutMonitor = new TimeoutMonitor(
      conf.getInt("hbase.master.assignment.timeoutmonitor.period", 30000),
      server, serverManager,
      conf.getInt("hbase.master.assignment.timeoutmonitor.timeout", 600000));
    this.timerUpdater = new TimerUpdater(conf.getInt(
      "hbase.master.assignment.timerupdater.period", 10000), server);
    Threads.setDaemonThreadRunning(timerUpdater.getThread(),
      server.getServerName() + ".timerUpdater");
    this.zkTable = new ZKTable(this.watcher);
    this.maximumAttempts =
      this.server.getConfiguration().getInt("hbase.assignment.maximum.attempts", 10);
    this.balancer = balancer;
    int maxThreads = conf.getInt("hbase.assignment.threads.max", 30);
    this.threadPoolExecutorService = Threads.getBoundedCachedThreadPool(
      maxThreads, 60L, TimeUnit.SECONDS, Threads.newDaemonThreadFactory("hbase-am"));
    this.metricsMaster = metricsMaster;// can be null only with tests.
    this.regionStates = new RegionStates(server, serverManager);

    int workers = conf.getInt("hbase.assignment.zkevent.workers", 20);
    ThreadFactory threadFactory = Threads.newDaemonThreadFactory("hbase-am-zkevent-worker");
    zkEventWorkers = Threads.getBoundedCachedThreadPool(workers, 60L,
            TimeUnit.SECONDS, threadFactory);
  }

  void startTimeOutMonitor() {
    Threads.setDaemonThreadRunning(timeoutMonitor.getThread(), server.getServerName()
        + ".timeoutMonitor");
  }

  /**
   * @return Instance of ZKTable.
   */
  public ZKTable getZKTable() {
    // These are 'expensive' to make involving trip to zk ensemble so allow
    // sharing.
    return this.zkTable;
  }

  /**
   * This SHOULD not be public. It is public now
   * because of some unit tests.
   *
   * TODO: make it package private and keep RegionStates in the master package
   */
  public RegionStates getRegionStates() {
    return regionStates;
  }

  public RegionPlan getRegionReopenPlan(HRegionInfo hri) {
    return new RegionPlan(hri, null, regionStates.getRegionServerOfRegion(hri));
  }

  /**
   * Add a regionPlan for the specified region.
   * @param encodedName
   * @param plan
   */
  public void addPlan(String encodedName, RegionPlan plan) {
    synchronized (regionPlans) {
      regionPlans.put(encodedName, plan);
    }
  }

  /**
   * Add a map of region plans.
   */
  public void addPlans(Map<String, RegionPlan> plans) {
    synchronized (regionPlans) {
      regionPlans.putAll(plans);
    }
  }

  /**
   * Set the list of regions that will be reopened
   * because of an update in table schema
   *
   * @param regions
   *          list of regions that should be tracked for reopen
   */
  public void setRegionsToReopen(List <HRegionInfo> regions) {
    for(HRegionInfo hri : regions) {
      regionsToReopen.put(hri.getEncodedName(), hri);
    }
  }

  /**
   * Used by the client to identify if all regions have the schema updates
   *
   * @param tableName
   * @return Pair indicating the status of the alter command
   * @throws IOException
   */
  public Pair<Integer, Integer> getReopenStatus(byte[] tableName)
      throws IOException {
    List <HRegionInfo> hris =
      MetaReader.getTableRegions(this.server.getCatalogTracker(), tableName);
    Integer pending = 0;
    for (HRegionInfo hri : hris) {
      String name = hri.getEncodedName();
      // no lock concurrent access ok: sequential consistency respected.
      if (regionsToReopen.containsKey(name)
          || regionStates.isRegionInTransition(name)) {
        pending++;
      }
    }
    return new Pair<Integer, Integer>(pending, hris.size());
  }

  /**
   * Used by ServerShutdownHandler to make sure AssignmentManager has completed
   * the failover cleanup before re-assigning regions of dead servers. So that
   * when re-assignment happens, AssignmentManager has proper region states.
   */
  public boolean isFailoverCleanupDone() {
    return failoverCleanupDone.get();
  }

  /**
   * Now, failover cleanup is completed. Notify server manager to
   * process queued up dead servers processing, if any.
   */
  void failoverCleanupDone() {
    failoverCleanupDone.set(true);
    serverManager.processQueuedDeadServers();
  }

  /**
   * Called on startup.
   * Figures whether a fresh cluster start of we are joining extant running cluster.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  void joinCluster() throws IOException,
      KeeperException, InterruptedException {
    // Concurrency note: In the below the accesses on regionsInTransition are
    // outside of a synchronization block where usually all accesses to RIT are
    // synchronized.  The presumption is that in this case it is safe since this
    // method is being played by a single thread on startup.

    // TODO: Regions that have a null location and are not in regionsInTransitions
    // need to be handled.

    // Scan META to build list of existing regions, servers, and assignment
    // Returns servers who have not checked in (assumed dead) and their regions
    Map<ServerName, List<HRegionInfo>> deadServers = rebuildUserRegions();

    // This method will assign all user regions if a clean server startup or
    // it will reconstruct master state and cleanup any leftovers from
    // previous master process.
    processDeadServersAndRegionsInTransition(deadServers);

    recoverTableInDisablingState();
    recoverTableInEnablingState();
  }

  /**
   * Process all regions that are in transition in zookeeper and also
   * processes the list of dead servers by scanning the META.
   * Used by master joining an cluster.  If we figure this is a clean cluster
   * startup, will assign all user regions.
   * @param deadServers
   *          Map of dead servers and their regions. Can be null.
   * @throws KeeperException
   * @throws IOException
   * @throws InterruptedException
   */
  void processDeadServersAndRegionsInTransition(
      final Map<ServerName, List<HRegionInfo>> deadServers)
          throws KeeperException, IOException, InterruptedException {
    List<String> nodes = ZKUtil.listChildrenNoWatch(watcher,
      watcher.assignmentZNode);

    if (nodes == null) {
      String errorMessage = "Failed to get the children from ZK";
      server.abort(errorMessage, new IOException(errorMessage));
      return;
    }

    boolean failover = !serverManager.getDeadServers().isEmpty();

    if (!failover) {
      // Run through all regions.  If they are not assigned and not in RIT, then
      // its a clean cluster startup, else its a failover.
      Map<HRegionInfo, ServerName> regions = regionStates.getRegionAssignments();
      for (Map.Entry<HRegionInfo, ServerName> e: regions.entrySet()) {
        if (!e.getKey().isMetaTable() && e.getValue() != null) {
          LOG.debug("Found " + e + " out on cluster");
          failover = true;
          break;
        }
        if (nodes.contains(e.getKey().getEncodedName())) {
          LOG.debug("Found " + e.getKey().getRegionNameAsString() + " in RITs");
          // Could be a meta region.
          failover = true;
          break;
        }
      }
    }

    // If we found user regions out on cluster, its a failover.
    if (failover) {
      LOG.info("Found regions out on cluster or in RIT; failover");
      // Process list of dead servers and regions in RIT.
      // See HBASE-4580 for more information.
      processDeadServersAndRecoverLostRegions(deadServers, nodes);
    } else {
      // Fresh cluster startup.
      LOG.info("Clean cluster startup. Assigning userregions");
      assignAllUserRegions();
    }
  }

  /**
   * If region is up in zk in transition, then do fixup and block and wait until
   * the region is assigned and out of transition.  Used on startup for
   * catalog regions.
   * @param hri Region to look for.
   * @return True if we processed a region in transition else false if region
   * was not up in zk in transition.
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   */
  boolean processRegionInTransitionAndBlockUntilAssigned(final HRegionInfo hri)
      throws InterruptedException, KeeperException, IOException {
    boolean intransistion = processRegionInTransition(hri.getEncodedName(), hri);
    if (!intransistion) return intransistion;
    LOG.debug("Waiting on " + HRegionInfo.prettyPrint(hri.getEncodedName()));
    while (!this.server.isStopped() &&
      this.regionStates.isRegionInTransition(hri.getEncodedName())) {
      // We put a timeout because we may have the region getting in just between the test
      //  and the waitForUpdate
      this.regionStates.waitForUpdate(100);
    }
    return intransistion;
  }

  /**
   * Process failover of new master for region <code>encodedRegionName</code>
   * up in zookeeper.
   * @param encodedRegionName Region to process failover for.
   * @param regionInfo If null we'll go get it from meta table.
   * @return True if we processed <code>regionInfo</code> as a RIT.
   * @throws KeeperException
   * @throws IOException
   */
  boolean processRegionInTransition(final String encodedRegionName,
      final HRegionInfo regionInfo) throws KeeperException, IOException {
    // We need a lock here to ensure that we will not put the same region twice
    // It has no reason to be a lock shared with the other operations.
    // We can do the lock on the region only, instead of a global lock: what we want to ensure
    // is that we don't have two threads working on the same region.
    Lock lock = locker.acquireLock(encodedRegionName);
    try {
      Stat stat = new Stat();
      byte [] data = ZKAssign.getDataAndWatch(watcher, encodedRegionName, stat);
      if (data == null) return false;
      RegionTransition rt;
      try {
        rt = RegionTransition.parseFrom(data);
      } catch (DeserializationException e) {
        LOG.warn("Failed parse znode data", e);
        return false;
      }
      HRegionInfo hri = regionInfo;
      if (hri == null) {
        hri = regionStates.getRegionInfo(rt.getRegionName());
        if (hri == null) return false;
      }
      processRegionsInTransition(rt, hri, stat.getVersion());
      return true;
    } finally {
      lock.unlock();
    }
  }

  /**
   * This call is invoked only (1) master assign root and meta;
   * (2) during failover mode startup, zk assignment node processing.
   * The locker is set in the caller.
   *
   * It should be private but it is used by some test too.
   */
  void processRegionsInTransition(
      final RegionTransition rt, final HRegionInfo regionInfo,
      int expectedVersion) throws KeeperException {
    EventType et = rt.getEventType();
    // Get ServerName.  Could not be null.
    ServerName sn = rt.getServerName();
    String encodedRegionName = regionInfo.getEncodedName();
    LOG.info("Processing region " + regionInfo.getRegionNameAsString() + " in state " + et);

    if (regionStates.isRegionInTransition(encodedRegionName)) {
      // Just return
      return;
    }
    switch (et) {
    case M_ZK_REGION_CLOSING:
      // If zk node of the region was updated by a live server skip this
      // region and just add it into RIT.
      if (!serverManager.isServerOnline(sn)) {
        // If was not online, its closed now. Force to OFFLINE and this
        // will get it reassigned if appropriate
        forceOffline(regionInfo, rt);
      } else {
        // Just insert region into RIT.
        // If this never updates the timeout will trigger new assignment
        regionStates.updateRegionState(rt, RegionState.State.CLOSING);
      }
      break;

    case RS_ZK_REGION_CLOSED:
    case RS_ZK_REGION_FAILED_OPEN:
      // Region is closed, insert into RIT and handle it
      addToRITandCallClose(regionInfo, RegionState.State.CLOSED, rt);
      break;

    case M_ZK_REGION_OFFLINE:
      // If zk node of the region was updated by a live server skip this
      // region and just add it into RIT.
      if (!serverManager.isServerOnline(sn)) {
        // Region is offline, insert into RIT and handle it like a closed
        addToRITandCallClose(regionInfo, RegionState.State.OFFLINE, rt);
      } else {
        // Just insert region into RIT.
        // If this never updates the timeout will trigger new assignment
        regionStates.updateRegionState(rt, RegionState.State.PENDING_OPEN);
      }
      break;

    case RS_ZK_REGION_OPENING:
      regionStates.updateRegionState(rt, RegionState.State.OPENING);
      if (regionInfo.isMetaTable() || !serverManager.isServerOnline(sn)) {
        // If ROOT or .META. table is waiting for timeout monitor to assign
        // it may take lot of time when the assignment.timeout.period is
        // the default value which may be very long.  We will not be able
        // to serve any request during this time.
        // So we will assign the ROOT and .META. region immediately.
        // For a user region, if the server is not online, it takes
        // some time for timeout monitor to kick in.  We know the region
        // won't open. So we will assign the opening
        // region immediately too.
        //
        // Otherwise, just insert region into RIT. If the state never
        // updates, the timeout will trigger new assignment
        processOpeningState(regionInfo);
      }
      break;

    case RS_ZK_REGION_OPENED:
      if (!serverManager.isServerOnline(sn)) {
        forceOffline(regionInfo, rt);
      } else {
        // Region is opened, insert into RIT and handle it
        regionStates.updateRegionState(rt, RegionState.State.OPEN);
        new OpenedRegionHandler(server, this, regionInfo, sn, expectedVersion).process();
      }
      break;
    case RS_ZK_REGION_SPLITTING:
      LOG.debug("Processed region in state : " + et);
      break;
    case RS_ZK_REGION_SPLIT:
      LOG.debug("Processed region in state : " + et);
      break;
    default:
      throw new IllegalStateException("Received region in state :" + et + " is not valid");
    }
  }

  /**
   * Put the region <code>hri</code> into an offline state up in zk.
   *
   * You need to have lock on the region before calling this method.
   *
   * @param hri
   * @param oldRt
   * @throws KeeperException
   */
  private void forceOffline(final HRegionInfo hri, final RegionTransition oldRt)
      throws KeeperException {
    // If was on dead server, its closed now.  Force to OFFLINE and then
    // handle it like a close; this will get it reassigned if appropriate
    LOG.debug("RIT " + hri.getEncodedName() + " in state=" + oldRt.getEventType() +
      " was on deadserver; forcing offline");
    ZKAssign.createOrForceNodeOffline(this.watcher, hri, oldRt.getServerName());
    addToRITandCallClose(hri, RegionState.State.OFFLINE, oldRt);
  }

  /**
   * Add to the in-memory copy of regions in transition and then call close
   * handler on passed region <code>hri</code>
   * @param hri
   * @param state
   * @param oldData
   */
  private void addToRITandCallClose(final HRegionInfo hri,
      final RegionState.State state, final RegionTransition oldData) {
    regionStates.updateRegionState(oldData, state);
    new ClosedRegionHandler(this.server, this, hri).process();
  }

  /**
   * When a region is closed, it should be removed from the regionsToReopen
   * @param hri HRegionInfo of the region which was closed
   */
  public void removeClosedRegion(HRegionInfo hri) {
    if (regionsToReopen.remove(hri.getEncodedName()) != null) {
      LOG.debug("Removed region from reopening regions because it was closed");
    }
  }

  /**
   * Handles various states an unassigned node can be in.
   * <p>
   * Method is called when a state change is suspected for an unassigned node.
   * <p>
   * This deals with skipped transitions (we got a CLOSED but didn't see CLOSING
   * yet).
   * @param rt
   * @param expectedVersion
   */
  private void handleRegion(final RegionTransition rt, int expectedVersion) {
    if (rt == null) {
      LOG.warn("Unexpected NULL input for RegionTransition rt");
      return;
    }
    final ServerName sn = rt.getServerName();
    // Check if this is a special HBCK transition
    if (sn.equals(HBCK_CODE_SERVERNAME)) {
      handleHBCK(rt);
      return;
    }
    final long createTime = rt.getCreateTime();
    final byte[] regionName = rt.getRegionName();
    String encodedName = HRegionInfo.encodeRegionName(regionName);
    String prettyPrintedRegionName = HRegionInfo.prettyPrint(encodedName);
    // Verify this is a known server
    if (!serverManager.isServerOnline(sn)
      && !ignoreStatesRSOffline.contains(rt.getEventType())) {
      LOG.warn("Attempted to handle region transition for server but " +
        "server is not online: " + prettyPrintedRegionName);
      return;
    }

    RegionState regionState =
      regionStates.getRegionTransitionState(encodedName);
    long startTime = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      boolean lateEvent = createTime < (startTime - 15000);
      LOG.debug("Handling transition=" + rt.getEventType() +
        ", server=" + sn + ", region=" +
        (prettyPrintedRegionName == null ? "null" : prettyPrintedRegionName) +
        (lateEvent ? ", which is more than 15 seconds late" : "") +
        ", current state from region state map =" + regionState);
    }
    // We don't do anything for this event,
    // so separate it out, no need to lock/unlock anything
    if (rt.getEventType() == EventType.M_ZK_REGION_OFFLINE) {
      return;
    }

    // We need a lock on the region as we could update it
    Lock lock = locker.acquireLock(encodedName);
    try {
      RegionState latestState =
        regionStates.getRegionTransitionState(encodedName);
      if ((regionState == null && latestState != null)
          || (regionState != null && latestState == null)
          || (regionState != null && latestState != null
            && latestState.getState() != regionState.getState())) {
        LOG.warn("Region state changed from " + regionState + " to "
          + latestState + ", while acquiring lock");
      }
      long waitedTime = System.currentTimeMillis() - startTime;
      if (waitedTime > 5000) {
        LOG.warn("Took " + waitedTime + "ms to acquire the lock");
      }
      regionState = latestState;
      switch (rt.getEventType()) {
        case RS_ZK_REGION_SPLITTING:
          if (!isInStateForSplitting(regionState)) break;
          regionStates.updateRegionState(rt, RegionState.State.SPLITTING);
          break;

        case RS_ZK_REGION_SPLIT:
          // RegionState must be null, or SPLITTING or PENDING_CLOSE.
          if (!isInStateForSplitting(regionState)) break;
          // If null, add SPLITTING state before going to SPLIT
          if (regionState == null) {
            regionState = regionStates.updateRegionState(rt,
              RegionState.State.SPLITTING);

            String message = "Received SPLIT for region " + prettyPrintedRegionName +
              " from server " + sn;
            // If still null, it means we cannot find it and it was already processed
            if (regionState == null) {
              LOG.warn(message + " but it doesn't exist anymore," +
                  " probably already processed its split");
              break;
            }
            LOG.info(message +
                " but region was not first in SPLITTING state; continuing");
          }
          // Check it has daughters.
          byte [] payload = rt.getPayload();
          List<HRegionInfo> daughters = null;
          try {
            daughters = HRegionInfo.parseDelimitedFrom(payload, 0, payload.length);
          } catch (IOException e) {
            LOG.error("Dropped split! Failed reading split payload for " +
              prettyPrintedRegionName);
            break;
          }
          assert daughters.size() == 2;
          // Assert that we can get a serverinfo for this server.
          if (!this.serverManager.isServerOnline(sn)) {
            LOG.error("Dropped split! ServerName=" + sn + " unknown.");
            break;
          }
          // Run handler to do the rest of the SPLIT handling.
          this.executorService.submit(new SplitRegionHandler(server, this,
            regionState.getRegion(), sn, daughters));
          break;

        case M_ZK_REGION_CLOSING:
          // Should see CLOSING after we have asked it to CLOSE or additional
          // times after already being in state of CLOSING
          if (regionState != null
              && !regionState.isPendingCloseOrClosingOnServer(sn)) {
            LOG.warn("Received CLOSING for region " + prettyPrintedRegionName
              + " from server " + sn + " but region was in the state " + regionState
              + " and not in expected PENDING_CLOSE or CLOSING states,"
              + " or not on the expected server");
            return;
          }
          // Transition to CLOSING (or update stamp if already CLOSING)
          regionStates.updateRegionState(rt, RegionState.State.CLOSING);
          break;

        case RS_ZK_REGION_CLOSED:
          // Should see CLOSED after CLOSING but possible after PENDING_CLOSE
          if (regionState != null
              && !regionState.isPendingCloseOrClosingOnServer(sn)) {
            LOG.warn("Received CLOSED for region " + prettyPrintedRegionName
              + " from server " + sn + " but region was in the state " + regionState
              + " and not in expected PENDING_CLOSE or CLOSING states,"
              + " or not on the expected server");
            return;
          }
          // Handle CLOSED by assigning elsewhere or stopping if a disable
          // If we got here all is good.  Need to update RegionState -- else
          // what follows will fail because not in expected state.
          regionState = regionStates.updateRegionState(rt, RegionState.State.CLOSED);
          if (regionState != null) {
            removeClosedRegion(regionState.getRegion());
            this.executorService.submit(new ClosedRegionHandler(server,
              this, regionState.getRegion()));
          }
          break;

        case RS_ZK_REGION_FAILED_OPEN:
          if (regionState != null
              && !regionState.isPendingOpenOrOpeningOnServer(sn)) {
            LOG.warn("Received FAILED_OPEN for region " + prettyPrintedRegionName
              + " from server " + sn + " but region was in the state " + regionState
              + " and not in expected PENDING_OPEN or OPENING states,"
              + " or not on the expected server");
            return;
          }
          // Handle this the same as if it were opened and then closed.
          regionState = regionStates.updateRegionState(rt, RegionState.State.CLOSED);
          // When there are more than one region server a new RS is selected as the
          // destination and the same is updated in the regionplan. (HBASE-5546)
          if (regionState != null) {
            getRegionPlan(regionState.getRegion(), sn, true);
            this.executorService.submit(new ClosedRegionHandler(server,
              this, regionState.getRegion()));
          }
          break;

        case RS_ZK_REGION_OPENING:
          // Should see OPENING after we have asked it to OPEN or additional
          // times after already being in state of OPENING
          if (regionState != null
              && !regionState.isPendingOpenOrOpeningOnServer(sn)) {
            LOG.warn("Received OPENING for region " + prettyPrintedRegionName
              + " from server " + sn + " but region was in the state " + regionState
              + " and not in expected PENDING_OPEN or OPENING states,"
              + " or not on the expected server");
            return;
          }
          // Transition to OPENING (or update stamp if already OPENING)
          regionStates.updateRegionState(rt, RegionState.State.OPENING);
          break;

        case RS_ZK_REGION_OPENED:
          // Should see OPENED after OPENING but possible after PENDING_OPEN
          if (regionState != null
              && !regionState.isPendingOpenOrOpeningOnServer(sn)) {
            LOG.warn("Received OPENED for region " + prettyPrintedRegionName
              + " from server " + sn + " but region was in the state " + regionState
              + " and not in expected PENDING_OPEN or OPENING states,"
              + " or not on the expected server");
            return;
          }
          // Handle OPENED by removing from transition and deleted zk node
          regionState = regionStates.updateRegionState(rt, RegionState.State.OPEN);
          if (regionState != null) {
            this.executorService.submit(new OpenedRegionHandler(
              server, this, regionState.getRegion(), sn, expectedVersion));
          }
          break;

        default:
          throw new IllegalStateException("Received event is not valid.");
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return Returns true if this RegionState is splittable; i.e. the
   * RegionState is currently in splitting state or pending_close or
   * null (Anything else will return false). (Anything else will return false).
   */
  private boolean isInStateForSplitting(final RegionState rs) {
    if (rs == null) return true;
    if (rs.isSplitting()) return true;
    if (convertPendingCloseToSplitting(rs)) return true;
    LOG.warn("Dropped region split! Not in state good for SPLITTING; rs=" + rs);
    return false;
  }

  /**
   * If the passed regionState is in PENDING_CLOSE, clean up PENDING_CLOSE
   * state and convert it to SPLITTING instead.
   * This can happen in case where master wants to close a region at same time
   * a regionserver starts a split.  The split won.  Clean out old PENDING_CLOSE
   * state.
   * @param rs
   * @return True if we converted from PENDING_CLOSE to SPLITTING
   */
  private boolean convertPendingCloseToSplitting(final RegionState rs) {
    if (!rs.isPendingClose()) return false;
    LOG.debug("Converting PENDING_CLOSE to SPLITING; rs=" + rs);
    regionStates.updateRegionState(
      rs.getRegion(), RegionState.State.SPLITTING);
    // Clean up existing state.  Clear from region plans seems all we
    // have to do here by way of clean up of PENDING_CLOSE.
    clearRegionPlan(rs.getRegion());
    return true;
  }

  /**
   * Handle a ZK unassigned node transition triggered by HBCK repair tool.
   * <p>
   * This is handled in a separate code path because it breaks the normal rules.
   * @param rt
   */
  private void handleHBCK(RegionTransition rt) {
    String encodedName = HRegionInfo.encodeRegionName(rt.getRegionName());
    LOG.info("Handling HBCK triggered transition=" + rt.getEventType() +
      ", server=" + rt.getServerName() + ", region=" +
      HRegionInfo.prettyPrint(encodedName));
    RegionState regionState = regionStates.getRegionTransitionState(encodedName);
    switch (rt.getEventType()) {
      case M_ZK_REGION_OFFLINE:
        HRegionInfo regionInfo = null;
        if (regionState != null) {
          regionInfo = regionState.getRegion();
        } else {
          try {
            byte [] name = rt.getRegionName();
            Pair<HRegionInfo, ServerName> p = MetaReader.getRegion(catalogTracker, name);
            regionInfo = p.getFirst();
          } catch (IOException e) {
            LOG.info("Exception reading META doing HBCK repair operation", e);
            return;
          }
        }
        LOG.info("HBCK repair is triggering assignment of region=" +
            regionInfo.getRegionNameAsString());
        // trigger assign, node is already in OFFLINE so don't need to update ZK
        assign(regionInfo, false);
        break;

      default:
        LOG.warn("Received unexpected region state from HBCK: " + rt.toString());
        break;
    }

  }

  // ZooKeeper events

  /**
   * New unassigned node has been created.
   *
   * <p>This happens when an RS begins the OPENING or CLOSING of a region by
   * creating an unassigned node.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further events</li>
   *   <li>Read and handle the state in the node</li>
   * </ol>
   */
  @Override
  public void nodeCreated(String path) {
    handleAssignmentEvent(path);
  }

  /**
   * Existing unassigned node has had data changed.
   *
   * <p>This happens when an RS transitions from OFFLINE to OPENING, or between
   * OPENING/OPENED and CLOSING/CLOSED.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further events</li>
   *   <li>Read and handle the state in the node</li>
   * </ol>
   */
  @Override
  public void nodeDataChanged(String path) {
    handleAssignmentEvent(path);
  }


  // We  don't want to have two events on the same region managed simultaneously.
  // For this reason, we need to wait if an event on the same region is currently in progress.
  // So we track the region names of the events in progress, and we keep a waiting list.
  private final Set<String> regionsInProgress = new HashSet<String>();
  // In a LinkedHashMultimap, the put order is kept when we retrieve the collection back. We need
  //  this as we want the events to be managed in the same order as we received them.
  private final LinkedHashMultimap <String, RegionRunnable>
      zkEventWorkerWaitingList = LinkedHashMultimap.create();

  /**
   * A specific runnable that works only on a region.
   */
  private static interface RegionRunnable extends Runnable{
    /**
     * @return - the name of the region it works on.
     */
    public String getRegionName();
  }

  /**
   * Submit a task, ensuring that there is only one task at a time that working on a given region.
   * Order is respected.
   */
  protected void zkEventWorkersSubmit(final RegionRunnable regRunnable) {

    synchronized (regionsInProgress) {
      // If we're there is already a task with this region, we add it to the
      //  waiting list and return.
      if (regionsInProgress.contains(regRunnable.getRegionName())) {
        synchronized (zkEventWorkerWaitingList){
          zkEventWorkerWaitingList.put(regRunnable.getRegionName(), regRunnable);
        }
        return;
      }

      // No event in progress on this region => we can submit a new task immediately.
      regionsInProgress.add(regRunnable.getRegionName());
      zkEventWorkers.submit(new Runnable() {
        @Override
        public void run() {
          try {
            regRunnable.run();
          } finally {
            // now that we have finished, let's see if there is an event for the same region in the
            //  waiting list. If it's the case, we can now submit it to the pool.
            synchronized (regionsInProgress) {
              regionsInProgress.remove(regRunnable.getRegionName());
              synchronized (zkEventWorkerWaitingList) {
                java.util.Set<RegionRunnable> waiting = zkEventWorkerWaitingList.get(
                    regRunnable.getRegionName());
                if (!waiting.isEmpty()) {
                  // We want the first object only. The only way to get it is through an iterator.
                  RegionRunnable toSubmit = waiting.iterator().next();
                  zkEventWorkerWaitingList.remove(toSubmit.getRegionName(), toSubmit);
                  zkEventWorkersSubmit(toSubmit);
                }
              }
            }
          }
        }
      });
    }
  }

  @Override
  public void nodeDeleted(final String path) {
    if (path.startsWith(watcher.assignmentZNode)) {
      final String regionName = ZKAssign.getRegionName(watcher, path);
      zkEventWorkersSubmit(new RegionRunnable() {
        @Override
        public String getRegionName() {
          return regionName;
        }

        @Override
        public void run() {
          Lock lock = locker.acquireLock(regionName);
          try {
            RegionState rs = regionStates.getRegionTransitionState(regionName);
            if (rs == null) return;

            HRegionInfo regionInfo = rs.getRegion();
            if (rs.isSplit()) {
              LOG.debug("Ephemeral node deleted, regionserver crashed?, " +
                "clearing from RIT; rs=" + rs);
              regionOffline(rs.getRegion());
            } else {
              String regionNameStr = regionInfo.getRegionNameAsString();
              LOG.debug("The znode of region " + regionNameStr
                + " has been deleted.");
              if (rs.isOpened()) {
                ServerName serverName = rs.getServerName();
                regionOnline(regionInfo, serverName);
                LOG.info("The master has opened the region "
                  + regionNameStr + " that was online on " + serverName);
                boolean disabled = getZKTable().isDisablingOrDisabledTable(
                  regionInfo.getTableNameAsString());
                if (!serverManager.isServerOnline(serverName) && !disabled) {
                  LOG.info("Opened region " + regionNameStr
                    + "but the region server is offline, reassign the region");
                  assign(regionInfo, true);
                } else if (disabled) {
                  // if server is offline, no hurt to unassign again
                  LOG.info("Opened region " + regionNameStr
                    + "but this table is disabled, triggering close of region");
                  unassign(regionInfo);
                }
              }
            }
          } finally {
            lock.unlock();
          }
        }
      });
    }
  }

  /**
   * New unassigned node has been created.
   *
   * <p>This happens when an RS begins the OPENING, SPLITTING or CLOSING of a
   * region by creating a znode.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further children changed events</li>
   *   <li>Watch all new children for changed events</li>
   * </ol>
   */
  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(watcher.assignmentZNode)) {
      zkEventWorkers.submit(new Runnable() {
        @Override
        public void run() {
          try {
            // Just make sure we see the changes for the new znodes
            List<String> children =
              ZKUtil.listChildrenAndWatchForNewChildren(
                watcher, watcher.assignmentZNode);
            if (children != null) {
              for (String child : children) {
                // if region is in transition, we already have a watch
                // on it, so no need to watch it again. So, as I know for now,
                // this is needed to watch splitting nodes only.
                if (!regionStates.isRegionInTransition(child)) {
                  ZKUtil.watchAndCheckExists(watcher,
                    ZKUtil.joinZNode(watcher.assignmentZNode, child));
                }
              }
            }
          } catch (KeeperException e) {
            server.abort("Unexpected ZK exception reading unassigned children", e);
          }
        }
      });
    }
  }

  /**
   * Marks the region as online.  Removes it from regions in transition and
   * updates the in-memory assignment information.
   * <p>
   * Used when a region has been successfully opened on a region server.
   * @param regionInfo
   * @param sn
   */
  void regionOnline(HRegionInfo regionInfo, ServerName sn) {
    if (!serverManager.isServerOnline(sn)) {
      LOG.warn("A region was opened on a dead server, ServerName=" +
        sn + ", region=" + regionInfo.getEncodedName());
    }

    regionStates.regionOnline(regionInfo, sn);

    // Remove plan if one.
    clearRegionPlan(regionInfo);
    // Add the server to serversInUpdatingTimer
    addToServersInUpdatingTimer(sn);
  }

  /**
   * Pass the assignment event to a worker for processing.
   * Each worker is a single thread executor service.  The reason
   * for just one thread is to make sure all events for a given
   * region are processed in order.
   *
   * @param path
   */
  private void handleAssignmentEvent(final String path) {
    if (path.startsWith(watcher.assignmentZNode)) {
      final String regionName = ZKAssign.getRegionName(watcher, path);

      zkEventWorkersSubmit(new RegionRunnable() {
        @Override
        public String getRegionName() {
          return regionName;
        }

        @Override
        public void run() {
          try {
            Stat stat = new Stat();
            byte [] data = ZKAssign.getDataAndWatch(watcher, path, stat);
            if (data == null) return;

            RegionTransition rt = RegionTransition.parseFrom(data);
            handleRegion(rt, stat.getVersion());
          } catch (KeeperException e) {
            server.abort("Unexpected ZK exception reading unassigned node data", e);
          } catch (DeserializationException e) {
            server.abort("Unexpected exception deserializing node data", e);
          }
        }
      });
    }
  }

  /**
   * Add the server to the set serversInUpdatingTimer, then {@link TimerUpdater}
   * will update timers for this server in background
   * @param sn
   */
  private void addToServersInUpdatingTimer(final ServerName sn) {
    this.serversInUpdatingTimer.add(sn);
  }

  /**
   * Touch timers for all regions in transition that have the passed
   * <code>sn</code> in common.
   * Call this method whenever a server checks in.  Doing so helps the case where
   * a new regionserver has joined the cluster and its been given 1k regions to
   * open.  If this method is tickled every time the region reports in a
   * successful open then the 1k-th region won't be timed out just because its
   * sitting behind the open of 999 other regions.  This method is NOT used
   * as part of bulk assign -- there we have a different mechanism for extending
   * the regions in transition timer (we turn it off temporarily -- because
   * there is no regionplan involved when bulk assigning.
   * @param sn
   */
  private void updateTimers(final ServerName sn) {
    if (sn == null) return;

    // This loop could be expensive.
    // First make a copy of current regionPlan rather than hold sync while
    // looping because holding sync can cause deadlock.  Its ok in this loop
    // if the Map we're going against is a little stale
    List<Map.Entry<String, RegionPlan>> rps;
    synchronized(this.regionPlans) {
      rps = new ArrayList<Map.Entry<String, RegionPlan>>(regionPlans.entrySet());
    }

    for (Map.Entry<String, RegionPlan> e : rps) {
      if (e.getValue() != null && e.getKey() != null && sn.equals(e.getValue().getDestination())) {
        RegionState regionState = regionStates.getRegionTransitionState(e.getKey());
        if (regionState != null) {
          regionState.updateTimestampToNow();
        }
      }
    }
  }

  /**
   * Marks the region as offline.  Removes it from regions in transition and
   * removes in-memory assignment information.
   * <p>
   * Used when a region has been closed and should remain closed.
   * @param regionInfo
   */
  public void regionOffline(final HRegionInfo regionInfo) {
    regionStates.regionOffline(regionInfo);

    // remove the region plan as well just in case.
    clearRegionPlan(regionInfo);
  }

  public void offlineDisabledRegion(HRegionInfo regionInfo) {
    // Disabling so should not be reassigned, just delete the CLOSED node
    LOG.debug("Table being disabled so deleting ZK node and removing from " +
        "regions in transition, skipping assignment of region " +
          regionInfo.getRegionNameAsString());
    try {
      if (!ZKAssign.deleteClosedNode(watcher, regionInfo.getEncodedName())) {
        // Could also be in OFFLINE mode
        ZKAssign.deleteOfflineNode(watcher, regionInfo.getEncodedName());
      }
    } catch (KeeperException.NoNodeException nne) {
      LOG.debug("Tried to delete closed node for " + regionInfo + " but it " +
          "does not exist so just offlining");
    } catch (KeeperException e) {
      this.server.abort("Error deleting CLOSED node in ZK", e);
    }
    regionOffline(regionInfo);
  }

  // Assignment methods

  /**
   * Assigns the specified region.
   * <p>
   * If a RegionPlan is available with a valid destination then it will be used
   * to determine what server region is assigned to.  If no RegionPlan is
   * available, region will be assigned to a random available server.
   * <p>
   * Updates the RegionState and sends the OPEN RPC.
   * <p>
   * This will only succeed if the region is in transition and in a CLOSED or
   * OFFLINE state or not in transition (in-memory not zk), and of course, the
   * chosen server is up and running (It may have just crashed!).  If the
   * in-memory checks pass, the zk node is forced to OFFLINE before assigning.
   *
   * @param region server to be assigned
   * @param setOfflineInZK whether ZK node should be created/transitioned to an
   *                       OFFLINE state before assigning the region
   */
  public void assign(HRegionInfo region, boolean setOfflineInZK) {
    assign(region, setOfflineInZK, false);
  }

  /**
   * Use care with forceNewPlan. It could cause double assignment.
   */
  public void assign(HRegionInfo region,
      boolean setOfflineInZK, boolean forceNewPlan) {
    if (!setOfflineInZK && isDisabledorDisablingRegionInRIT(region)) {
      return;
    }
    if (this.serverManager.isClusterShutdown()) {
      LOG.info("Cluster shutdown is set; skipping assign of " +
        region.getRegionNameAsString());
      return;
    }
    String encodedName = region.getEncodedName();
    Lock lock = locker.acquireLock(encodedName);
    try {
      RegionState state = forceRegionStateToOffline(region, forceNewPlan);
      if (state != null) {
        assign(state, setOfflineInZK, forceNewPlan);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Bulk assign regions to <code>destination</code>.
   * @param destination
   * @param regions Regions to assign.
   * @return true if successful
   */
  boolean assign(final ServerName destination,
      final List<HRegionInfo> regions) {
    int regionCount = regions.size();
    if (regionCount == 0) {
      return true;
    }
    LOG.debug("Bulk assigning " + regionCount + " region(s) to " +
      destination.toString());

    Set<String> encodedNames = new HashSet<String>(regionCount);
    for (HRegionInfo region : regions) {
      encodedNames.add(region.getEncodedName());
    }

    List<HRegionInfo> failedToOpenRegions = new ArrayList<HRegionInfo>();
    Map<String, Lock> locks = locker.acquireLocks(encodedNames);
    try {
      AtomicInteger counter = new AtomicInteger(0);
      Map<String, Integer> offlineNodesVersions = new ConcurrentHashMap<String, Integer>();
      OfflineCallback cb = new OfflineCallback(
        watcher, destination, counter, offlineNodesVersions);
      Map<String, RegionPlan> plans = new HashMap<String, RegionPlan>(regions.size());
      List<RegionState> states = new ArrayList<RegionState>(regions.size());
      for (HRegionInfo region : regions) {
        String encodedRegionName = region.getEncodedName();
        RegionState state = forceRegionStateToOffline(region, true);
        if (state != null && asyncSetOfflineInZooKeeper(state, cb, destination)) {
          RegionPlan plan = new RegionPlan(region, state.getServerName(), destination);
          plans.put(encodedRegionName, plan);
          states.add(state);
        } else {
          LOG.warn("failed to force region state to offline or "
            + "failed to set it offline in ZK, will reassign later: " + region);
          failedToOpenRegions.add(region); // assign individually later
          Lock lock = locks.remove(encodedRegionName);
          lock.unlock();
        }
      }

      // Wait until all unassigned nodes have been put up and watchers set.
      int total = states.size();
      for (int oldCounter = 0; !server.isStopped();) {
        int count = counter.get();
        if (oldCounter != count) {
          LOG.info(destination.toString() + " unassigned znodes=" + count +
            " of total=" + total);
          oldCounter = count;
        }
        if (count >= total) break;
        Threads.sleep(5);
      }

      if (server.isStopped()) {
        return false;
      }

      // Add region plans, so we can updateTimers when one region is opened so
      // that unnecessary timeout on RIT is reduced.
      this.addPlans(plans);

      List<Pair<HRegionInfo, Integer>> regionOpenInfos =
        new ArrayList<Pair<HRegionInfo, Integer>>(states.size());
      for (RegionState state: states) {
        HRegionInfo region = state.getRegion();
        String encodedRegionName = region.getEncodedName();
        Integer nodeVersion = offlineNodesVersions.get(encodedRegionName);
        if (nodeVersion == null || nodeVersion.intValue() == -1) {
          LOG.warn("failed to offline in zookeeper: " + region);
          failedToOpenRegions.add(region); // assign individually later
          Lock lock = locks.remove(encodedRegionName);
          lock.unlock();
        } else {
          regionStates.updateRegionState(region,
            RegionState.State.PENDING_OPEN, destination);
          regionOpenInfos.add(new Pair<HRegionInfo, Integer>(
            region, nodeVersion));
        }
      }

      // Move on to open regions.
      try {
        // Send OPEN RPC. If it fails on a IOE or RemoteException, the
        // TimeoutMonitor will pick up the pieces.
        long maxWaitTime = System.currentTimeMillis() +
          this.server.getConfiguration().
            getLong("hbase.regionserver.rpc.startup.waittime", 60000);
        for (int i = 1; i <= maximumAttempts && !server.isStopped(); i++) {
          try {
            List<RegionOpeningState> regionOpeningStateList = serverManager
              .sendRegionOpen(destination, regionOpenInfos);
            if (regionOpeningStateList == null) {
              // Failed getting RPC connection to this server
              return false;
            }
            for (int k = 0, n = regionOpeningStateList.size(); k < n; k++) {
              RegionOpeningState openingState = regionOpeningStateList.get(k);
              if (openingState != RegionOpeningState.OPENED) {
                HRegionInfo region = regionOpenInfos.get(k).getFirst();
                if (openingState == RegionOpeningState.ALREADY_OPENED) {
                  processAlreadyOpenedRegion(region, destination);
                } else if (openingState == RegionOpeningState.FAILED_OPENING) {
                  // Failed opening this region, reassign it later
                  failedToOpenRegions.add(region);
                } else {
                  LOG.warn("THIS SHOULD NOT HAPPEN: unknown opening state "
                    + openingState + " in assigning region " + region);
                }
              }
            }
            break;
          } catch (IOException e) {
            if (e instanceof RemoteException) {
              e = ((RemoteException)e).unwrapRemoteException();
            }
            if (e instanceof RegionServerStoppedException) {
              LOG.warn("The region server was shut down, ", e);
              // No need to retry, the region server is a goner.
              return false;
            } else if (e instanceof ServerNotRunningYetException) {
              long now = System.currentTimeMillis();
              if (now < maxWaitTime) {
                LOG.debug("Server is not yet up; waiting up to " +
                  (maxWaitTime - now) + "ms", e);
                Thread.sleep(100);
                i--; // reset the try count
                continue;
              }
            } else if (e instanceof java.net.SocketTimeoutException
                && this.serverManager.isServerOnline(destination)) {
              // In case socket is timed out and the region server is still online,
              // the openRegion RPC could have been accepted by the server and
              // just the response didn't go through.  So we will retry to
              // open the region on the same server.
              if (LOG.isDebugEnabled()) {
                LOG.debug("Bulk assigner openRegion() to " + destination
                  + " has timed out, but the regions might"
                  + " already be opened on it.", e);
              }
              continue;
            }
            throw e;
          }
        }
      } catch (IOException e) {
        // Can be a socket timeout, EOF, NoRouteToHost, etc
        LOG.info("Unable to communicate with the region server in order" +
          " to assign regions", e);
        return false;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    } finally {
      for (Lock lock : locks.values()) {
        lock.unlock();
      }
    }

    if (!failedToOpenRegions.isEmpty()) {
      for (HRegionInfo region : failedToOpenRegions) {
        invokeAssign(region);
      }
    }
    LOG.debug("Bulk assigning done for " + destination.toString());
    return true;
  }

  /**
   * Send CLOSE RPC if the server is online, otherwise, offline the region
   */
  private void unassign(final HRegionInfo region,
      final RegionState state, final int versionOfClosingNode,
      final ServerName dest, final boolean transitionInZK) {
    // Send CLOSE RPC
    ServerName server = state.getServerName();
    // ClosedRegionhandler can remove the server from this.regions
    if (!serverManager.isServerOnline(server)) {
      if (transitionInZK) {
        // delete the node. if no node exists need not bother.
        deleteClosingOrClosedNode(region);
      }
      regionOffline(region);
      return;
    }

    for (int i = 1; i <= this.maximumAttempts; i++) {
      try {
        if (serverManager.sendRegionClose(server, region,
          versionOfClosingNode, dest, transitionInZK)) {
          LOG.debug("Sent CLOSE to " + server + " for region " +
            region.getRegionNameAsString());
          return;
        }
        // This never happens. Currently regionserver close always return true.
        // Todo; this can now happen (0.96) if there is an exception in a coprocessor
        LOG.warn("Server " + server + " region CLOSE RPC returned false for " +
          region.getRegionNameAsString());
      } catch (Throwable t) {
        if (t instanceof RemoteException) {
          t = ((RemoteException)t).unwrapRemoteException();
        }
        if (t instanceof NotServingRegionException) {
          if (transitionInZK) {
            deleteClosingOrClosedNode(region);
          }
          regionOffline(region);
          return;
        } else if (t instanceof RegionAlreadyInTransitionException) {
          // RS is already processing this region, only need to update the timestamp
          LOG.debug("update " + state + " the timestamp.");
          state.updateTimestampToNow();
        }
        LOG.info("Server " + server + " returned " + t + " for "
          + region.getRegionNameAsString() + ", try=" + i
          + " of " + this.maximumAttempts, t);
        // Presume retry or server will expire.
      }
    }
  }

  /**
   * Set region to OFFLINE unless it is opening and forceNewPlan is false.
   */
  private RegionState forceRegionStateToOffline(
      final HRegionInfo region, final boolean forceNewPlan) {
    RegionState state = regionStates.getRegionState(region);
    if (state == null) {
      LOG.warn("Assigning a region not in region states: " + region);
      state = regionStates.createRegionState(region);
    } else {
      switch (state.getState()) {
      case OPEN:
      case OPENING:
      case PENDING_OPEN:
        if (!forceNewPlan) {
          LOG.debug("Attempting to assign region " +
            region + " but it is already in transition: " + state);
          return null;
        }
      case CLOSING:
      case PENDING_CLOSE:
        unassign(region, state, -1, null, false);
      case CLOSED:
        if (!state.isOffline()) {
          LOG.debug("Forcing OFFLINE; was=" + state);
          state = regionStates.updateRegionState(
            region, RegionState.State.OFFLINE);
        }
      case OFFLINE:
        break;
      default:
        LOG.error("Trying to assign region " + region
          + ", which is in state " + state);
        return null;
      }
    }
    return state;
  }

  /**
   * Caller must hold lock on the passed <code>state</code> object.
   * @param state
   * @param setOfflineInZK
   * @param forceNewPlan
   */
  private void assign(RegionState state,
      final boolean setOfflineInZK, final boolean forceNewPlan) {
    RegionState currentState = state;
    int versionOfOfflineNode = -1;
    RegionPlan plan = null;
    long maxRegionServerStartupWaitTime = -1;
    HRegionInfo region = state.getRegion();
    for (int i = 1; i <= maximumAttempts && !server.isStopped(); i++) {
      if (plan == null) { // Get a server for the region at first
        plan = getRegionPlan(region, forceNewPlan);
      }
      if (plan == null) {
        LOG.debug("Unable to determine a plan to assign " + region);
        this.timeoutMonitor.setAllRegionServersOffline(true);
        return; // Should get reassigned later when RIT times out.
      }
      if (setOfflineInZK && versionOfOfflineNode == -1) {
        // get the version of the znode after setting it to OFFLINE.
        // versionOfOfflineNode will be -1 if the znode was not set to OFFLINE
        versionOfOfflineNode = setOfflineInZooKeeper(currentState, plan.getDestination());
        if (versionOfOfflineNode != -1) {
          if (isDisabledorDisablingRegionInRIT(region)) {
            return;
          }
          // In case of assignment from EnableTableHandler table state is ENABLING. Any how
          // EnableTableHandler will set ENABLED after assigning all the table regions. If we
          // try to set to ENABLED directly then client API may think table is enabled.
          // When we have a case such as all the regions are added directly into .META. and we call
          // assignRegion then we need to make the table ENABLED. Hence in such case the table
          // will not be in ENABLING or ENABLED state.
          String tableName = region.getTableNameAsString();
          if (!zkTable.isEnablingTable(tableName) && !zkTable.isEnabledTable(tableName)) {
            LOG.debug("Setting table " + tableName + " to ENABLED state.");
            setEnabledTable(tableName);
          }
        }
      }
      if (setOfflineInZK && versionOfOfflineNode == -1) {
        return;
      }
      if (this.server.isStopped()) {
        LOG.debug("Server stopped; skipping assign of " + region);
        return;
      }
      try {
        LOG.info("Assigning region " + region.getRegionNameAsString() +
          " to " + plan.getDestination().toString());
        // Transition RegionState to PENDING_OPEN
        currentState = regionStates.updateRegionState(region,
          RegionState.State.PENDING_OPEN, plan.getDestination());
        // Send OPEN RPC. This can fail if the server on other end is is not up.
        // Pass the version that was obtained while setting the node to OFFLINE.
        RegionOpeningState regionOpenState = serverManager.sendRegionOpen(plan
            .getDestination(), region, versionOfOfflineNode);
        if (regionOpenState == RegionOpeningState.ALREADY_OPENED) {
          processAlreadyOpenedRegion(region, plan.getDestination());
        } else if (regionOpenState == RegionOpeningState.FAILED_OPENING) {
          // Failed opening this region
          throw new Exception("Get regionOpeningState=" + regionOpenState);
        }
        break;
      } catch (Throwable t) {
        if (t instanceof RemoteException) {
          t = ((RemoteException) t).unwrapRemoteException();
        }
        boolean regionAlreadyInTransitionException = false;
        boolean serverNotRunningYet = false;
        boolean socketTimedOut = false;
        if (t instanceof RegionAlreadyInTransitionException) {
          regionAlreadyInTransitionException = true;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failed assignment in: " + plan.getDestination() + " due to "
              + t.getMessage());
          }
        } else if (t instanceof ServerNotRunningYetException) {
          if (maxRegionServerStartupWaitTime < 0) {
            maxRegionServerStartupWaitTime = System.currentTimeMillis() +
              this.server.getConfiguration().
                getLong("hbase.regionserver.rpc.startup.waittime", 60000);
          }
          try {
            long now = System.currentTimeMillis();
            if (now < maxRegionServerStartupWaitTime) {
              LOG.debug("Server is not yet up; waiting up to " +
                (maxRegionServerStartupWaitTime - now) + "ms", t);
              serverNotRunningYet = true;
              Thread.sleep(100);
              i--; // reset the try count
            } else {
              LOG.debug("Server is not up for a while; try a new one", t);
            }
          } catch (InterruptedException ie) {
            LOG.warn("Failed to assign "
              + region.getRegionNameAsString() + " since interrupted", ie);
            Thread.currentThread().interrupt();
            return;
          }
        } else if (t instanceof java.net.SocketTimeoutException
            && this.serverManager.isServerOnline(plan.getDestination())) {
          // In case socket is timed out and the region server is still online,
          // the openRegion RPC could have been accepted by the server and
          // just the response didn't go through.  So we will retry to
          // open the region on the same server to avoid possible
          // double assignment.
          socketTimedOut = true;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Call openRegion() to " + plan.getDestination()
              + " has timed out when trying to assign "
              + region.getRegionNameAsString()
              + ", but the region might already be opened on "
              + plan.getDestination() + ".", t);
          }
        }

        LOG.warn("Failed assignment of "
          + region.getRegionNameAsString()
          + " to "
          + plan.getDestination()
          + ", trying to assign "
          + (regionAlreadyInTransitionException || serverNotRunningYet || socketTimedOut
            ? "to the same region server because of RegionAlreadyInTransitionException"
              + "/ServerNotRunningYetException/SocketTimeoutException;"
              : "elsewhere instead; ")
          + "try=" + i + " of " + this.maximumAttempts, t);

        if (i == this.maximumAttempts) {
          // Don't reset the region state or get a new plan any more.
          // This is the last try.
          continue;
        }

        // If region opened on destination of present plan, reassigning to new
        // RS may cause double assignments. In case of RegionAlreadyInTransitionException
        // reassigning to same RS.
        RegionPlan newPlan = plan;
        if (!(regionAlreadyInTransitionException
            || serverNotRunningYet || socketTimedOut)) {
          // Force a new plan and reassign. Will return null if no servers.
          // The new plan could be the same as the existing plan since we don't
          // exclude the server of the original plan, which should not be
          // excluded since it could be the only server up now.
          newPlan = getRegionPlan(region, true);
        }
        if (newPlan == null) {
          this.timeoutMonitor.setAllRegionServersOffline(true);
          LOG.warn("Unable to find a viable location to assign region " +
            region.getRegionNameAsString());
          return;
        }
        if (plan != newPlan
            && !plan.getDestination().equals(newPlan.getDestination())) {
          // Clean out plan we failed execute and one that doesn't look like it'll
          // succeed anyways; we need a new plan!
          // Transition back to OFFLINE
          currentState = regionStates.updateRegionState(
            region, RegionState.State.OFFLINE);
          versionOfOfflineNode = -1;
          plan = newPlan;
        }
      }
    }
  }

  private void processAlreadyOpenedRegion(HRegionInfo region, ServerName sn) {
    // Remove region from in-memory transition and unassigned node from ZK
    // While trying to enable the table the regions of the table were
    // already enabled.
    LOG.debug("ALREADY_OPENED region " + region.getRegionNameAsString()
        + " to " + sn);
    String encodedRegionName = region.getEncodedName();
    try {
      ZKAssign.deleteOfflineNode(watcher, encodedRegionName);
    } catch (KeeperException.NoNodeException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("The unassigned node " + encodedRegionName
            + " doesnot exist.");
      }
    } catch (KeeperException e) {
      server.abort(
          "Error deleting OFFLINED node in ZK for transition ZK node ("
              + encodedRegionName + ")", e);
    }

    regionStates.regionOnline(region, sn);
  }

  private boolean isDisabledorDisablingRegionInRIT(final HRegionInfo region) {
    String tableName = region.getTableNameAsString();
    boolean disabled = this.zkTable.isDisabledTable(tableName);
    if (disabled || this.zkTable.isDisablingTable(tableName)) {
      LOG.info("Table " + tableName + (disabled ? " disabled;" : " disabling;") +
        " skipping assign of " + region.getRegionNameAsString());
      offlineDisabledRegion(region);
      return true;
    }
    return false;
  }

  /**
   * Set region as OFFLINED up in zookeeper
   *
   * @param state
   * @return the version of the offline node if setting of the OFFLINE node was
   *         successful, -1 otherwise.
   */
  private int setOfflineInZooKeeper(final RegionState state, final ServerName destination) {
    if (!state.isClosed() && !state.isOffline()) {
      String msg = "Unexpected state : " + state + " .. Cannot transit it to OFFLINE.";
      this.server.abort(msg, new IllegalStateException(msg));
      return -1;
    }
    regionStates.updateRegionState(state.getRegion(),
      RegionState.State.OFFLINE);
    int versionOfOfflineNode = -1;
    try {
      // get the version after setting the znode to OFFLINE
      versionOfOfflineNode = ZKAssign.createOrForceNodeOffline(watcher,
        state.getRegion(), destination);
      if (versionOfOfflineNode == -1) {
        LOG.warn("Attempted to create/force node into OFFLINE state before "
            + "completing assignment but failed to do so for " + state);
        return -1;
      }
    } catch (KeeperException e) {
      server.abort("Unexpected ZK exception creating/setting node OFFLINE", e);
      return -1;
    }
    return versionOfOfflineNode;
  }

  /**
   * @param region the region to assign
   * @return Plan for passed <code>region</code> (If none currently, it creates one or
   * if no servers to assign, it returns null).
   */
  private RegionPlan getRegionPlan(final HRegionInfo region,
      final boolean forceNewPlan) {
    return getRegionPlan(region, null, forceNewPlan);
  }

  /**
   * @param region the region to assign
   * @param serverToExclude Server to exclude (we know its bad). Pass null if
   * all servers are thought to be assignable.
   * @param forceNewPlan If true, then if an existing plan exists, a new plan
   * will be generated.
   * @return Plan for passed <code>region</code> (If none currently, it creates one or
   * if no servers to assign, it returns null).
   */
  private RegionPlan getRegionPlan(final HRegionInfo region,
      final ServerName serverToExclude, final boolean forceNewPlan) {
    // Pickup existing plan or make a new one
    final String encodedName = region.getEncodedName();
    final List<ServerName> destServers =
      serverManager.createDestinationServersList(serverToExclude);

    if (destServers.isEmpty()){
      LOG.warn("Can't move the region " + encodedName +
        ", there is no destination server available.");
      return null;
    }

    RegionPlan randomPlan = null;
    boolean newPlan = false;
    RegionPlan existingPlan = null;

    synchronized (this.regionPlans) {
      existingPlan = this.regionPlans.get(encodedName);

      if (existingPlan != null && existingPlan.getDestination() != null) {
        LOG.debug("Found an existing plan for " + region.getRegionNameAsString()
          + " destination server is " + existingPlan.getDestination());
      }

      if (forceNewPlan
          || existingPlan == null
          || existingPlan.getDestination() == null
          || !destServers.contains(existingPlan.getDestination())) {
        newPlan = true;
        randomPlan = new RegionPlan(region, null,
            balancer.randomAssignment(region, destServers));
        this.regionPlans.put(encodedName, randomPlan);
      }
    }

    if (newPlan) {
      LOG.debug("No previous transition plan was found (or we are ignoring " +
        "an existing plan) for " + region.getRegionNameAsString() +
        " so generated a random one; " + randomPlan + "; " +
        serverManager.countOfRegionServers() +
               " (online=" + serverManager.getOnlineServers().size() +
               ", available=" + destServers.size() + ") available servers");
        return randomPlan;
      }
    LOG.debug("Using pre-existing plan for region " +
      region.getRegionNameAsString() + "; plan=" + existingPlan);
    return existingPlan;
  }

  /**
   * Unassign the list of regions. Configuration knobs:
   * hbase.bulk.waitbetween.reopen indicates the number of milliseconds to
   * wait before unassigning another region from this region server
   *
   * @param regions
   * @throws InterruptedException
   */
  public void unassign(List<HRegionInfo> regions) {
    int waitTime = this.server.getConfiguration().getInt(
        "hbase.bulk.waitbetween.reopen", 0);
    for (HRegionInfo region : regions) {
      if (regionStates.isRegionInTransition(region))
        continue;
      unassign(region, false);
      while (regionStates.isRegionInTransition(region)) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          // Do nothing, continue
        }
      }
      if (waitTime > 0)
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e) {
          // Do nothing, continue
        }
    }
  }

  /**
   * Unassigns the specified region.
   * <p>
   * Updates the RegionState and sends the CLOSE RPC unless region is being
   * split by regionserver; then the unassign fails (silently) because we
   * presume the region being unassigned no longer exists (its been split out
   * of existence). TODO: What to do if split fails and is rolled back and
   * parent is revivified?
   * <p>
   * If a RegionPlan is already set, it will remain.
   *
   * @param region server to be unassigned
   */
  public void unassign(HRegionInfo region) {
    unassign(region, false);
  }


  /**
   * Unassigns the specified region.
   * <p>
   * Updates the RegionState and sends the CLOSE RPC unless region is being
   * split by regionserver; then the unassign fails (silently) because we
   * presume the region being unassigned no longer exists (its been split out
   * of existence). TODO: What to do if split fails and is rolled back and
   * parent is revivified?
   * <p>
   * If a RegionPlan is already set, it will remain.
   *
   * @param region server to be unassigned
   * @param force if region should be closed even if already closing
   */
  public void unassign(HRegionInfo region, boolean force, ServerName dest) {
    // TODO: Method needs refactoring.  Ugly buried returns throughout.  Beware!
    LOG.debug("Starting unassignment of region " +
      region.getRegionNameAsString() + " (offlining)");

    String encodedName = region.getEncodedName();
    // Grab the state of this region and synchronize on it
    int versionOfClosingNode = -1;
    // We need a lock here as we're going to do a put later and we don't want multiple states
    //  creation
    ReentrantLock lock = locker.acquireLock(encodedName);
    RegionState state = regionStates.getRegionTransitionState(encodedName);
    try {
      if (state == null) {
        // Create the znode in CLOSING state
        try {
          state = regionStates.getRegionState(region);
          if (state == null || state.getServerName() == null) {
            // We don't know where the region is, offline it.
            // No need to send CLOSE RPC
            regionOffline(region);
            return;
          }
          versionOfClosingNode = ZKAssign.createNodeClosing(
            watcher, region, state.getServerName());
          if (versionOfClosingNode == -1) {
            LOG.debug("Attempting to unassign region " +
                region.getRegionNameAsString() + " but ZK closing node "
                + "can't be created.");
            return;
          }
        } catch (KeeperException e) {
          if (e instanceof NodeExistsException) {
            // Handle race between master initiated close and regionserver
            // orchestrated splitting. See if existing node is in a
            // SPLITTING or SPLIT state.  If so, the regionserver started
            // an op on node before we could get our CLOSING in.  Deal.
            NodeExistsException nee = (NodeExistsException)e;
            String path = nee.getPath();
            try {
              if (isSplitOrSplitting(path)) {
                LOG.debug(path + " is SPLIT or SPLITTING; " +
                  "skipping unassign because region no longer exists -- its split");
                return;
              }
            } catch (KeeperException.NoNodeException ke) {
              LOG.warn("Failed getData on SPLITTING/SPLIT at " + path +
                "; presuming split and that the region to unassign, " +
                encodedName + ", no longer exists -- confirm", ke);
              return;
            } catch (KeeperException ke) {
              LOG.error("Unexpected zk state", ke);
            } catch (DeserializationException de) {
              LOG.error("Failed parse", de);
            }
          }
          // If we get here, don't understand whats going on -- abort.
          server.abort("Unexpected ZK exception creating node CLOSING", e);
          return;
        }
        state = regionStates.updateRegionState(region, RegionState.State.PENDING_CLOSE);
      } else if (force && (state.isPendingClose() || state.isClosing())) {
        LOG.debug("Attempting to unassign region " + region.getRegionNameAsString() +
          " which is already " + state.getState()  +
          " but forcing to send a CLOSE RPC again ");
        state.updateTimestampToNow();
      } else {
        LOG.debug("Attempting to unassign region " +
          region.getRegionNameAsString() + " but it is " +
          "already in transition (" + state.getState() + ", force=" + force + ")");
        return;
      }

      unassign(region, state, versionOfClosingNode, dest, true);
    } finally {
      lock.unlock();
    }
  }

  public void unassign(HRegionInfo region, boolean force){
     unassign(region, force, null);
  }

  /**
   * @param region regioninfo of znode to be deleted.
   */
  public void deleteClosingOrClosedNode(HRegionInfo region) {
    String encodedName = region.getEncodedName();
    try {
      if (!ZKAssign.deleteNode(watcher, encodedName,
          EventHandler.EventType.M_ZK_REGION_CLOSING)) {
        boolean deleteNode = ZKAssign.deleteNode(watcher,
          encodedName, EventHandler.EventType.RS_ZK_REGION_CLOSED);
        // TODO : We don't abort if the delete node returns false. Is there any
        // such corner case?
        if (!deleteNode) {
          LOG.error("The deletion of the CLOSED node for the region "
            + encodedName + " returned " + deleteNode);
        }
      }
    } catch (NoNodeException e) {
      LOG.debug("CLOSING/CLOSED node for the region " + encodedName
        + " already deleted");
    } catch (KeeperException ke) {
      server.abort(
        "Unexpected ZK exception deleting node CLOSING/CLOSED for the region "
          + encodedName, ke);
      return;
    }
  }

  /**
   * @param path
   * @return True if znode is in SPLIT or SPLITTING state.
   * @throws KeeperException Can happen if the znode went away in meantime.
   * @throws DeserializationException
   */
  private boolean isSplitOrSplitting(final String path)
      throws KeeperException, DeserializationException {
    boolean result = false;
    // This may fail if the SPLIT or SPLITTING znode gets cleaned up before we
    // can get data from it.
    byte [] data = ZKAssign.getData(watcher, path);
    if (data == null) return false;
    RegionTransition rt = RegionTransition.parseFrom(data);
    switch (rt.getEventType()) {
    case RS_ZK_REGION_SPLIT:
    case RS_ZK_REGION_SPLITTING:
      result = true;
      break;
    default:
      break;
    }
    return result;
  }

  /**
   * Waits until the specified region has completed assignment.
   * <p>
   * If the region is already assigned, returns immediately.  Otherwise, method
   * blocks until the region is assigned.
   * @param regionInfo region to wait on assignment for
   * @throws InterruptedException
   */
  public void waitForAssignment(HRegionInfo regionInfo)
      throws InterruptedException {
    while(!this.server.isStopped() &&
        !regionStates.isRegionAssigned(regionInfo)) {
      // We should receive a notification, but it's
      //  better to have a timeout to recheck the condition here:
      //  it lowers the impact of a race condition if any
      regionStates.waitForUpdate(100);
    }
  }

  /**
   * Assigns the ROOT region.
   * <p>
   * Assumes that ROOT is currently closed and is not being actively served by
   * any RegionServer.
   * <p>
   * Forcibly unsets the current root region location in ZooKeeper and assigns
   * ROOT to a random RegionServer.
   * @throws KeeperException
   */
  public void assignRoot() throws KeeperException {
    RootRegionTracker.deleteRootLocation(this.watcher);
    assign(HRegionInfo.ROOT_REGIONINFO, true);
  }

  /**
   * Assigns the META region.
   * <p>
   * Assumes that META is currently closed and is not being actively served by
   * any RegionServer.
   * <p>
   * Forcibly assigns META to a random RegionServer.
   */
  public void assignMeta() {
    // Force assignment to a random server
    assign(HRegionInfo.FIRST_META_REGIONINFO, true);
  }

  /**
   * Assigns specified regions retaining assignments, if any.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned.  If anything fails, an exception is thrown
   * @throws InterruptedException
   * @throws IOException
   */
  public void assign(Map<HRegionInfo, ServerName> regions)
        throws IOException, InterruptedException {
    if (regions == null || regions.isEmpty()) {
      return;
    }
    List<ServerName> servers = serverManager.createDestinationServersList();
    if (servers == null || servers.isEmpty()) {
      throw new IOException("Found no destination server to assign region(s)");
    }

    // Reuse existing assignment info
    Map<ServerName, List<HRegionInfo>> bulkPlan =
      balancer.retainAssignment(regions, servers);

    LOG.info("Bulk assigning " + regions.size() + " region(s) across " +
      servers.size() + " server(s), retainAssignment=true");
    BulkAssigner ba = new GeneralBulkAssigner(this.server, bulkPlan, this);
    ba.bulkAssign();
    LOG.info("Bulk assigning done");
  }

  /**
   * Assigns specified regions round robin, if any.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned.  If anything fails, an exception is thrown
   * @throws InterruptedException
   * @throws IOException
   */
  public void assign(List<HRegionInfo> regions)
        throws IOException, InterruptedException {
    if (regions == null || regions.isEmpty()) {
      return;
    }
    List<ServerName> servers = serverManager.createDestinationServersList();
    if (servers == null || servers.isEmpty()) {
      throw new IOException("Found no destination server to assign region(s)");
    }

    // Generate a round-robin bulk assignment plan
    Map<ServerName, List<HRegionInfo>> bulkPlan
      = balancer.roundRobinAssignment(regions, servers);

    LOG.info("Bulk assigning " + regions.size() + " region(s) round-robin across "
      + servers.size() + " server(s)");

    // Use fixed count thread pool assigning.
    BulkAssigner ba = new GeneralBulkAssigner(this.server, bulkPlan, this);
    ba.bulkAssign();
    LOG.info("Bulk assigning done");
  }

  /**
   * Assigns all user regions, if any exist.  Used during cluster startup.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned.  If anything fails, an exception is thrown and the cluster
   * should be shutdown.
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   */
  private void assignAllUserRegions()
      throws IOException, InterruptedException, KeeperException {
    // Cleanup any existing ZK nodes and start watching
    ZKAssign.deleteAllNodes(watcher);
    ZKUtil.listChildrenAndWatchForNewChildren(this.watcher,
      this.watcher.assignmentZNode);
    failoverCleanupDone();

    // Skip assignment for regions of tables in DISABLING state because during clean cluster startup
    // no RS is alive and regions map also doesn't have any information about the regions.
    // See HBASE-6281.
    Set<String> disabledOrDisablingOrEnabling = ZKTable.getDisabledOrDisablingTables(watcher);
    disabledOrDisablingOrEnabling.addAll(ZKTable.getEnablingTables(watcher));
    // Scan META for all user regions, skipping any disabled tables
    Map<HRegionInfo, ServerName> allRegions = MetaReader.fullScan(
      catalogTracker, disabledOrDisablingOrEnabling, true);
    if (allRegions == null || allRegions.isEmpty()) return;

    // Determine what type of assignment to do on startup
    boolean retainAssignment = server.getConfiguration().
      getBoolean("hbase.master.startup.retainassign", true);

    if (retainAssignment) {
      assign(allRegions);
    } else {
      List<HRegionInfo> regions = new ArrayList<HRegionInfo>(allRegions.keySet());
      assign(regions);
    }

    for (HRegionInfo hri : allRegions.keySet()) {
      String tableName = hri.getTableNameAsString();
      if (!zkTable.isEnabledTable(tableName)) {
        setEnabledTable(tableName);
      }
    }
  }

  /**
   * Wait until no regions in transition.
   * @param timeout How long to wait.
   * @return True if nothing in regions in transition.
   * @throws InterruptedException
   */
  boolean waitUntilNoRegionsInTransition(final long timeout)
      throws InterruptedException {
    // Blocks until there are no regions in transition. It is possible that
    // there
    // are regions in transition immediately after this returns but guarantees
    // that if it returns without an exception that there was a period of time
    // with no regions in transition from the point-of-view of the in-memory
    // state of the Master.
    final long endTime = System.currentTimeMillis() + timeout;

    while (!this.server.isStopped() && regionStates.isRegionsInTransition()
        && endTime > System.currentTimeMillis()) {
      regionStates.waitForUpdate(100);
    }

    return !regionStates.isRegionsInTransition();
  }

  /**
   * Rebuild the list of user regions and assignment information.
   * <p>
   * Returns a map of servers that are not found to be online and the regions
   * they were hosting.
   * @return map of servers not online to their assigned regions, as stored
   *         in META
   * @throws IOException
   */
  Map<ServerName, List<HRegionInfo>> rebuildUserRegions() throws IOException, KeeperException {
    Set<String> enablingTables = ZKTable.getEnablingTables(watcher);
    Set<String> disabledOrEnablingTables = ZKTable.getDisabledTables(watcher);
    disabledOrEnablingTables.addAll(enablingTables);
    Set<String> disabledOrDisablingOrEnabling = ZKTable.getDisablingTables(watcher);
    disabledOrDisablingOrEnabling.addAll(disabledOrEnablingTables);

    // Region assignment from META
    List<Result> results = MetaReader.fullScan(this.catalogTracker);
    // Get any new but slow to checkin region server that joined the cluster
    Set<ServerName> onlineServers = serverManager.getOnlineServers().keySet();
    // Map of offline servers and their regions to be returned
    Map<ServerName, List<HRegionInfo>> offlineServers =
      new TreeMap<ServerName, List<HRegionInfo>>();
    // Iterate regions in META
    for (Result result : results) {
      Pair<HRegionInfo, ServerName> region = HRegionInfo.getHRegionInfoAndServerName(result);
      if (region == null) continue;
      HRegionInfo regionInfo = region.getFirst();
      ServerName regionLocation = region.getSecond();
      if (regionInfo == null) continue;
      regionStates.createRegionState(regionInfo);
      String tableName = regionInfo.getTableNameAsString();
      if (regionLocation == null) {
        // regionLocation could be null if createTable didn't finish properly.
        // When createTable is in progress, HMaster restarts.
        // Some regions have been added to .META., but have not been assigned.
        // When this happens, the region's table must be in ENABLING state.
        // It can't be in ENABLED state as that is set when all regions are
        // assigned.
        // It can't be in DISABLING state, because DISABLING state transitions
        // from ENABLED state when application calls disableTable.
        // It can't be in DISABLED state, because DISABLED states transitions
        // from DISABLING state.
        if (!enablingTables.contains(tableName)) {
          LOG.warn("Region " + regionInfo.getEncodedName() +
            " has null regionLocation." + " But its table " + tableName +
            " isn't in ENABLING state.");
        }
      } else if (!onlineServers.contains(regionLocation)) {
        // Region is located on a server that isn't online
        List<HRegionInfo> offlineRegions = offlineServers.get(regionLocation);
        if (offlineRegions == null) {
          offlineRegions = new ArrayList<HRegionInfo>(1);
          offlineServers.put(regionLocation, offlineRegions);
        }
        offlineRegions.add(regionInfo);
        // need to enable the table if not disabled or disabling or enabling
        // this will be used in rolling restarts
        if (!disabledOrDisablingOrEnabling.contains(tableName)
            && !getZKTable().isEnabledTable(tableName)) {
          setEnabledTable(tableName);
        }
      } else {
        // If region is in offline and split state check the ZKNode
        if (regionInfo.isOffline() && regionInfo.isSplit()) {
          String node = ZKAssign.getNodeName(this.watcher, regionInfo
              .getEncodedName());
          Stat stat = new Stat();
          byte[] data = ZKUtil.getDataNoWatch(this.watcher, node, stat);
          // If znode does not exist, don't consider this region
          if (data == null) {
            LOG.debug("Region "	+  regionInfo.getRegionNameAsString()
               + " split is completed. Hence need not add to regions list");
            continue;
          }
        }
        // Region is being served and on an active server
        // add only if region not in disabled or enabling table
        if (!disabledOrEnablingTables.contains(tableName)) {
          regionStates.regionOnline(regionInfo, regionLocation);
        }
        // need to enable the table if not disabled or disabling or enabling
        // this will be used in rolling restarts
        if (!disabledOrDisablingOrEnabling.contains(tableName)
            && !getZKTable().isEnabledTable(tableName)) {
          setEnabledTable(tableName);
        }
      }
    }
    return offlineServers;
  }

  /**
   * Recover the tables that were not fully moved to DISABLED state. These
   * tables are in DISABLING state when the master restarted/switched.
   *
   * @throws KeeperException
   * @throws TableNotFoundException
   * @throws IOException
   */
  private void recoverTableInDisablingState()
      throws KeeperException, TableNotFoundException, IOException {
    Set<String> disablingTables = ZKTable.getDisablingTables(watcher);
    if (disablingTables.size() != 0) {
      for (String tableName : disablingTables) {
        // Recover by calling DisableTableHandler
        LOG.info("The table " + tableName
            + " is in DISABLING state.  Hence recovering by moving the table"
            + " to DISABLED state.");
        new DisableTableHandler(this.server, tableName.getBytes(),
            catalogTracker, this, true).process();
      }
    }
  }

  /**
   * Recover the tables that are not fully moved to ENABLED state. These tables
   * are in ENABLING state when the master restarted/switched
   *
   * @throws KeeperException
   * @throws TableNotFoundException
   * @throws IOException
   */
  private void recoverTableInEnablingState()
      throws KeeperException, TableNotFoundException, IOException {
    Set<String> enablingTables = ZKTable.getEnablingTables(watcher);
    if (enablingTables.size() != 0) {
      for (String tableName : enablingTables) {
        // Recover by calling EnableTableHandler
        LOG.info("The table " + tableName
            + " is in ENABLING state.  Hence recovering by moving the table"
            + " to ENABLED state.");
        // enableTable in sync way during master startup,
        // no need to invoke coprocessor
        new EnableTableHandler(this.server, tableName.getBytes(),
            catalogTracker, this, true).process();
      }
    }
  }

  /**
   * Processes list of dead servers from result of META scan and regions in RIT
   * <p>
   * This is used for failover to recover the lost regions that belonged to
   * RegionServers which failed while there was no active master or regions
   * that were in RIT.
   * <p>
   *
   * @param deadServers
   *          The list of dead servers which failed while there was no active
   *          master. Can be null.
   * @param nodes
   *          The regions in RIT
   * @throws IOException
   * @throws KeeperException
   */
  private void processDeadServersAndRecoverLostRegions(
      Map<ServerName, List<HRegionInfo>> deadServers, List<String> nodes)
          throws IOException, KeeperException {
    if (deadServers != null) {
      for (Map.Entry<ServerName, List<HRegionInfo>> server: deadServers.entrySet()) {
        ServerName serverName = server.getKey();
        if (!serverManager.isServerDead(serverName)) {
          serverManager.expireServer(serverName); // Let SSH do region re-assign
        }
      }
    }
    nodes = ZKUtil.listChildrenAndWatchForNewChildren(
      this.watcher, this.watcher.assignmentZNode);
    if (!nodes.isEmpty()) {
      for (String encodedRegionName : nodes) {
        processRegionInTransition(encodedRegionName, null);
      }
    }

    // Now we can safely claim failover cleanup completed and enable
    // ServerShutdownHandler for further processing. The nodes (below)
    // in transition, if any, are for regions not related to those
    // dead servers at all, and can be done in parallel to SSH.
    failoverCleanupDone();
  }

  /**
   * Set Regions in transitions metrics.
   * This takes an iterator on the RegionInTransition map (CLSM), and is not synchronized.
   * This iterator is not fail fast, which may lead to stale read; but that's better than
   * creating a copy of the map for metrics computation, as this method will be invoked
   * on a frequent interval.
   */
  public void updateRegionsInTransitionMetrics() {
    long currentTime = System.currentTimeMillis();
    int totalRITs = 0;
    int totalRITsOverThreshold = 0;
    long oldestRITTime = 0;
    int ritThreshold = this.server.getConfiguration().
      getInt(HConstants.METRICS_RIT_STUCK_WARNING_THRESHOLD, 60000);
    for (RegionState state: regionStates.getRegionsInTransition().values()) {
      totalRITs++;
      long ritTime = currentTime - state.getStamp();
      if (ritTime > ritThreshold) { // more than the threshold
        totalRITsOverThreshold++;
      }
      if (oldestRITTime < ritTime) {
        oldestRITTime = ritTime;
      }
    }
    if (this.metricsMaster != null) {
      this.metricsMaster.updateRITOldestAge(oldestRITTime);
      this.metricsMaster.updateRITCount(totalRITs);
      this.metricsMaster.updateRITCountOverThreshold(totalRITsOverThreshold);
    }
  }

  /**
   * @param region Region whose plan we are to clear.
   */
  void clearRegionPlan(final HRegionInfo region) {
    synchronized (this.regionPlans) {
      this.regionPlans.remove(region.getEncodedName());
    }
  }

  /**
   * Wait on region to clear regions-in-transition.
   * @param hri Region to wait on.
   * @throws IOException
   */
  public void waitOnRegionToClearRegionsInTransition(final HRegionInfo hri)
      throws IOException, InterruptedException {
    if (!regionStates.isRegionInTransition(hri)) return;
    RegionState rs = null;
    // There is already a timeout monitor on regions in transition so I
    // should not have to have one here too?
    while(!this.server.isStopped() && regionStates.isRegionInTransition(hri)) {
      LOG.info("Waiting on " + rs + " to clear regions-in-transition");
      regionStates.waitForUpdate(100);
    }
    if (this.server.isStopped()) {
      LOG.info("Giving up wait on regions in " +
        "transition because stoppable.isStopped is set");
    }
  }

  /**
   * Update timers for all regions in transition going against the server in the
   * serversInUpdatingTimer.
   */
  public class TimerUpdater extends Chore {

    public TimerUpdater(final int period, final Stoppable stopper) {
      super("AssignmentTimerUpdater", period, stopper);
    }

    @Override
    protected void chore() {
      ServerName serverToUpdateTimer = null;
      while (!serversInUpdatingTimer.isEmpty() && !stopper.isStopped()) {
        if (serverToUpdateTimer == null) {
          serverToUpdateTimer = serversInUpdatingTimer.first();
        } else {
          serverToUpdateTimer = serversInUpdatingTimer
              .higher(serverToUpdateTimer);
        }
        if (serverToUpdateTimer == null) {
          break;
        }
        updateTimers(serverToUpdateTimer);
        serversInUpdatingTimer.remove(serverToUpdateTimer);
      }
    }
  }

  /**
   * Monitor to check for time outs on region transition operations
   */
  public class TimeoutMonitor extends Chore {
    private boolean allRegionServersOffline = false;
    private ServerManager serverManager;
    private final int timeout;

    /**
     * Creates a periodic monitor to check for time outs on region transition
     * operations.  This will deal with retries if for some reason something
     * doesn't happen within the specified timeout.
     * @param period
   * @param stopper When {@link Stoppable#isStopped()} is true, this thread will
   * cleanup and exit cleanly.
     * @param timeout
     */
    public TimeoutMonitor(final int period, final Stoppable stopper,
        ServerManager serverManager,
        final int timeout) {
      super("AssignmentTimeoutMonitor", period, stopper);
      this.timeout = timeout;
      this.serverManager = serverManager;
    }

    private synchronized void setAllRegionServersOffline(
      boolean allRegionServersOffline) {
      this.allRegionServersOffline = allRegionServersOffline;
    }

    @Override
    protected void chore() {
      boolean noRSAvailable = this.serverManager.createDestinationServersList().isEmpty();

      // Iterate all regions in transition checking for time outs
      long now = System.currentTimeMillis();
      // no lock concurrent access ok: we will be working on a copy, and it's java-valid to do
      //  a copy while another thread is adding/removing items
      for (String regionName : regionStates.getRegionsInTransition().keySet()) {
        RegionState regionState = regionStates.getRegionTransitionState(regionName);
        if (regionState == null) continue;

        if (regionState.getStamp() + timeout <= now) {
          // decide on action upon timeout
          actOnTimeOut(regionState);
        } else if (this.allRegionServersOffline && !noRSAvailable) {
          RegionPlan existingPlan = regionPlans.get(regionName);
          if (existingPlan == null
              || !this.serverManager.isServerOnline(existingPlan
                  .getDestination())) {
            // if some RSs just came back online, we can start the assignment
            // right away
            actOnTimeOut(regionState);
          }
        }
      }
      setAllRegionServersOffline(noRSAvailable);
    }

    private void actOnTimeOut(RegionState regionState) {
      HRegionInfo regionInfo = regionState.getRegion();
      LOG.info("Regions in transition timed out:  " + regionState);
      // Expired! Do a retry.
      switch (regionState.getState()) {
      case CLOSED:
        LOG.info("Region " + regionInfo.getEncodedName()
            + " has been CLOSED for too long, waiting on queued "
            + "ClosedRegionHandler to run or server shutdown");
        // Update our timestamp.
        regionState.updateTimestampToNow();
        break;
      case OFFLINE:
        LOG.info("Region has been OFFLINE for too long, " + "reassigning "
            + regionInfo.getRegionNameAsString() + " to a random server");
        invokeAssign(regionInfo);
        break;
      case PENDING_OPEN:
        LOG.info("Region has been PENDING_OPEN for too "
            + "long, reassigning region=" + regionInfo.getRegionNameAsString());
        invokeAssign(regionInfo);
        break;
      case OPENING:
        processOpeningState(regionInfo);
        break;
      case OPEN:
        LOG.error("Region has been OPEN for too long, " +
            "we don't know where region was opened so can't do anything");
        regionState.updateTimestampToNow();
        break;

      case PENDING_CLOSE:
        LOG.info("Region has been PENDING_CLOSE for too "
            + "long, running forced unassign again on region="
            + regionInfo.getRegionNameAsString());
        invokeUnassign(regionInfo);
        break;
      case CLOSING:
        LOG.info("Region has been CLOSING for too " +
          "long, this should eventually complete or the server will " +
          "expire, send RPC again");
        invokeUnassign(regionInfo);
        break;

      case SPLIT:
      case SPLITTING:
        break;

      default:
        throw new IllegalStateException("Received event is not valid.");
      }
    }
  }

  private void processOpeningState(HRegionInfo regionInfo) {
    LOG.info("Region has been OPENING for too long, reassigning region="
        + regionInfo.getRegionNameAsString());
    // Should have a ZK node in OPENING state
    try {
      String node = ZKAssign.getNodeName(watcher, regionInfo.getEncodedName());
      Stat stat = new Stat();
      byte [] data = ZKAssign.getDataNoWatch(watcher, node, stat);
      if (data == null) {
        LOG.warn("Data is null, node " + node + " no longer exists");
        return;
      }
      RegionTransition rt = RegionTransition.parseFrom(data);
      EventType et = rt.getEventType();
      if (et == EventType.RS_ZK_REGION_OPENED) {
        LOG.debug("Region has transitioned to OPENED, allowing "
            + "watched event handlers to process");
        return;
      } else if (et != EventType.RS_ZK_REGION_OPENING && et != EventType.RS_ZK_REGION_FAILED_OPEN ) {
        LOG.warn("While timing out a region, found ZK node in unexpected state: " + et);
        return;
      }
      invokeAssign(regionInfo);
    } catch (KeeperException ke) {
      LOG.error("Unexpected ZK exception timing out CLOSING region", ke);
      return;
    } catch (DeserializationException e) {
      LOG.error("Unexpected exception parsing CLOSING region", e);
      return;
    }
    return;
  }

  void invokeAssign(HRegionInfo regionInfo) {
    threadPoolExecutorService.submit(new AssignCallable(this, regionInfo));
  }

  private void invokeUnassign(HRegionInfo regionInfo) {
    threadPoolExecutorService.submit(new UnAssignCallable(this, regionInfo));
  }

  public boolean isCarryingRoot(ServerName serverName) {
    return isCarryingRegion(serverName, HRegionInfo.ROOT_REGIONINFO);
  }

  public boolean isCarryingMeta(ServerName serverName) {
    return isCarryingRegion(serverName, HRegionInfo.FIRST_META_REGIONINFO);
  }

  /**
   * Check if the shutdown server carries the specific region.
   * We have a bunch of places that store region location
   * Those values aren't consistent. There is a delay of notification.
   * The location from zookeeper unassigned node has the most recent data;
   * but the node could be deleted after the region is opened by AM.
   * The AM's info could be old when OpenedRegionHandler
   * processing hasn't finished yet when server shutdown occurs.
   * @return whether the serverName currently hosts the region
   */
  private boolean isCarryingRegion(ServerName serverName, HRegionInfo hri) {
    RegionTransition rt = null;
    try {
      byte [] data = ZKAssign.getData(watcher, hri.getEncodedName());
      // This call can legitimately come by null
      rt = data == null? null: RegionTransition.parseFrom(data);
    } catch (KeeperException e) {
      server.abort("Exception reading unassigned node for region=" + hri.getEncodedName(), e);
    } catch (DeserializationException e) {
      server.abort("Exception parsing unassigned node for region=" + hri.getEncodedName(), e);
    }

    ServerName addressFromZK = rt != null? rt.getServerName():  null;
    if (addressFromZK != null) {
      // if we get something from ZK, we will use the data
      boolean matchZK = addressFromZK.equals(serverName);
      LOG.debug("based on ZK, current region=" + hri.getRegionNameAsString() +
          " is on server=" + addressFromZK +
          " server being checked=: " + serverName);
      return matchZK;
    }

    ServerName addressFromAM = regionStates.getRegionServerOfRegion(hri);
    boolean matchAM = (addressFromAM != null &&
      addressFromAM.equals(serverName));
    LOG.debug("based on AM, current region=" + hri.getRegionNameAsString() +
      " is on server=" + (addressFromAM != null ? addressFromAM : "null") +
      " server being checked: " + serverName);

    return matchAM;
  }

  /**
   * Process shutdown server removing any assignments.
   * @param sn Server that went down.
   * @return list of regions in transition on this server
   */
  public List<RegionState> processServerShutdown(final ServerName sn) {
    // Clean out any existing assignment plans for this server
    synchronized (this.regionPlans) {
      for (Iterator <Map.Entry<String, RegionPlan>> i =
          this.regionPlans.entrySet().iterator(); i.hasNext();) {
        Map.Entry<String, RegionPlan> e = i.next();
        ServerName otherSn = e.getValue().getDestination();
        // The name will be null if the region is planned for a random assign.
        if (otherSn != null && otherSn.equals(sn)) {
          // Use iterator's remove else we'll get CME
          i.remove();
        }
      }
    }
    return regionStates.serverOffline(sn);
  }

  /**
   * Update inmemory structures.
   * @param sn Server that reported the split
   * @param parent Parent region that was split
   * @param a Daughter region A
   * @param b Daughter region B
   */
  public void handleSplitReport(final ServerName sn, final HRegionInfo parent,
      final HRegionInfo a, final HRegionInfo b) {
    regionOffline(parent);
    regionOnline(a, sn);
    regionOnline(b, sn);

    // There's a possibility that the region was splitting while a user asked
    // the master to disable, we need to make sure we close those regions in
    // that case. This is not racing with the region server itself since RS
    // report is done after the split transaction completed.
    if (this.zkTable.isDisablingOrDisabledTable(
        parent.getTableNameAsString())) {
      unassign(a);
      unassign(b);
    }
  }

  /**
   * @param plan Plan to execute.
   */
  void balance(final RegionPlan plan) {
    synchronized (this.regionPlans) {
      this.regionPlans.put(plan.getRegionName(), plan);
    }
    unassign(plan.getRegionInfo(), false, plan.getDestination());
  }

  public void stop() {
    this.timeoutMonitor.interrupt();
    this.timerUpdater.interrupt();
  }

  /**
   * Shutdown the threadpool executor service
   */
  public void shutdown() {
    // It's an immediate shutdown, so we're clearing the remaining tasks.
    synchronized (zkEventWorkerWaitingList){
      zkEventWorkerWaitingList.clear();
    }
    threadPoolExecutorService.shutdownNow();
    zkEventWorkers.shutdownNow();
  }

  protected void setEnabledTable(String tableName) {
    try {
      this.zkTable.setEnabledTable(tableName);
    } catch (KeeperException e) {
      // here we can abort as it is the start up flow
      String errorMsg = "Unable to ensure that the table " + tableName
          + " will be" + " enabled because of a ZooKeeper issue";
      LOG.error(errorMsg);
      this.server.abort(errorMsg, e);
    }
  }

  /**
   * Set region as OFFLINED up in zookeeper asynchronously.
   * @param state
   * @return True if we succeeded, false otherwise (State was incorrect or failed
   * updating zk).
   */
  private boolean asyncSetOfflineInZooKeeper(final RegionState state,
      final AsyncCallback.StringCallback cb, final ServerName destination) {
    if (!state.isClosed() && !state.isOffline()) {
      this.server.abort("Unexpected state trying to OFFLINE; " + state,
        new IllegalStateException());
      return false;
    }
    regionStates.updateRegionState(
      state.getRegion(), RegionState.State.OFFLINE);
    try {
      ZKAssign.asyncCreateNodeOffline(watcher, state.getRegion(),
        destination, cb, state);
    } catch (KeeperException e) {
      if (e instanceof NodeExistsException) {
        LOG.warn("Node for " + state.getRegion() + " already exists");
      } else {
        server.abort("Unexpected ZK exception creating/setting node OFFLINE", e);
      }
      return false;
    }
    return true;
  }
}
