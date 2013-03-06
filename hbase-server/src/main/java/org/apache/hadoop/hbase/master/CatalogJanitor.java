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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;

/**
 * A janitor for the catalog tables.  Scans the <code>.META.</code> catalog
 * table on a period looking for unused regions to garbage collect.
 */
@InterfaceAudience.Private
class CatalogJanitor extends Chore {
  private static final Log LOG = LogFactory.getLog(CatalogJanitor.class.getName());
  private final Server server;
  private final MasterServices services;
  private AtomicBoolean enabled = new AtomicBoolean(true);
  private AtomicBoolean alreadyRunning = new AtomicBoolean(false);

  CatalogJanitor(final Server server, final MasterServices services) {
    super(server.getServerName() + "-CatalogJanitor",
      server.getConfiguration().getInt("hbase.catalogjanitor.interval", 300000),
      server);
    this.server = server;
    this.services = services;
  }

  @Override
  protected boolean initialChore() {
    try {
      if (this.enabled.get()) scan();
    } catch (IOException e) {
      LOG.warn("Failed initial scan of catalog table", e);
      return false;
    }
    return true;
  }

  /**
   * @param enabled
   */
  public boolean setEnabled(final boolean enabled) {
    return this.enabled.getAndSet(enabled);
  }

  boolean getEnabled() {
    return this.enabled.get();
  }

  @Override
  protected void chore() {
    try {
      if (this.enabled.get()) {
        scan();
      } else {
        LOG.warn("CatalogJanitor disabled! Not running scan.");
      }
    } catch (IOException e) {
      LOG.warn("Failed scan of catalog table", e);
    }
  }

  /**
   * Scans META and returns a number of scanned rows, and
   * an ordered map of split parents.
   */
  Pair<Integer, Map<HRegionInfo, Result>> getSplitParents() throws IOException {
    // TODO: Only works with single .META. region currently.  Fix.
    final AtomicInteger count = new AtomicInteger(0);
    // Keep Map of found split parents.  There are candidates for cleanup.
    // Use a comparator that has split parents come before its daughters.
    final Map<HRegionInfo, Result> splitParents =
      new TreeMap<HRegionInfo, Result>(new SplitParentFirstComparator());
    // This visitor collects split parents and counts rows in the .META. table
    MetaReader.Visitor visitor = new MetaReader.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r == null || r.isEmpty()) return true;
        count.incrementAndGet();
        HRegionInfo info = HRegionInfo.getHRegionInfo(r);
        if (info == null) return true; // Keep scanning
        if (info.isSplitParent()) splitParents.put(info, r);
        // Returning true means "keep scanning"
        return true;
      }
    };
    // Run full scan of .META. catalog table passing in our custom visitor
    MetaReader.fullScan(this.server.getCatalogTracker(), visitor);

    return new Pair<Integer, Map<HRegionInfo, Result>>(count.get(), splitParents);
  }

  /**
   * Run janitorial scan of catalog <code>.META.</code> table looking for
   * garbage to collect.
   * @throws IOException
   */
  int scan() throws IOException {
    try {
      if (!alreadyRunning.compareAndSet(false, true)) {
        return 0;
      }
      Pair<Integer, Map<HRegionInfo, Result>> pair = getSplitParents();
      int count = pair.getFirst();
      Map<HRegionInfo, Result> splitParents = pair.getSecond();

      // Now work on our list of found parents. See if any we can clean up.
      int cleaned = 0;
    //regions whose parents are still around
      HashSet<String> parentNotCleaned = new HashSet<String>();
      for (Map.Entry<HRegionInfo, Result> e : splitParents.entrySet()) {
        if (!parentNotCleaned.contains(e.getKey().getEncodedName()) &&
            cleanParent(e.getKey(), e.getValue())) {
          cleaned++;
        } else {
          // We could not clean the parent, so it's daughters should not be cleaned either (HBASE-6160)
          PairOfSameType<HRegionInfo> daughters = HRegionInfo.getDaughterRegions(e.getValue());
          parentNotCleaned.add(daughters.getFirst().getEncodedName());
          parentNotCleaned.add(daughters.getSecond().getEncodedName());
        }
      }
      if (cleaned != 0) {
        LOG.info("Scanned " + count + " catalog row(s) and gc'd " + cleaned +
            " unreferenced parent region(s)");
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("Scanned " + count + " catalog row(s) and gc'd " + cleaned +
            " unreferenced parent region(s)");
      }
      return cleaned;
    } finally {
      alreadyRunning.set(false);
    }
  }

  /**
   * Compare HRegionInfos in a way that has split parents sort BEFORE their
   * daughters.
   */
  static class SplitParentFirstComparator implements Comparator<HRegionInfo> {
    @Override
    public int compare(HRegionInfo left, HRegionInfo right) {
      // This comparator differs from the one HRegionInfo in that it sorts
      // parent before daughters.
      if (left == null) return -1;
      if (right == null) return 1;
      // Same table name.
      int result = Bytes.compareTo(left.getTableName(),
          right.getTableName());
      if (result != 0) return result;
      // Compare start keys.
      result = Bytes.compareTo(left.getStartKey(), right.getStartKey());
      if (result != 0) return result;
      // Compare end keys.
      result = Bytes.compareTo(left.getEndKey(), right.getEndKey());
      if (result != 0) {
        if (left.getStartKey().length != 0
                && left.getEndKey().length == 0) {
            return -1;  // left is last region
        }
        if (right.getStartKey().length != 0
                && right.getEndKey().length == 0) {
            return 1;  // right is the last region
        }
        return -result; // Flip the result so parent comes first.
      }
      return result;
    }
  }

  /**
   * If daughters no longer hold reference to the parents, delete the parent.
   * @param parent HRegionInfo of split offlined parent
   * @param rowContent Content of <code>parent</code> row in
   * <code>metaRegionName</code>
   * @return True if we removed <code>parent</code> from meta table and from
   * the filesystem.
   * @throws IOException
   */
  boolean cleanParent(final HRegionInfo parent, Result rowContent)
  throws IOException {
    boolean result = false;
    // Run checks on each daughter split.
    PairOfSameType<HRegionInfo> daughters = HRegionInfo.getDaughterRegions(rowContent);
    Pair<Boolean, Boolean> a =
      checkDaughterInFs(parent, daughters.getFirst());
    Pair<Boolean, Boolean> b =
      checkDaughterInFs(parent, daughters.getSecond());
    if (hasNoReferences(a) && hasNoReferences(b)) {
      LOG.debug("Deleting region " + parent.getRegionNameAsString() +
        " because daughter splits no longer hold references");
      // wipe out daughter references from parent region in meta
      removeDaughtersFromParent(parent);

      // This latter regionOffline should not be necessary but is done for now
      // until we let go of regionserver to master heartbeats.  See HBASE-3368.
      if (this.services.getAssignmentManager() != null) {
        // The mock used in testing catalogjanitor returns null for getAssignmnetManager.
        // Allow for null result out of getAssignmentManager.
        this.services.getAssignmentManager().regionOffline(parent);
      }
      FileSystem fs = this.services.getMasterFileSystem().getFileSystem();
      LOG.debug("Archiving parent region:" + parent);
      HFileArchiver.archiveRegion(this.services.getConfiguration(), fs, parent);
      MetaEditor.deleteRegion(this.server.getCatalogTracker(), parent);
      result = true;
    }
    return result;
  }

  /**
   * @param p A pair where the first boolean says whether or not the daughter
   * region directory exists in the filesystem and then the second boolean says
   * whether the daughter has references to the parent.
   * @return True the passed <code>p</code> signifies no references.
   */
  private boolean hasNoReferences(final Pair<Boolean, Boolean> p) {
    return !p.getFirst() || !p.getSecond();
  }

  /**
   * Remove mention of daughters from parent row.
   * @param parent
   * @throws IOException
   */
  private void removeDaughtersFromParent(final HRegionInfo parent)
  throws IOException {
    MetaEditor.deleteDaughtersReferencesInParent(this.server.getCatalogTracker(), parent);
  }

  /**
   * Checks if a daughter region -- either splitA or splitB -- still holds
   * references to parent.
   * @param parent Parent region
   * @param daughter Daughter region
   * @return A pair where the first boolean says whether or not the daughter
   * region directory exists in the filesystem and then the second boolean says
   * whether the daughter has references to the parent.
   * @throws IOException
   */
  Pair<Boolean, Boolean> checkDaughterInFs(final HRegionInfo parent, final HRegionInfo daughter)
  throws IOException {
    boolean references = false;
    boolean exists = false;
    if (daughter == null)  {
      return new Pair<Boolean, Boolean>(Boolean.FALSE, Boolean.FALSE);
    }
    FileSystem fs = this.services.getMasterFileSystem().getFileSystem();
    Path rootdir = this.services.getMasterFileSystem().getRootDir();
    Path tabledir = new Path(rootdir, daughter.getTableNameAsString());
    Path regiondir = new Path(tabledir, daughter.getEncodedName());
    exists = fs.exists(regiondir);
    if (!exists) {
      LOG.warn("Daughter regiondir does not exist: " + regiondir.toString());
      return new Pair<Boolean, Boolean>(exists, Boolean.FALSE);
    }
    HTableDescriptor parentDescriptor = getTableDescriptor(parent.getTableName());

    for (HColumnDescriptor family: parentDescriptor.getFamilies()) {
      Path p = HStore.getStoreHomedir(tabledir, daughter.getEncodedName(),
        family.getName());
      if (!fs.exists(p)) continue;
      // Look for reference files.  Call listStatus with anonymous instance of PathFilter.
      FileStatus [] ps = FSUtils.listStatus(fs, p,
          new PathFilter () {
            public boolean accept(Path path) {
              return StoreFile.isReference(path);
            }
          }
      );

      if (ps != null && ps.length > 0) {
        references = true;
        break;
      }
    }
    return new Pair<Boolean, Boolean>(Boolean.valueOf(exists),
      Boolean.valueOf(references));
  }

  private HTableDescriptor getTableDescriptor(byte[] tableName)
  throws FileNotFoundException, IOException {
    return this.services.getTableDescriptors().get(Bytes.toString(tableName));
  }
}
