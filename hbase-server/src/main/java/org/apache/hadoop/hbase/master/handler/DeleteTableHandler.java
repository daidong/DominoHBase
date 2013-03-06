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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;

@InterfaceAudience.Private
public class DeleteTableHandler extends TableEventHandler {
  private static final Log LOG = LogFactory.getLog(DeleteTableHandler.class);

  public DeleteTableHandler(byte [] tableName, Server server,
      final MasterServices masterServices)
  throws IOException {
    super(EventType.C_M_DELETE_TABLE, tableName, server, masterServices);
    // The next call fails if no such table.
    getTableDescriptor();
  }

  @Override
  protected void handleTableOperation(List<HRegionInfo> regions)
  throws IOException, KeeperException {
    MasterCoprocessorHost cpHost = ((HMaster) this.server)
        .getCoprocessorHost();
    if (cpHost != null) {
      cpHost.preDeleteTableHandler(this.tableName);
    }
    AssignmentManager am = this.masterServices.getAssignmentManager();
    long waitTime = server.getConfiguration().
      getLong("hbase.master.wait.on.region", 5 * 60 * 1000);
    for (HRegionInfo region : regions) {
      long done = System.currentTimeMillis() + waitTime;
      while (System.currentTimeMillis() < done) {
        if (!am.getRegionStates().isRegionInTransition(region)) break;
        Threads.sleep(waitingTimeForEvents);
        LOG.debug("Waiting on region to clear regions in transition; "
          + am.getRegionStates().getRegionTransitionState(region));
      }
      if (am.getRegionStates().isRegionInTransition(region)) {
        throw new IOException("Waited hbase.master.wait.on.region (" +
          waitTime + "ms) for region to leave region " +
          region.getRegionNameAsString() + " in transitions");
      }
      LOG.debug("Deleting region " + region.getRegionNameAsString() +
        " from META and FS");
      // Remove region from META
      MetaEditor.deleteRegion(this.server.getCatalogTracker(), region);
      // Delete region from FS
      this.masterServices.getMasterFileSystem().deleteRegion(region);
    }
    // Delete table from FS
    this.masterServices.getMasterFileSystem().deleteTable(tableName);
    // Update table descriptor cache
    this.masterServices.getTableDescriptors().remove(Bytes.toString(tableName));

    // If entry for this table in zk, and up in AssignmentManager, remove it.

    am.getZKTable().setDeletedTable(Bytes.toString(tableName));
    if (cpHost != null) {
      cpHost.postDeleteTableHandler(this.tableName);
    }
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-" + tableNameStr;
  }
}
