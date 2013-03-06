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
package org.apache.hadoop.hbase.protobuf;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest.RegionOpenInfo;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry.WALEdit.FamilyScope;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry.WALEdit.ScopeType;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry.WALKey;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest.FamilyPath;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Condition;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.LockRowRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiAction;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.ColumnValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.MutateType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.UnlockRowRequest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.CompareType;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.AddColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.BalanceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.CatalogScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DisableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.EnableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.ByteString;

/**
 * Helper utility to build protocol buffer requests,
 * or build components for protocol buffer requests.
 */
@InterfaceAudience.Private
public final class RequestConverter {

  private RequestConverter() {
  }

// Start utilities for Client

/**
   * Create a new protocol buffer GetRequest to get a row, all columns in a family.
   * If there is no such row, return the closest row before it.
   *
   * @param regionName the name of the region to get
   * @param row the row to get
   * @param family the column family to get
   * should return the immediate row before
   * @return a protocol buffer GetReuqest
   */
  public static GetRequest buildGetRowOrBeforeRequest(
      final byte[] regionName, final byte[] row, final byte[] family) {
    GetRequest.Builder builder = GetRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setClosestRowBefore(true);
    builder.setRegion(region);

    Column.Builder columnBuilder = Column.newBuilder();
    columnBuilder.setFamily(ByteString.copyFrom(family));
    ClientProtos.Get.Builder getBuilder =
      ClientProtos.Get.newBuilder();
    getBuilder.setRow(ByteString.copyFrom(row));
    getBuilder.addColumn(columnBuilder.build());
    builder.setGet(getBuilder.build());
    return builder.build();
  }

  /**
   * Create a protocol buffer GetRequest for a client Get
   *
   * @param regionName the name of the region to get
   * @param get the client Get
   * @return a protocol buffer GetReuqest
   */
  public static GetRequest buildGetRequest(final byte[] regionName,
      final Get get) throws IOException {
    return buildGetRequest(regionName, get, false);
  }

  /**
   * Create a protocol buffer GetRequest for a client Get
   *
   * @param regionName the name of the region to get
   * @param get the client Get
   * @param existenceOnly indicate if check row existence only
   * @return a protocol buffer GetReuqest
   */
  public static GetRequest buildGetRequest(final byte[] regionName,
      final Get get, final boolean existenceOnly) throws IOException {
    GetRequest.Builder builder = GetRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setExistenceOnly(existenceOnly);
    builder.setRegion(region);
    builder.setGet(ProtobufUtil.toGet(get));
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for a client increment
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param amount
   * @param writeToWAL
   * @return a mutate request
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final byte[] row, final byte[] family,
      final byte [] qualifier, final long amount, final boolean writeToWAL) {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);

    Mutate.Builder mutateBuilder = Mutate.newBuilder();
    mutateBuilder.setRow(ByteString.copyFrom(row));
    mutateBuilder.setMutateType(MutateType.INCREMENT);
    mutateBuilder.setWriteToWAL(writeToWAL);
    ColumnValue.Builder columnBuilder = ColumnValue.newBuilder();
    columnBuilder.setFamily(ByteString.copyFrom(family));
    QualifierValue.Builder valueBuilder = QualifierValue.newBuilder();
    valueBuilder.setValue(ByteString.copyFrom(Bytes.toBytes(amount)));
    valueBuilder.setQualifier(ByteString.copyFrom(qualifier));
    columnBuilder.addQualifierValue(valueBuilder.build());
    mutateBuilder.addColumnValue(columnBuilder.build());

    builder.setMutate(mutateBuilder.build());
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for a conditioned put
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param comparator
   * @param compareType
   * @param put
   * @return a mutate request
   * @throws IOException
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final byte[] row, final byte[] family,
      final byte [] qualifier, final ByteArrayComparable comparator,
      final CompareType compareType, final Put put) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    Condition condition = buildCondition(
      row, family, qualifier, comparator, compareType);
    builder.setMutate(ProtobufUtil.toMutate(MutateType.PUT, put));
    builder.setCondition(condition);
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for a conditioned delete
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param comparator
   * @param compareType
   * @param delete
   * @return a mutate request
   * @throws IOException
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final byte[] row, final byte[] family,
      final byte [] qualifier, final ByteArrayComparable comparator,
      final CompareType compareType, final Delete delete) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    Condition condition = buildCondition(
      row, family, qualifier, comparator, compareType);
    builder.setMutate(ProtobufUtil.toMutate(MutateType.DELETE, delete));
    builder.setCondition(condition);
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for a put
   *
   * @param regionName
   * @param put
   * @return a mutate request
   * @throws IOException
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final Put put) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setMutate(ProtobufUtil.toMutate(MutateType.PUT, put));
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for an append
   *
   * @param regionName
   * @param append
   * @return a mutate request
   * @throws IOException
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final Append append) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setMutate(ProtobufUtil.toMutate(MutateType.APPEND, append));
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for a client increment
   *
   * @param regionName
   * @param increment
   * @return a mutate request
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final Increment increment) {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setMutate(ProtobufUtil.toMutate(increment));
    return builder.build();
  }

  /**
   * Create a protocol buffer MutateRequest for a delete
   *
   * @param regionName
   * @param delete
   * @return a mutate request
   * @throws IOException
   */
  public static MutateRequest buildMutateRequest(
      final byte[] regionName, final Delete delete) throws IOException {
    MutateRequest.Builder builder = MutateRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setMutate(ProtobufUtil.toMutate(MutateType.DELETE, delete));
    return builder.build();
  }

  /**
   * Create a protocol buffer MultiRequest for a row mutations
   *
   * @param regionName
   * @param rowMutations
   * @return a multi request
   * @throws IOException
   */
  public static MultiRequest buildMultiRequest(final byte[] regionName,
      final RowMutations rowMutations) throws IOException {
    MultiRequest.Builder builder = MultiRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setAtomic(true);
    for (Mutation mutation: rowMutations.getMutations()) {
      MutateType mutateType = null;
      if (mutation instanceof Put) {
        mutateType = MutateType.PUT;
      } else if (mutation instanceof Delete) {
        mutateType = MutateType.DELETE;
      } else {
        throw new DoNotRetryIOException(
          "RowMutations supports only put and delete, not "
            + mutation.getClass().getName());
      }
      Mutate mutate = ProtobufUtil.toMutate(mutateType, mutation);
      builder.addAction(MultiAction.newBuilder().setMutate(mutate).build());
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer ScanRequest for a client Scan
   *
   * @param regionName
   * @param scan
   * @param numberOfRows
   * @param closeScanner
   * @return a scan request
   * @throws IOException
   */
  public static ScanRequest buildScanRequest(final byte[] regionName,
      final Scan scan, final int numberOfRows,
        final boolean closeScanner) throws IOException {
    ScanRequest.Builder builder = ScanRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setNumberOfRows(numberOfRows);
    builder.setCloseScanner(closeScanner);
    builder.setRegion(region);
    builder.setScan(ProtobufUtil.toScan(scan));
    return builder.build();
  }

  /**
   * Create a protocol buffer ScanRequest for a scanner id
   *
   * @param scannerId
   * @param numberOfRows
   * @param closeScanner
   * @return a scan request
   */
  public static ScanRequest buildScanRequest(final long scannerId,
      final int numberOfRows, final boolean closeScanner) {
    ScanRequest.Builder builder = ScanRequest.newBuilder();
    builder.setNumberOfRows(numberOfRows);
    builder.setCloseScanner(closeScanner);
    builder.setScannerId(scannerId);
    return builder.build();
  }
  
  /**
   * Create a protocol buffer ScanRequest for a scanner id
   * 
   * @param scannerId
   * @param numberOfRows
   * @param closeScanner
   * @param nextCallSeq
   * @return a scan request
   */
  public static ScanRequest buildScanRequest(final long scannerId, final int numberOfRows,
      final boolean closeScanner, final long nextCallSeq) {
    ScanRequest.Builder builder = ScanRequest.newBuilder();
    builder.setNumberOfRows(numberOfRows);
    builder.setCloseScanner(closeScanner);
    builder.setScannerId(scannerId);
    builder.setNextCallSeq(nextCallSeq);
    return builder.build();
  }

  /**
   * Create a protocol buffer LockRowRequest
   *
   * @param regionName
   * @param row
   * @return a lock row request
   */
  public static LockRowRequest buildLockRowRequest(
      final byte[] regionName, final byte[] row) {
    LockRowRequest.Builder builder = LockRowRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.addRow(ByteString.copyFrom(row));
    return builder.build();
  }

  /**
   * Create a protocol buffer UnlockRowRequest
   *
   * @param regionName
   * @param lockId
   * @return a unlock row request
   */
  public static UnlockRowRequest buildUnlockRowRequest(
      final byte[] regionName, final long lockId) {
    UnlockRowRequest.Builder builder = UnlockRowRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setLockId(lockId);
    return builder.build();
  }

  /**
   * Create a protocol buffer bulk load request
   *
   * @param familyPaths
   * @param regionName
   * @param assignSeqNum
   * @return a bulk load request
   */
  public static BulkLoadHFileRequest buildBulkLoadHFileRequest(
      final List<Pair<byte[], String>> familyPaths,
      final byte[] regionName, boolean assignSeqNum) {
    BulkLoadHFileRequest.Builder builder = BulkLoadHFileRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    FamilyPath.Builder familyPathBuilder = FamilyPath.newBuilder();
    for (Pair<byte[], String> familyPath: familyPaths) {
      familyPathBuilder.setFamily(ByteString.copyFrom(familyPath.getFirst()));
      familyPathBuilder.setPath(familyPath.getSecond());
      builder.addFamilyPath(familyPathBuilder.build());
    }
    builder.setAssignSeqNum(assignSeqNum);
    return builder.build();
  }

  /**
   * Create a protocol buffer multi request for a list of actions.
   * RowMutations in the list (if any) will be ignored.
   *
   * @param regionName
   * @param actions
   * @return a multi request
   * @throws IOException
   */
  public static <R> MultiRequest buildMultiRequest(final byte[] regionName,
      final List<Action<R>> actions) throws IOException {
    MultiRequest.Builder builder = MultiRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    for (Action<R> action: actions) {
      MultiAction.Builder protoAction = MultiAction.newBuilder();

      Row row = action.getAction();
      if (row instanceof Get) {
        protoAction.setGet(ProtobufUtil.toGet((Get)row));
      } else if (row instanceof Put) {
        protoAction.setMutate(ProtobufUtil.toMutate(MutateType.PUT, (Put)row));
      } else if (row instanceof Delete) {
        protoAction.setMutate(ProtobufUtil.toMutate(MutateType.DELETE, (Delete)row));
      } else if (row instanceof Append) {
        protoAction.setMutate(ProtobufUtil.toMutate(MutateType.APPEND, (Append)row));
      } else if (row instanceof Increment) {
        protoAction.setMutate(ProtobufUtil.toMutate((Increment)row));
      } else if (row instanceof RowMutations) {
        continue; // ignore RowMutations
      } else {
        throw new DoNotRetryIOException(
          "multi doesn't support " + row.getClass().getName());
      }
      builder.addAction(protoAction.build());
    }
    return builder.build();
  }

// End utilities for Client
//Start utilities for Admin

  /**
   * Create a protocol buffer GetRegionInfoRequest for a given region name
   *
   * @param regionName the name of the region to get info
   * @return a protocol buffer GetRegionInfoRequest
   */
  public static GetRegionInfoRequest
      buildGetRegionInfoRequest(final byte[] regionName) {
    return buildGetRegionInfoRequest(regionName, false);
  }

  /**
   * Create a protocol buffer GetRegionInfoRequest for a given region name
   *
   * @param regionName the name of the region to get info
   * @param includeCompactionState indicate if the compaction state is requested
   * @return a protocol buffer GetRegionInfoRequest
   */
  public static GetRegionInfoRequest
      buildGetRegionInfoRequest(final byte[] regionName,
        final boolean includeCompactionState) {
    GetRegionInfoRequest.Builder builder = GetRegionInfoRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    if (includeCompactionState) {
      builder.setCompactionState(includeCompactionState);
    }
    return builder.build();
  }

 /**
  * Create a protocol buffer GetStoreFileRequest for a given region name
  *
  * @param regionName the name of the region to get info
  * @param family the family to get store file list
  * @return a protocol buffer GetStoreFileRequest
  */
 public static GetStoreFileRequest
     buildGetStoreFileRequest(final byte[] regionName, final byte[] family) {
   GetStoreFileRequest.Builder builder = GetStoreFileRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   builder.addFamily(ByteString.copyFrom(family));
   return builder.build();
 }

 /**
  * Create a protocol buffer GetOnlineRegionRequest
  *
  * @return a protocol buffer GetOnlineRegionRequest
  */
 public static GetOnlineRegionRequest buildGetOnlineRegionRequest() {
   return GetOnlineRegionRequest.newBuilder().build();
 }

 /**
  * Create a protocol buffer FlushRegionRequest for a given region name
  *
  * @param regionName the name of the region to get info
  * @return a protocol buffer FlushRegionRequest
  */
 public static FlushRegionRequest
     buildFlushRegionRequest(final byte[] regionName) {
   FlushRegionRequest.Builder builder = FlushRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   return builder.build();
 }

 /**
  * Create a protocol buffer OpenRegionRequest to open a list of regions
  *
  * @param regionOpenInfos info of a list of regions to open
  * @return a protocol buffer OpenRegionRequest
  */
 public static OpenRegionRequest
     buildOpenRegionRequest(final List<Pair<HRegionInfo, Integer>> regionOpenInfos) {
   OpenRegionRequest.Builder builder = OpenRegionRequest.newBuilder();
   for (Pair<HRegionInfo, Integer> regionOpenInfo: regionOpenInfos) {
     Integer second = regionOpenInfo.getSecond();
     int versionOfOfflineNode = second == null ? -1 : second.intValue();
     builder.addOpenInfo(buildRegionOpenInfo(
       regionOpenInfo.getFirst(), versionOfOfflineNode));
   }
   return builder.build();
 }

 /**
  * Create a protocol buffer OpenRegionRequest for a given region
  *
  * @param region the region to open
  * @param versionOfOfflineNode that needs to be present in the offline node
  * @return a protocol buffer OpenRegionRequest
  */
 public static OpenRegionRequest buildOpenRegionRequest(
     final HRegionInfo region, final int versionOfOfflineNode) {
   OpenRegionRequest.Builder builder = OpenRegionRequest.newBuilder();
   builder.addOpenInfo(buildRegionOpenInfo(region, versionOfOfflineNode));
   return builder.build();
 }

 /**
  * Create a CloseRegionRequest for a given region name
  *
  * @param regionName the name of the region to close
  * @param transitionInZK indicator if to transition in ZK
  * @return a CloseRegionRequest
  */
 public static CloseRegionRequest buildCloseRegionRequest(
     final byte[] regionName, final boolean transitionInZK) {
   CloseRegionRequest.Builder builder = CloseRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   builder.setTransitionInZK(transitionInZK);
   return builder.build();
 }

  public static CloseRegionRequest buildCloseRegionRequest(
    final byte[] regionName, final int versionOfClosingNode,
    ServerName destinationServer, final boolean transitionInZK) {
    CloseRegionRequest.Builder builder = CloseRegionRequest.newBuilder();
    RegionSpecifier region = buildRegionSpecifier(
      RegionSpecifierType.REGION_NAME, regionName);
    builder.setRegion(region);
    builder.setVersionOfClosingNode(versionOfClosingNode);
    builder.setTransitionInZK(transitionInZK);
    if (destinationServer != null){
      builder.setDestinationServer(ProtobufUtil.toServerName( destinationServer) );
    }
    return builder.build();
  }

 /**
  * Create a CloseRegionRequest for a given encoded region name
  *
  * @param encodedRegionName the name of the region to close
  * @param transitionInZK indicator if to transition in ZK
  * @return a CloseRegionRequest
  */
 public static CloseRegionRequest
     buildCloseRegionRequest(final String encodedRegionName,
       final boolean transitionInZK) {
   CloseRegionRequest.Builder builder = CloseRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.ENCODED_REGION_NAME,
     Bytes.toBytes(encodedRegionName));
   builder.setRegion(region);
   builder.setTransitionInZK(transitionInZK);
   return builder.build();
 }

 /**
  * Create a SplitRegionRequest for a given region name
  *
  * @param regionName the name of the region to split
  * @param splitPoint the split point
  * @return a SplitRegionRequest
  */
 public static SplitRegionRequest buildSplitRegionRequest(
     final byte[] regionName, final byte[] splitPoint) {
   SplitRegionRequest.Builder builder = SplitRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   if (splitPoint != null) {
     builder.setSplitPoint(ByteString.copyFrom(splitPoint));
   }
   return builder.build();
 }

 /**
  * Create a  CompactRegionRequest for a given region name
  *
  * @param regionName the name of the region to get info
  * @param major indicator if it is a major compaction
  * @return a CompactRegionRequest
  */
 public static CompactRegionRequest buildCompactRegionRequest(
     final byte[] regionName, final boolean major, final byte [] family) {
   CompactRegionRequest.Builder builder = CompactRegionRequest.newBuilder();
   RegionSpecifier region = buildRegionSpecifier(
     RegionSpecifierType.REGION_NAME, regionName);
   builder.setRegion(region);
   builder.setMajor(major);
   if (family != null) {
     builder.setFamily(ByteString.copyFrom(family));
   }
   return builder.build();
 }

 /**
  * Create a new ReplicateWALEntryRequest from a list of HLog entries
  *
  * @param entries the HLog entries to be replicated
  * @return a ReplicateWALEntryRequest
  */
 public static ReplicateWALEntryRequest
     buildReplicateWALEntryRequest(final HLog.Entry[] entries) {
   FamilyScope.Builder scopeBuilder = FamilyScope.newBuilder();
   WALEntry.Builder entryBuilder = WALEntry.newBuilder();
   ReplicateWALEntryRequest.Builder builder =
     ReplicateWALEntryRequest.newBuilder();
   for (HLog.Entry entry: entries) {
     entryBuilder.clear();
     WALKey.Builder keyBuilder = entryBuilder.getKeyBuilder();
     HLogKey key = entry.getKey();
     keyBuilder.setEncodedRegionName(
       ByteString.copyFrom(key.getEncodedRegionName()));
     keyBuilder.setTableName(ByteString.copyFrom(key.getTablename()));
     keyBuilder.setLogSequenceNumber(key.getLogSeqNum());
     keyBuilder.setWriteTime(key.getWriteTime());
     UUID clusterId = key.getClusterId();
     if (clusterId != null) {
       AdminProtos.UUID.Builder uuidBuilder = keyBuilder.getClusterIdBuilder();
       uuidBuilder.setLeastSigBits(clusterId.getLeastSignificantBits());
       uuidBuilder.setMostSigBits(clusterId.getMostSignificantBits());
     }
     WALEdit edit = entry.getEdit();
     WALEntry.WALEdit.Builder editBuilder = entryBuilder.getEditBuilder();
     NavigableMap<byte[], Integer> scopes = edit.getScopes();
     if (scopes != null && !scopes.isEmpty()) {
       for (Map.Entry<byte[], Integer> scope: scopes.entrySet()) {
         scopeBuilder.setFamily(ByteString.copyFrom(scope.getKey()));
         ScopeType scopeType = ScopeType.valueOf(scope.getValue().intValue());
         scopeBuilder.setScopeType(scopeType);
         editBuilder.addFamilyScope(scopeBuilder.build());
       }
     }
     List<KeyValue> keyValues = edit.getKeyValues();
     for (KeyValue value: keyValues) {
       editBuilder.addKeyValueBytes(ByteString.copyFrom(
         value.getBuffer(), value.getOffset(), value.getLength()));
     }
     builder.addEntry(entryBuilder.build());
   }
   return builder.build();
 }

 /**
  * Create a new RollWALWriterRequest
  *
  * @return a ReplicateWALEntryRequest
  */
 public static RollWALWriterRequest buildRollWALWriterRequest() {
   RollWALWriterRequest.Builder builder = RollWALWriterRequest.newBuilder();
   return builder.build();
 }

 /**
  * Create a new GetServerInfoRequest
  *
  * @return a GetServerInfoRequest
  */
 public static GetServerInfoRequest buildGetServerInfoRequest() {
   GetServerInfoRequest.Builder builder =  GetServerInfoRequest.newBuilder();
   return builder.build();
 }

 /**
  * Create a new StopServerRequest
  *
  * @param reason the reason to stop the server
  * @return a StopServerRequest
  */
 public static StopServerRequest buildStopServerRequest(final String reason) {
   StopServerRequest.Builder builder = StopServerRequest.newBuilder();
   builder.setReason(reason);
   return builder.build();
 }

//End utilities for Admin

  /**
   * Convert a byte array to a protocol buffer RegionSpecifier
   *
   * @param type the region specifier type
   * @param value the region specifier byte array value
   * @return a protocol buffer RegionSpecifier
   */
  public static RegionSpecifier buildRegionSpecifier(
      final RegionSpecifierType type, final byte[] value) {
    RegionSpecifier.Builder regionBuilder = RegionSpecifier.newBuilder();
    regionBuilder.setValue(ByteString.copyFrom(value));
    regionBuilder.setType(type);
    return regionBuilder.build();
  }

  /**
   * Create a protocol buffer Condition
   *
   * @param row
   * @param family
   * @param qualifier
   * @param comparator
   * @param compareType
   * @return a Condition
   * @throws IOException
   */
  private static Condition buildCondition(final byte[] row,
      final byte[] family, final byte [] qualifier,
      final ByteArrayComparable comparator,
      final CompareType compareType) throws IOException {
    Condition.Builder builder = Condition.newBuilder();
    builder.setRow(ByteString.copyFrom(row));
    builder.setFamily(ByteString.copyFrom(family));
    builder.setQualifier(ByteString.copyFrom(qualifier));
    builder.setComparator(ProtobufUtil.toComparator(comparator));
    builder.setCompareType(compareType);
    return builder.build();
  }

  /**
   * Create a protocol buffer AddColumnRequest
   *
   * @param tableName
   * @param column
   * @return an AddColumnRequest
   */
  public static AddColumnRequest buildAddColumnRequest(
      final byte [] tableName, final HColumnDescriptor column) {
    AddColumnRequest.Builder builder = AddColumnRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    builder.setColumnFamilies(column.convert());
    return builder.build();
  }

  /**
   * Create a protocol buffer DeleteColumnRequest
   *
   * @param tableName
   * @param columnName
   * @return a DeleteColumnRequest
   */
  public static DeleteColumnRequest buildDeleteColumnRequest(
      final byte [] tableName, final byte [] columnName) {
    DeleteColumnRequest.Builder builder = DeleteColumnRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    builder.setColumnName(ByteString.copyFrom(columnName));
    return builder.build();
  }

  /**
   * Create a protocol buffer ModifyColumnRequest
   *
   * @param tableName
   * @param column
   * @return an ModifyColumnRequest
   */
  public static ModifyColumnRequest buildModifyColumnRequest(
      final byte [] tableName, final HColumnDescriptor column) {
    ModifyColumnRequest.Builder builder = ModifyColumnRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    builder.setColumnFamilies(column.convert());
    return builder.build();
  }

  /**
   * Create a protocol buffer MoveRegionRequest
   *
   * @param encodedRegionName
   * @param destServerName
   * @return A MoveRegionRequest
   * @throws DeserializationException
   */
  public static MoveRegionRequest buildMoveRegionRequest(
      final byte [] encodedRegionName, final byte [] destServerName) throws DeserializationException {
	MoveRegionRequest.Builder builder = MoveRegionRequest.newBuilder();
    builder.setRegion(
      buildRegionSpecifier(RegionSpecifierType.ENCODED_REGION_NAME,encodedRegionName));
    if (destServerName != null) {
      builder.setDestServerName(
        ProtobufUtil.toServerName(new ServerName(Bytes.toString(destServerName))));
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer AssignRegionRequest
   *
   * @param regionName
   * @return an AssignRegionRequest
   */
  public static AssignRegionRequest buildAssignRegionRequest(final byte [] regionName) {
    AssignRegionRequest.Builder builder = AssignRegionRequest.newBuilder();
    builder.setRegion(buildRegionSpecifier(RegionSpecifierType.REGION_NAME,regionName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer UnassignRegionRequest
   *
   * @param regionName
   * @param force
   * @return an UnassignRegionRequest
   */
  public static UnassignRegionRequest buildUnassignRegionRequest(
      final byte [] regionName, final boolean force) {
    UnassignRegionRequest.Builder builder = UnassignRegionRequest.newBuilder();
    builder.setRegion(buildRegionSpecifier(RegionSpecifierType.REGION_NAME,regionName));
    builder.setForce(force);
    return builder.build();
  }

  /**
   * Creates a protocol buffer OfflineRegionRequest
   *
   * @param regionName
   * @return an OfflineRegionRequest
   */
  public static OfflineRegionRequest buildOfflineRegionRequest(final byte [] regionName) {
    OfflineRegionRequest.Builder builder = OfflineRegionRequest.newBuilder();
    builder.setRegion(buildRegionSpecifier(RegionSpecifierType.REGION_NAME,regionName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer DeleteTableRequest
   *
   * @param tableName
   * @return a DeleteTableRequest
   */
  public static DeleteTableRequest buildDeleteTableRequest(final byte [] tableName) {
    DeleteTableRequest.Builder builder = DeleteTableRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer EnableTableRequest
   *
   * @param tableName
   * @return an EnableTableRequest
   */
  public static EnableTableRequest buildEnableTableRequest(final byte [] tableName) {
    EnableTableRequest.Builder builder = EnableTableRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer DisableTableRequest
   *
   * @param tableName
   * @return a DisableTableRequest
   */
  public static DisableTableRequest buildDisableTableRequest(final byte [] tableName) {
    DisableTableRequest.Builder builder = DisableTableRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer CreateTableRequest
   *
   * @param hTableDesc
   * @param splitKeys
   * @return a CreateTableRequest
   */
  public static CreateTableRequest buildCreateTableRequest(
      final HTableDescriptor hTableDesc, final byte [][] splitKeys) {
    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    builder.setTableSchema(hTableDesc.convert());
    if (splitKeys != null) {
      for (byte [] splitKey : splitKeys) {
        builder.addSplitKeys(ByteString.copyFrom(splitKey));
      }
    }
    return builder.build();
  }


  /**
   * Creates a protocol buffer ModifyTableRequest
   *
   * @param table
   * @param hTableDesc
   * @return a ModifyTableRequest
   */
  public static ModifyTableRequest buildModifyTableRequest(
      final byte [] table, final HTableDescriptor hTableDesc) {
    ModifyTableRequest.Builder builder = ModifyTableRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(table));
    builder.setTableSchema(hTableDesc.convert());
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetSchemaAlterStatusRequest
   *
   * @param tableName
   * @return a GetSchemaAlterStatusRequest
   */
  public static GetSchemaAlterStatusRequest buildGetSchemaAlterStatusRequest(
      final byte [] tableName) {
    GetSchemaAlterStatusRequest.Builder builder = GetSchemaAlterStatusRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetTableDescriptorsRequest
   *
   * @param tableNames
   * @return a GetTableDescriptorsRequest
   */
  public static GetTableDescriptorsRequest buildGetTableDescriptorsRequest(
      final List<String> tableNames) {
    GetTableDescriptorsRequest.Builder builder = GetTableDescriptorsRequest.newBuilder();
    if (tableNames != null) {
      for (String str : tableNames) {
        builder.addTableNames(str);
      }
    }
    return builder.build();
  }

  /**
   * Creates a protocol buffer IsMasterRunningRequest
   *
   * @return a IsMasterRunningRequest
   */
  public static IsMasterRunningRequest buildIsMasterRunningRequest() {
    return IsMasterRunningRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer BalanceRequest
   *
   * @return a BalanceRequest
   */
  public static BalanceRequest buildBalanceRequest() {
    return BalanceRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer SetBalancerRunningRequest
   *
   * @param on
   * @param synchronous
   * @return a SetBalancerRunningRequest
   */
  public static SetBalancerRunningRequest buildSetBalancerRunningRequest(boolean on, boolean synchronous) {
    return SetBalancerRunningRequest.newBuilder().setOn(on).setSynchronous(synchronous).build();
  }

  /**
   * Creates a protocol buffer GetClusterStatusRequest
   *
   * @return A GetClusterStatusRequest
   */
  public static GetClusterStatusRequest buildGetClusterStatusRequest() {
    return GetClusterStatusRequest.newBuilder().build();
  }

  /**
   * Creates a request for running a catalog scan
   * @return A {@link CatalogScanRequest}
   */
  public static CatalogScanRequest buildCatalogScanRequest() {
    return CatalogScanRequest.newBuilder().build();
  }

  /**
   * Creates a request for enabling/disabling the catalog janitor
   * @return A {@link EnableCatalogJanitorRequest}
   */
  public static EnableCatalogJanitorRequest buildEnableCatalogJanitorRequest(boolean enable) {
    return EnableCatalogJanitorRequest.newBuilder().setEnable(enable).build();
  }

  /**
   * Creates a request for querying the master whether the catalog janitor is enabled
   * @return A {@link IsCatalogJanitorEnabledRequest}
   */
  public static IsCatalogJanitorEnabledRequest buildIsCatalogJanitorEnabledRequest() {
    return IsCatalogJanitorEnabledRequest.newBuilder().build();
  }

  /**
   * Creates a request for querying the master the last flushed sequence Id for a region
   * @param regionName
   * @return A {@link GetLastFlushedSequenceIdRequest}
   */
  public static GetLastFlushedSequenceIdRequest buildGetLastFlushedSequenceIdRequest(
      byte[] regionName) {
    return GetLastFlushedSequenceIdRequest.newBuilder().setRegionName(
        ByteString.copyFrom(regionName)).build();
  }

  /**
   * Create a request to grant user permissions.
   *
   * @param username the short user name who to grant permissions
   * @param table optional table name the permissions apply
   * @param family optional column family
   * @param qualifier optional qualifier
   * @param actions the permissions to be granted
   * @return A {@link AccessControlProtos} GrantRequest
   */
  public static AccessControlProtos.GrantRequest buildGrantRequest(
      String username, byte[] table, byte[] family, byte[] qualifier,
      AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder permissionBuilder =
        AccessControlProtos.Permission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    if (table != null) {
      permissionBuilder.setTable(ByteString.copyFrom(table));
    }
    if (family != null) {
      permissionBuilder.setFamily(ByteString.copyFrom(family));
    }
    if (qualifier != null) {
      permissionBuilder.setQualifier(ByteString.copyFrom(qualifier));
    }

    return AccessControlProtos.GrantRequest.newBuilder()
      .setPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(permissionBuilder.build())
      ).build();
  }

  /**
   * Create a request to revoke user permissions.
   *
   * @param username the short user name whose permissions to be revoked
   * @param table optional table name the permissions apply
   * @param family optional column family
   * @param qualifier optional qualifier
   * @param actions the permissions to be revoked
   * @return A {@link AccessControlProtos} RevokeRequest
   */
  public static AccessControlProtos.RevokeRequest buildRevokeRequest(
      String username, byte[] table, byte[] family, byte[] qualifier,
      AccessControlProtos.Permission.Action... actions) {
    AccessControlProtos.Permission.Builder permissionBuilder =
        AccessControlProtos.Permission.newBuilder();
    for (AccessControlProtos.Permission.Action a : actions) {
      permissionBuilder.addAction(a);
    }
    if (table != null) {
      permissionBuilder.setTable(ByteString.copyFrom(table));
    }
    if (family != null) {
      permissionBuilder.setFamily(ByteString.copyFrom(family));
    }
    if (qualifier != null) {
      permissionBuilder.setQualifier(ByteString.copyFrom(qualifier));
    }

    return AccessControlProtos.RevokeRequest.newBuilder()
      .setPermission(
          AccessControlProtos.UserPermission.newBuilder()
              .setUser(ByteString.copyFromUtf8(username))
              .setPermission(permissionBuilder.build())
      ).build();
  }

  /**
   * Create a RegionOpenInfo based on given region info and version of offline node
   */
  private static RegionOpenInfo buildRegionOpenInfo(
      final HRegionInfo region, final int versionOfOfflineNode) {
    RegionOpenInfo.Builder builder = RegionOpenInfo.newBuilder();
    builder.setRegion(HRegionInfo.convert(region));
    if (versionOfOfflineNode >= 0) {
      builder.setVersionOfOfflineNode(versionOfOfflineNode);
    }
    return builder.build();
  }
}
