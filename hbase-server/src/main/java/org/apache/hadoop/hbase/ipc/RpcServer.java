/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ipc;

import com.google.common.base.Function;
import com.google.protobuf.Message;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.IpcProtocol;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcRequestBody;

import java.io.IOException;
import java.net.InetSocketAddress;

@InterfaceAudience.Private
public interface RpcServer {
  // TODO: Needs cleanup.  Why a 'start', and then a 'startThreads' and an 'openServer'?
  // Also, the call takes a RpcRequestBody, an already composed combination of
  // rpc Request and metadata.  Should disentangle metadata and rpc Request Message.

  void setSocketSendBufSize(int size);

  void start();

  void stop();

  void join() throws InterruptedException;

  InetSocketAddress getListenerAddress();

  /** Called for each call.
   * @param param parameter
   * @param receiveTime time
   * @return Message Protobuf response Message
   * @throws java.io.IOException e
   */
  Message call(Class<? extends IpcProtocol> protocol,
      RpcRequestBody param, long receiveTime, MonitoredRPCHandler status)
      throws IOException;

  void setErrorHandler(HBaseRPCErrorHandler handler);

  void setQosFunction(Function<RpcRequestBody, Integer> newFunc);

  void openServer();

  void startThreads();

  /**
   * Returns the metrics instance for reporting RPC call statistics
   */
  MetricsHBaseServer getMetrics();
}