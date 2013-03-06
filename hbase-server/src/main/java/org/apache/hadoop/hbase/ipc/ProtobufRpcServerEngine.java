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

package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.IpcProtocol;
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcRequestBody;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSecretManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
/**
 * The {@link RpcServerEngine} implementation for ProtoBuf-based RPCs.
 */
@InterfaceAudience.Private
class ProtobufRpcServerEngine implements RpcServerEngine {
  private static final Log LOG =
      LogFactory.getLog("org.apache.hadoop.hbase.ipc.ProtobufRpcServerEngine");

  ProtobufRpcServerEngine() {
    super();
  }

  @Override
  public Server getServer(Object instance, Class<?>[] ifaces,
      String bindAddress, int port, int numHandlers, int metaHandlerCount,
      boolean verbose, Configuration conf, int highPriorityLevel)
  throws IOException {
    return new Server(instance, ifaces, conf, bindAddress, port, numHandlers,
        metaHandlerCount, verbose, highPriorityLevel);
  }


  public static class Server extends HBaseServer {
    boolean verbose;
    Object instance;
    Class<?> implementation;
    private static final String WARN_RESPONSE_TIME =
        "hbase.ipc.warn.response.time";
    private static final String WARN_RESPONSE_SIZE =
        "hbase.ipc.warn.response.size";

    /** Default value for above params */
    private static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
    private static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

    private final int warnResponseTime;
    private final int warnResponseSize;

    private static String classNameBase(String className) {
      String[] names = className.split("\\.", -1);
      if (names == null || names.length == 0) {
        return className;
      }
      return names[names.length-1];
    }

    public Server(Object instance, final Class<?>[] ifaces,
                  Configuration conf, String bindAddress,  int port,
                  int numHandlers, int metaHandlerCount, boolean verbose,
                  int highPriorityLevel)
        throws IOException {
      super(bindAddress, port, numHandlers, metaHandlerCount,
          conf, classNameBase(instance.getClass().getName()),
          highPriorityLevel);
      this.instance = instance;
      this.implementation = instance.getClass();
      this.verbose = verbose;

      this.warnResponseTime = conf.getInt(WARN_RESPONSE_TIME,
          DEFAULT_WARN_RESPONSE_TIME);
      this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE,
          DEFAULT_WARN_RESPONSE_SIZE);
      this.verbose = verbose;
      this.instance = instance;
      this.implementation = instance.getClass();
    }
    private static final Map<String, Message> methodArg =
        new ConcurrentHashMap<String, Message>();
    private static final Map<String, Method> methodInstances =
        new ConcurrentHashMap<String, Method>();

    private AuthenticationTokenSecretManager createSecretManager(){
      if (!isSecurityEnabled ||
          !(instance instanceof org.apache.hadoop.hbase.Server)) {
        return null;
      }
      org.apache.hadoop.hbase.Server server =
          (org.apache.hadoop.hbase.Server)instance;
      Configuration conf = server.getConfiguration();
      long keyUpdateInterval =
          conf.getLong("hbase.auth.key.update.interval", 24*60*60*1000);
      long maxAge =
          conf.getLong("hbase.auth.token.max.lifetime", 7*24*60*60*1000);
      return new AuthenticationTokenSecretManager(conf, server.getZooKeeper(),
          server.getServerName().toString(), keyUpdateInterval, maxAge);
    }

    @Override
    public void startThreads() {
      AuthenticationTokenSecretManager mgr = createSecretManager();
      if (mgr != null) {
        setSecretManager(mgr);
        mgr.start();
      }
      this.authManager = new ServiceAuthorizationManager();
      HBasePolicyProvider.init(conf, authManager);

      // continue with base startup
      super.startThreads();
    }

    @Override
    /**
     * This is a server side method, which is invoked over RPC. On success
     * the return response has protobuf response payload. On failure, the
     * exception name and the stack trace are returned in the protobuf response.
     */
    public Message call(Class<? extends IpcProtocol> protocol,
      RpcRequestBody rpcRequest, long receiveTime, MonitoredRPCHandler status)
    throws IOException {
      try {
        String methodName = rpcRequest.getMethodName();
        Method method = getMethod(protocol, methodName);
        if (method == null) {
          throw new UnknownProtocolException("Method " + methodName +
              " doesn't exist in protocol " + protocol.getName());
        }

        /**
         * RPCs for a particular interface (ie protocol) are done using a
         * IPC connection that is setup using rpcProxy.
         * The rpcProxy's has a declared protocol name that is
         * sent form client to server at connection time.
         */

        if (verbose) {
          LOG.info("Call: protocol name=" + protocol.getName() +
              ", method=" + methodName);
        }

        status.setRPC(rpcRequest.getMethodName(),
            new Object[]{rpcRequest.getRequest()}, receiveTime);
        status.setRPCPacket(rpcRequest);
        status.resume("Servicing call");
        //get an instance of the method arg type
        Message protoType = getMethodArgType(method);
        Message param = protoType.newBuilderForType()
            .mergeFrom(rpcRequest.getRequest()).build();
        Message result;
        Object impl = null;
        if (protocol.isAssignableFrom(this.implementation)) {
          impl = this.instance;
        } else {
          throw new UnknownProtocolException(protocol);
        }

        long startTime = System.currentTimeMillis();
        if (method.getParameterTypes().length == 2) {
          // RpcController + Message in the method args
          // (generated code from RPC bits in .proto files have RpcController)
          result = (Message)method.invoke(impl, null, param);
        } else if (method.getParameterTypes().length == 1) {
          // Message (hand written code usually has only a single argument)
          result = (Message)method.invoke(impl, param);
        } else {
          throw new ServiceException("Too many parameters for method: ["
              + method.getName() + "]" + ", allowed (at most): 2, Actual: "
              + method.getParameterTypes().length);
        }
        int processingTime = (int) (System.currentTimeMillis() - startTime);
        int qTime = (int) (startTime-receiveTime);
        if (TRACELOG.isDebugEnabled()) {
          TRACELOG.debug("Call #" + CurCall.get().id +
              "; served=" + protocol.getSimpleName() + "#" + method.getName() +
              ", queueTime=" + qTime +
              ", processingTime=" + processingTime +
              ", request=" + param.toString() +
              " response=" + result.toString());
        }
        metrics.dequeuedCall(qTime);
        metrics.processedCall(processingTime);

        if (verbose) {
          log("Return: "+result, LOG);
        }
        long responseSize = result.getSerializedSize();
        // log any RPC responses that are slower than the configured warn
        // response time or larger than configured warning size
        boolean tooSlow = (processingTime > warnResponseTime
            && warnResponseTime > -1);
        boolean tooLarge = (responseSize > warnResponseSize
            && warnResponseSize > -1);
        if (tooSlow || tooLarge) {
          // when tagging, we let TooLarge trump TooSmall to keep output simple
          // note that large responses will often also be slow.
          StringBuilder buffer = new StringBuilder(256);
          buffer.append(methodName);
          buffer.append("(");
          buffer.append(param.getClass().getName());
          buffer.append(")");
          logResponse(new Object[]{rpcRequest.getRequest()},
              methodName, buffer.toString(), (tooLarge ? "TooLarge" : "TooSlow"),
              status.getClient(), startTime, processingTime, qTime,
              responseSize);
        }
        return result;
      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          throw (IOException)target;
        }
        if (target instanceof ServiceException) {
          throw ProtobufUtil.getRemoteException((ServiceException)target);
        }
        IOException ioe = new IOException(target.toString());
        ioe.setStackTrace(target.getStackTrace());
        throw ioe;
      } catch (Throwable e) {
        if (!(e instanceof IOException)) {
          LOG.error("Unexpected throwable object ", e);
        }
        IOException ioe = new IOException(e.toString());
        ioe.setStackTrace(e.getStackTrace());
        throw ioe;
      }
    }

    static Method getMethod(Class<? extends IpcProtocol> protocol,
                            String methodName) {
      Method method = methodInstances.get(methodName);
      if (method != null) {
        return method;
      }
      Method[] methods = protocol.getMethods();
      for (Method m : methods) {
        if (m.getName().equals(methodName)) {
          m.setAccessible(true);
          methodInstances.put(methodName, m);
          return m;
        }
      }
      return null;
    }

    static Message getMethodArgType(Method method) throws Exception {
      Message protoType = methodArg.get(method.getName());
      if (protoType != null) {
        return protoType;
      }

      Class<?>[] args = method.getParameterTypes();
      Class<?> arg;
      if (args.length == 2) {
        // RpcController + Message in the method args
        // (generated code from RPC bits in .proto files have RpcController)
        arg = args[1];
      } else if (args.length == 1) {
        arg = args[0];
      } else {
        //unexpected
        return null;
      }
      //in the protobuf methods, args[1] is the only significant argument
      Method newInstMethod = arg.getMethod("getDefaultInstance");
      newInstMethod.setAccessible(true);
      protoType = (Message) newInstMethod.invoke(null, (Object[]) null);
      methodArg.put(method.getName(), protoType);
      return protoType;
    }
    /**
     * Logs an RPC response to the LOG file, producing valid JSON objects for
     * client Operations.
     * @param params The parameters received in the call.
     * @param methodName The name of the method invoked
     * @param call The string representation of the call
     * @param tag  The tag that will be used to indicate this event in the log.
     * @param clientAddress   The address of the client who made this call.
     * @param startTime       The time that the call was initiated, in ms.
     * @param processingTime  The duration that the call took to run, in ms.
     * @param qTime           The duration that the call spent on the queue
     *                        prior to being initiated, in ms.
     * @param responseSize    The size in bytes of the response buffer.
     */
    void logResponse(Object[] params, String methodName, String call, String tag,
                     String clientAddress, long startTime, int processingTime, int qTime,
                     long responseSize)
        throws IOException {
      // for JSON encoding
      ObjectMapper mapper = new ObjectMapper();
      // base information that is reported regardless of type of call
      Map<String, Object> responseInfo = new HashMap<String, Object>();
      responseInfo.put("starttimems", startTime);
      responseInfo.put("processingtimems", processingTime);
      responseInfo.put("queuetimems", qTime);
      responseInfo.put("responsesize", responseSize);
      responseInfo.put("client", clientAddress);
      responseInfo.put("class", instance.getClass().getSimpleName());
      responseInfo.put("method", methodName);
      if (params.length == 2 && instance instanceof HRegionServer &&
          params[0] instanceof byte[] &&
          params[1] instanceof Operation) {
        // if the slow process is a query, we want to log its table as well
        // as its own fingerprint
        byte [] tableName =
            HRegionInfo.parseRegionName((byte[]) params[0])[0];
        responseInfo.put("table", Bytes.toStringBinary(tableName));
        // annotate the response map with operation details
        responseInfo.putAll(((Operation) params[1]).toMap());
        // report to the log file
        LOG.warn("(operation" + tag + "): " +
            mapper.writeValueAsString(responseInfo));
      } else if (params.length == 1 && instance instanceof HRegionServer &&
          params[0] instanceof Operation) {
        // annotate the response map with operation details
        responseInfo.putAll(((Operation) params[0]).toMap());
        // report to the log file
        LOG.warn("(operation" + tag + "): " +
            mapper.writeValueAsString(responseInfo));
      } else {
        // can't get JSON details, so just report call.toString() along with
        // a more generic tag.
        responseInfo.put("call", call);
        LOG.warn("(response" + tag + "): " +
            mapper.writeValueAsString(responseInfo));
      }
    }
    protected static void log(String value, Log LOG) {
      String v = value;
      if (v != null && v.length() > 55)
        v = v.substring(0, 55)+"...";
      LOG.info(v);
    }
  }
}