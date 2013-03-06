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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * A container for Result objects, grouped by regionName.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MultiResponse {

  // map of regionName to list of (Results paired to the original index for that
  // Result)
  private Map<byte[], List<Pair<Integer, Object>>> results =
      new TreeMap<byte[], List<Pair<Integer, Object>>>(Bytes.BYTES_COMPARATOR);

  public MultiResponse() {
    super();
  }

  /**
   * @return Number of pairs in this container
   */
  public int size() {
    int size = 0;
    for (Collection<?> c : results.values()) {
      size += c.size();
    }
    return size;
  }

  /**
   * Add the pair to the container, grouped by the regionName
   *
   * @param regionName
   * @param r
   *          First item in the pair is the original index of the Action
   *          (request). Second item is the Result. Result will be empty for
   *          successful Put and Delete actions.
   */
  public void add(byte[] regionName, Pair<Integer, Object> r) {
    List<Pair<Integer, Object>> rs = results.get(regionName);
    if (rs == null) {
      rs = new ArrayList<Pair<Integer, Object>>();
      results.put(regionName, rs);
    }
    rs.add(r);
  }

  public void add(byte []regionName, int originalIndex, Object resOrEx) {
    add(regionName, new Pair<Integer,Object>(originalIndex, resOrEx));
  }

  public Map<byte[], List<Pair<Integer, Object>>> getResults() {
    return results;
  }
}