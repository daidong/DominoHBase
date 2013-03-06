/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance                                                                       with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue;

import java.util.List;
import java.util.ArrayList;

/**
 * Abstract base class to help you implement new Filters.  Common "ignore" or NOOP type
 * methods can go here, helping to reduce boiler plate in an ever-expanding filter
 * library.
 *
 * If you could instantiate FilterBase, it would end up being a "null" filter -
 * that is one that never filters anything.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FilterBase extends Filter {

  /**
   * Filters that are purely stateless and do nothing in their reset() methods can inherit
   * this null/empty implementation.
   *
   * @inheritDoc
   */
  @Override
  public void reset() {
  }

  /**
   * Filters that do not filter by row key can inherit this implementation that
   * never filters anything. (ie: returns false).
   *
   * @inheritDoc
   */
  @Override
  public boolean filterRowKey(byte [] buffer, int offset, int length) {
    return false;
  }

  /**
   * Filters that never filter all remaining can inherit this implementation that
   * never stops the filter early.
   *
   * @inheritDoc
   */
  @Override
  public boolean filterAllRemaining() {
    return false;
  }

  /**
   * Filters that dont filter by key value can inherit this implementation that
   * includes all KeyValues.
   *
   * @inheritDoc
   */
  @Override
  public ReturnCode filterKeyValue(KeyValue ignored) {
    return ReturnCode.INCLUDE;
  }

  /**
   * By default no transformation takes place
   *
   * @inheritDoc
   */
  @Override
  public KeyValue transform(KeyValue v) {
    return v;
  }

  /**
   * Filters that never filter by modifying the returned List of KeyValues can
   * inherit this implementation that does nothing.
   *
   * @inheritDoc
   */
  @Override
  public void filterRow(List<KeyValue> ignored) {
  }

  /**
   * Fitlers that never filter by modifying the returned List of KeyValues can
   * inherit this implementation that does nothing.
   *
   * @inheritDoc
   */
  @Override
  public boolean hasFilterRow() {
    return false;
  }

  /**
   * Filters that never filter by rows based on previously gathered state from
   * {@link #filterKeyValue(KeyValue)} can inherit this implementation that
   * never filters a row.
   *
   * @inheritDoc
   */
  @Override
  public boolean filterRow() {
    return false;
  }

  /**
   * Filters that are not sure which key must be next seeked to, can inherit
   * this implementation that, by default, returns a null KeyValue.
   *
   * @inheritDoc
   */
  public KeyValue getNextKeyHint(KeyValue currentKV) {
    return null;
  }

  /**
   * By default, we require all scan's column families to be present. Our
   * subclasses may be more precise.
   *
   * @inheritDoc
   */
  public boolean isFamilyEssential(byte[] name) {
    return true;
  }

  /**
   * Given the filter's arguments it constructs the filter
   * <p>
   * @param filterArguments the filter's arguments
   * @return constructed filter object
   */
  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    throw new IllegalArgumentException("This method has not been implemented");
  }

  /**
   * Return filter's info for debugging and logging purpose.
   */
  public String toString() {
    return this.getClass().getSimpleName();
  }

  /**
   * Return length 0 byte array for Filters that don't require special serialization
   */
  public byte [] toByteArray() {
    return new byte[0];
  }

  /**
   * Default implementation so that writers of custom filters aren't forced to implement.
   *
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter other) {
    return true;
  }
}
