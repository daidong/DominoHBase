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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SplitKeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Single row result of a {@link Get} or {@link Scan} query.<p>
 *
 * This class is NOT THREAD SAFE.<p>
 *
 * Convenience methods are available that return various {@link Map}
 * structures and values directly.<p>
 *
 * To get a complete mapping of all cells in the Result, which can include
 * multiple families and multiple versions, use {@link #getMap()}.<p>
 *
 * To get a mapping of each family to its columns (qualifiers and values),
 * including only the latest version of each, use {@link #getNoVersionMap()}.
 *
 * To get a mapping of qualifiers to latest values for an individual family use
 * {@link #getFamilyMap(byte[])}.<p>
 *
 * To get the latest value for a specific family and qualifier use {@link #getValue(byte[], byte[])}.
 *
 * A Result is backed by an array of {@link KeyValue} objects, each representing
 * an HBase cell defined by the row, family, qualifier, timestamp, and value.<p>
 *
 * The underlying {@link KeyValue} objects can be accessed through the method {@link #list()}.
 * Each KeyValue can then be accessed through
 * {@link KeyValue#getRow()}, {@link KeyValue#getFamily()}, {@link KeyValue#getQualifier()},
 * {@link KeyValue#getTimestamp()}, and {@link KeyValue#getValue()}.<p>
 * 
 * If you need to overwrite a Result with another Result instance -- as in the old 'mapred' RecordReader next
 * invocations -- then create an empty Result with the null constructor and in then use {@link #copyFrom(Result)}
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Result {
  private KeyValue [] kvs;
  // We're not using java serialization.  Transient here is just a marker to say
  // that this is where we cache row if we're ever asked for it.
  private transient byte [] row = null;
  // Ditto for familyMap.  It can be composed on fly from passed in kvs.
  private transient NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = null;

  // never use directly
  private static byte [] buffer = null;
  private static final int PAD_WIDTH = 128;

  /**
   * Creates an empty Result w/ no KeyValue payload; returns null if you call {@link #raw()}.
   * Use this to represent no results if <code>null</code> won't do or in old 'mapred' as oppposed to 'mapreduce' package
   * MapReduce where you need to overwrite a Result
   * instance with a {@link #copyFrom(Result)} call.
   */
  public Result() {
    super();
  }

  /**
   * Instantiate a Result with the specified array of KeyValues.
   * <br><strong>Note:</strong> You must ensure that the keyvalues
   * are already sorted
   * @param kvs array of KeyValues
   */
  public Result(KeyValue [] kvs) {
    this.kvs = kvs;
  }

  /**
   * Instantiate a Result with the specified List of KeyValues.
   * <br><strong>Note:</strong> You must ensure that the keyvalues
   * are already sorted
   * @param kvs List of KeyValues
   */
  public Result(List<KeyValue> kvs) {
    this(kvs.toArray(new KeyValue[kvs.size()]));
  }

  /**
   * Method for retrieving the row key that corresponds to
   * the row from which this Result was created.
   * @return row
   */
  public byte [] getRow() {
    if (this.row == null) {
      this.row = this.kvs == null || this.kvs.length == 0? null: this.kvs[0].getRow();
    }
    return this.row;
  }

  /**
   * Return the array of KeyValues backing this Result instance.
   *
   * The array is sorted from smallest -> largest using the
   * {@link KeyValue#COMPARATOR}.
   *
   * The array only contains what your Get or Scan specifies and no more.
   * For example if you request column "A" 1 version you will have at most 1
   * KeyValue in the array. If you request column "A" with 2 version you will
   * have at most 2 KeyValues, with the first one being the newer timestamp and
   * the second being the older timestamp (this is the sort order defined by
   * {@link KeyValue#COMPARATOR}).  If columns don't exist, they won't be
   * present in the result. Therefore if you ask for 1 version all columns,
   * it is safe to iterate over this array and expect to see 1 KeyValue for
   * each column and no more.
   *
   * This API is faster than using getFamilyMap() and getMap()
   *
   * @return array of KeyValues; can be null if nothing in the result
   */
  public KeyValue[] raw() {
    return kvs;
  }

  /**
   * Create a sorted list of the KeyValue's in this result.
   *
   * Since HBase 0.20.5 this is equivalent to raw().
   *
   * @return The sorted list of KeyValue's.
   */
  public List<KeyValue> list() {
    return isEmpty()? null: Arrays.asList(raw());
  }

  /**
   * Return the KeyValues for the specific column.  The KeyValues are sorted in
   * the {@link KeyValue#COMPARATOR} order.  That implies the first entry in
   * the list is the most recent column.  If the query (Scan or Get) only
   * requested 1 version the list will contain at most 1 entry.  If the column
   * did not exist in the result set (either the column does not exist
   * or the column was not selected in the query) the list will be empty.
   *
   * Also see getColumnLatest which returns just a KeyValue
   *
   * @param family the family
   * @param qualifier
   * @return a list of KeyValues for this column or empty list if the column
   * did not exist in the result set
   */
  public List<KeyValue> getColumn(byte [] family, byte [] qualifier) {
    List<KeyValue> result = new ArrayList<KeyValue>();

    KeyValue [] kvs = raw();

    if (kvs == null || kvs.length == 0) {
      return result;
    }
    int pos = binarySearch(kvs, family, qualifier);
    if (pos == -1) {
      return result; // cant find it
    }

    for (int i = pos ; i < kvs.length ; i++ ) {
      KeyValue kv = kvs[i];
      if (kv.matchingColumn(family,qualifier)) {
        result.add(kv);
      } else {
        break;
      }
    }

    return result;
  }

  protected int binarySearch(final KeyValue [] kvs,
                             final byte [] family,
                             final byte [] qualifier) {
    KeyValue searchTerm =
        KeyValue.createFirstOnRow(kvs[0].getRow(),
            family, qualifier);

    // pos === ( -(insertion point) - 1)
    int pos = Arrays.binarySearch(kvs, searchTerm, KeyValue.COMPARATOR);
    // never will exact match
    if (pos < 0) {
      pos = (pos+1) * -1;
      // pos is now insertion point
    }
    if (pos == kvs.length) {
      return -1; // doesn't exist
    }
    return pos;
  }

  /**
   * Searches for the latest value for the specified column.
   *
   * @param kvs the array to search
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return the index where the value was found, or -1 otherwise
   */
  protected int binarySearch(final KeyValue [] kvs,
      final byte [] family, final int foffset, final int flength,
      final byte [] qualifier, final int qoffset, final int qlength) {

    double keyValueSize = (double)
        KeyValue.getKeyValueDataStructureSize(kvs[0].getRowLength(), flength, qlength, 0);

    if (buffer == null || keyValueSize > buffer.length) {
      // pad to the smallest multiple of the pad width
      buffer = new byte[(int) Math.ceil(keyValueSize / PAD_WIDTH) * PAD_WIDTH];
    }

    KeyValue searchTerm = KeyValue.createFirstOnRow(buffer, 0,
        kvs[0].getBuffer(), kvs[0].getRowOffset(), kvs[0].getRowLength(),
        family, foffset, flength,
        qualifier, qoffset, qlength);

    // pos === ( -(insertion point) - 1)
    int pos = Arrays.binarySearch(kvs, searchTerm, KeyValue.COMPARATOR);
    // never will exact match
    if (pos < 0) {
      pos = (pos+1) * -1;
      // pos is now insertion point
    }
    if (pos == kvs.length) {
      return -1; // doesn't exist
    }
    return pos;
  }

  /**
   * The KeyValue for the most recent timestamp for a given column.
   *
   * @param family
   * @param qualifier
   *
   * @return the KeyValue for the column, or null if no value exists in the row or none have been
   * selected in the query (Get/Scan)
   */
  public KeyValue getColumnLatest(byte [] family, byte [] qualifier) {
    KeyValue [] kvs = raw(); // side effect possibly.
    if (kvs == null || kvs.length == 0) {
      return null;
    }
    int pos = binarySearch(kvs, family, qualifier);
    if (pos == -1) {
      return null;
    }
    KeyValue kv = kvs[pos];
    if (kv.matchingColumn(family, qualifier)) {
      return kv;
    }
    return null;
  }

  /**
   * The KeyValue for the most recent timestamp for a given column.
   *
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return the KeyValue for the column, or null if no value exists in the row or none have been
   * selected in the query (Get/Scan)
   */
  public KeyValue getColumnLatest(byte [] family, int foffset, int flength,
      byte [] qualifier, int qoffset, int qlength) {

    KeyValue [] kvs = raw(); // side effect possibly.
    if (kvs == null || kvs.length == 0) {
      return null;
    }
    int pos = binarySearch(kvs, family, foffset, flength, qualifier, qoffset, qlength);
    if (pos == -1) {
      return null;
    }
    KeyValue kv = kvs[pos];
    if (kv.matchingColumn(family, foffset, flength, qualifier, qoffset, qlength)) {
      return kv;
    }
    return null;
  }

  /**
   * Get the latest version of the specified column.
   * @param family family name
   * @param qualifier column qualifier
   * @return value of latest version of column, null if none found
   */
  public byte[] getValue(byte [] family, byte [] qualifier) {
    KeyValue kv = getColumnLatest(family, qualifier);
    if (kv == null) {
      return null;
    }
    return kv.getValue();
  }

  /**
   * Returns the value wrapped in a new <code>ByteBuffer</code>.
   *
   * @param family family name
   * @param qualifier column qualifier
   *
   * @return the latest version of the column, or <code>null</code> if none found
   */
  public ByteBuffer getValueAsByteBuffer(byte [] family, byte [] qualifier) {

    KeyValue kv = getColumnLatest(family, 0, family.length, qualifier, 0, qualifier.length);

    if (kv == null) {
      return null;
    }
    return kv.getValueAsByteBuffer();
  }

  /**
   * Returns the value wrapped in a new <code>ByteBuffer</code>.
   *
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return the latest version of the column, or <code>null</code> if none found
   */
  public ByteBuffer getValueAsByteBuffer(byte [] family, int foffset, int flength,
      byte [] qualifier, int qoffset, int qlength) {

    KeyValue kv = getColumnLatest(family, foffset, flength, qualifier, qoffset, qlength);

    if (kv == null) {
      return null;
    }
    return kv.getValueAsByteBuffer();
  }

  /**
   * Loads the latest version of the specified column into the provided <code>ByteBuffer</code>.
   * <p>
   * Does not clear or flip the buffer.
   *
   * @param family family name
   * @param qualifier column qualifier
   * @param dst the buffer where to write the value
   *
   * @return <code>true</code> if a value was found, <code>false</code> otherwise
   *
   * @throws BufferOverflowException there is insufficient space remaining in the buffer
   */
  public boolean loadValue(byte [] family, byte [] qualifier, ByteBuffer dst)
          throws BufferOverflowException {
    return loadValue(family, 0, family.length, qualifier, 0, qualifier.length, dst);
  }

  /**
   * Loads the latest version of the specified column into the provided <code>ByteBuffer</code>.
   * <p>
   * Does not clear or flip the buffer.
   *
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param dst the buffer where to write the value
   *
   * @return <code>true</code> if a value was found, <code>false</code> otherwise
   *
   * @throws BufferOverflowException there is insufficient space remaining in the buffer
   */
  public boolean loadValue(byte [] family, int foffset, int flength,
      byte [] qualifier, int qoffset, int qlength, ByteBuffer dst)
          throws BufferOverflowException {
    KeyValue kv = getColumnLatest(family, foffset, flength, qualifier, qoffset, qlength);

    if (kv == null) {
      return false;
    }
    kv.loadValue(dst);
    return true;
  }

  /**
   * Checks if the specified column contains a non-empty value (not a zero-length byte array).
   *
   * @param family family name
   * @param qualifier column qualifier
   *
   * @return whether or not a latest value exists and is not empty
   */
  public boolean containsNonEmptyColumn(byte [] family, byte [] qualifier) {

    return containsNonEmptyColumn(family, 0, family.length, qualifier, 0, qualifier.length);
  }

  /**
   * Checks if the specified column contains a non-empty value (not a zero-length byte array).
   *
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return whether or not a latest value exists and is not empty
   */
  public boolean containsNonEmptyColumn(byte [] family, int foffset, int flength,
      byte [] qualifier, int qoffset, int qlength) {

    KeyValue kv = getColumnLatest(family, foffset, flength, qualifier, qoffset, qlength);

    return (kv != null) && (kv.getValueLength() > 0);
  }

  /**
   * Checks if the specified column contains an empty value (a zero-length byte array).
   *
   * @param family family name
   * @param qualifier column qualifier
   *
   * @return whether or not a latest value exists and is empty
   */
  public boolean containsEmptyColumn(byte [] family, byte [] qualifier) {

    return containsEmptyColumn(family, 0, family.length, qualifier, 0, qualifier.length);
  }

  /**
   * Checks if the specified column contains an empty value (a zero-length byte array).
   *
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return whether or not a latest value exists and is empty
   */
  public boolean containsEmptyColumn(byte [] family, int foffset, int flength,
      byte [] qualifier, int qoffset, int qlength) {
    KeyValue kv = getColumnLatest(family, foffset, flength, qualifier, qoffset, qlength);

    return (kv != null) && (kv.getValueLength() == 0);
  }

  /**
   * Checks for existence of a value for the specified column (empty or not).
   *
   * @param family family name
   * @param qualifier column qualifier
   *
   * @return true if at least one value exists in the result, false if not
   */
  public boolean containsColumn(byte [] family, byte [] qualifier) {
    KeyValue kv = getColumnLatest(family, qualifier);
    return kv != null;
  }

  /**
   * Checks for existence of a value for the specified column (empty or not).
   *
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   *
   * @return true if at least one value exists in the result, false if not
   */
  public boolean containsColumn(byte [] family, int foffset, int flength,
      byte [] qualifier, int qoffset, int qlength) {

    return getColumnLatest(family, foffset, flength, qualifier, qoffset, qlength) != null;
  }

  /**
   * Map of families to all versions of its qualifiers and values.
   * <p>
   * Returns a three level Map of the form:
   * <code>Map&amp;family,Map&lt;qualifier,Map&lt;timestamp,value>>></code>
   * <p>
   * Note: All other map returning methods make use of this map internally.
   * @return map from families to qualifiers to versions
   */
  public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap() {
    if (this.familyMap != null) {
      return this.familyMap;
    }
    if(isEmpty()) {
      return null;
    }
    this.familyMap = new TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>(Bytes.BYTES_COMPARATOR);
    for(KeyValue kv : this.kvs) {
      SplitKeyValue splitKV = kv.split();
      byte [] family = splitKV.getFamily();
      NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap =
        familyMap.get(family);
      if(columnMap == null) {
        columnMap = new TreeMap<byte[], NavigableMap<Long, byte[]>>
          (Bytes.BYTES_COMPARATOR);
        familyMap.put(family, columnMap);
      }
      byte [] qualifier = splitKV.getQualifier();
      NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
      if(versionMap == null) {
        versionMap = new TreeMap<Long, byte[]>(new Comparator<Long>() {
          public int compare(Long l1, Long l2) {
            return l2.compareTo(l1);
          }
        });
        columnMap.put(qualifier, versionMap);
      }
      Long timestamp = Bytes.toLong(splitKV.getTimestamp());
      byte [] value = splitKV.getValue();
      versionMap.put(timestamp, value);
    }
    return this.familyMap;
  }

  /**
   * Map of families to their most recent qualifiers and values.
   * <p>
   * Returns a two level Map of the form: <code>Map&amp;family,Map&lt;qualifier,value>></code>
   * <p>
   * The most recent version of each qualifier will be used.
   * @return map from families to qualifiers and value
   */
  public NavigableMap<byte[], NavigableMap<byte[], byte[]>> getNoVersionMap() {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return null;
    }
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> returnMap =
      new TreeMap<byte[], NavigableMap<byte[], byte[]>>(Bytes.BYTES_COMPARATOR);
    for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
      familyEntry : familyMap.entrySet()) {
      NavigableMap<byte[], byte[]> qualifierMap =
        new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      for(Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry :
        familyEntry.getValue().entrySet()) {
        byte [] value =
          qualifierEntry.getValue().get(qualifierEntry.getValue().firstKey());
        qualifierMap.put(qualifierEntry.getKey(), value);
      }
      returnMap.put(familyEntry.getKey(), qualifierMap);
    }
    return returnMap;
  }

  /**
   * Map of qualifiers to values.
   * <p>
   * Returns a Map of the form: <code>Map&lt;qualifier,value></code>
   * @param family column family to get
   * @return map of qualifiers to values
   */
  public NavigableMap<byte[], byte[]> getFamilyMap(byte [] family) {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return null;
    }
    NavigableMap<byte[], byte[]> returnMap =
      new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap =
      familyMap.get(family);
    if(qualifierMap == null) {
      return returnMap;
    }
    for(Map.Entry<byte[], NavigableMap<Long, byte[]>> entry :
      qualifierMap.entrySet()) {
      byte [] value =
        entry.getValue().get(entry.getValue().firstKey());
      returnMap.put(entry.getKey(), value);
    }
    return returnMap;
  }

  /**
   * Returns the value of the first column in the Result.
   * @return value of the first column
   */
  public byte [] value() {
    if (isEmpty()) {
      return null;
    }
    return kvs[0].getValue();
  }

  /**
   * Check if the underlying KeyValue [] is empty or not
   * @return true if empty
   */
  public boolean isEmpty() {
    return this.kvs == null || this.kvs.length == 0;
  }

  /**
   * @return the size of the underlying KeyValue []
   */
  public int size() {
    return this.kvs == null? 0: this.kvs.length;
  }

  /**
   * @return String
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("keyvalues=");
    if(isEmpty()) {
      sb.append("NONE");
      return sb.toString();
    }
    sb.append("{");
    boolean moreThanOne = false;
    for(KeyValue kv : this.kvs) {
      if(moreThanOne) {
        sb.append(", ");
      } else {
        moreThanOne = true;
      }
      sb.append(kv.toString());
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Does a deep comparison of two Results, down to the byte arrays.
   * @param res1 first result to compare
   * @param res2 second result to compare
   * @throws Exception Every difference is throwing an exception
   */
  public static void compareResults(Result res1, Result res2)
      throws Exception {
    if (res2 == null) {  
      throw new Exception("There wasn't enough rows, we stopped at "
          + Bytes.toStringBinary(res1.getRow()));
    }
    if (res1.size() != res2.size()) {
      throw new Exception("This row doesn't have the same number of KVs: "
          + res1.toString() + " compared to " + res2.toString());
    }
    KeyValue[] ourKVs = res1.raw();
    KeyValue[] replicatedKVs = res2.raw();
    for (int i = 0; i < res1.size(); i++) {
      if (!ourKVs[i].equals(replicatedKVs[i]) ||
          !Bytes.equals(ourKVs[i].getValue(), replicatedKVs[i].getValue())) {
        throw new Exception("This result was different: "
            + res1.toString() + " compared to " + res2.toString());
      }
    }
  }

  /**
   * Copy another Result into this one. Needed for the old Mapred framework
   * @param other
   */
  public void copyFrom(Result other) {
    this.row = null;
    this.familyMap = null;
    this.kvs = other.kvs;
  }
}