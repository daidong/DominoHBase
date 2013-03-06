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
package org.apache.hadoop.hbase.filter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests filter sets
 *
 */
@Category(SmallTests.class)
public class TestFilterList {
  static final int MAX_PAGES = 2;
  static final char FIRST_CHAR = 'a';
  static final char LAST_CHAR = 'e';
  static byte[] GOOD_BYTES = Bytes.toBytes("abc");
  static byte[] BAD_BYTES = Bytes.toBytes("def");


  @Test
  public void testAddFilter() throws Exception {
    Filter filter1 = new FirstKeyOnlyFilter();
    Filter filter2 = new FirstKeyOnlyFilter();

    FilterList filterList = new FilterList(filter1, filter2);
    filterList.addFilter(new FirstKeyOnlyFilter());

    filterList = new FilterList(Arrays.asList(filter1, filter2));
    filterList.addFilter(new FirstKeyOnlyFilter());

    filterList = new FilterList(Operator.MUST_PASS_ALL, filter1, filter2);
    filterList.addFilter(new FirstKeyOnlyFilter());

    filterList = new FilterList(Operator.MUST_PASS_ALL, Arrays.asList(filter1, filter2));
    filterList.addFilter(new FirstKeyOnlyFilter());

  }


  /**
   * Test "must pass one"
   * @throws Exception
   */
  @Test
  public void testMPONE() throws Exception {
    mpOneTest(getFilterMPONE());
  }

  private Filter getFilterMPONE() {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new PageFilter(MAX_PAGES));
    filters.add(new WhileMatchFilter(new PrefixFilter(Bytes.toBytes("yyy"))));
    Filter filterMPONE =
      new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
    return filterMPONE;
  }

  private void mpOneTest(Filter filterMPONE) throws Exception {
    /* Filter must do all below steps:
     * <ul>
     * <li>{@link #reset()}</li>
     * <li>{@link #filterAllRemaining()} -> true indicates scan is over, false, keep going on.</li>
     * <li>{@link #filterRowKey(byte[],int,int)} -> true to drop this row,
     * if false, we will also call</li>
     * <li>{@link #filterKeyValue(org.apache.hadoop.hbase.KeyValue)} -> true to drop this key/value</li>
     * <li>{@link #filterRow()} -> last chance to drop entire row based on the sequence of
     * filterValue() calls. Eg: filter a row if it doesn't contain a specified column.
     * </li>
     * </ul>
    */
    filterMPONE.reset();
    assertFalse(filterMPONE.filterAllRemaining());

    /* Will pass both */
    byte [] rowkey = Bytes.toBytes("yyyyyyyyy");
    for (int i = 0; i < MAX_PAGES - 1; i++) {
      assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
      assertFalse(filterMPONE.filterRow());
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
        Bytes.toBytes(i));
      assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
    }

    /* Only pass PageFilter */
    rowkey = Bytes.toBytes("z");
    assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
    assertFalse(filterMPONE.filterRow());
    KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(0),
        Bytes.toBytes(0));
    assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));

    /* PageFilter will fail now, but should pass because we match yyy */
    rowkey = Bytes.toBytes("yyy");
    assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
    assertFalse(filterMPONE.filterRow());
    kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(0),
        Bytes.toBytes(0));
    assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));

    /* We should filter any row */
    rowkey = Bytes.toBytes("z");
    assertTrue(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
    assertTrue(filterMPONE.filterRow());
    assertTrue(filterMPONE.filterAllRemaining());

  }

  /**
   * Test "must pass all"
   * @throws Exception
   */
  @Test
  public void testMPALL() throws Exception {
    mpAllTest(getMPALLFilter());
  }

  private Filter getMPALLFilter() {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new PageFilter(MAX_PAGES));
    filters.add(new WhileMatchFilter(new PrefixFilter(Bytes.toBytes("yyy"))));
    Filter filterMPALL =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
    return filterMPALL;
  }

  private void mpAllTest(Filter filterMPALL) throws Exception {
    /* Filter must do all below steps:
     * <ul>
     * <li>{@link #reset()}</li>
     * <li>{@link #filterAllRemaining()} -> true indicates scan is over, false, keep going on.</li>
     * <li>{@link #filterRowKey(byte[],int,int)} -> true to drop this row,
     * if false, we will also call</li>
     * <li>{@link #filterKeyValue(org.apache.hadoop.hbase.KeyValue)} -> true to drop this key/value</li>
     * <li>{@link #filterRow()} -> last chance to drop entire row based on the sequence of
     * filterValue() calls. Eg: filter a row if it doesn't contain a specified column.
     * </li>
     * </ul>
    */
    filterMPALL.reset();
    assertFalse(filterMPALL.filterAllRemaining());
    byte [] rowkey = Bytes.toBytes("yyyyyyyyy");
    for (int i = 0; i < MAX_PAGES - 1; i++) {
      assertFalse(filterMPALL.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
        Bytes.toBytes(i));
      assertTrue(Filter.ReturnCode.INCLUDE == filterMPALL.filterKeyValue(kv));
    }
    filterMPALL.reset();
    rowkey = Bytes.toBytes("z");
    assertTrue(filterMPALL.filterRowKey(rowkey, 0, rowkey.length));
    // Should fail here; row should be filtered out.
    KeyValue kv = new KeyValue(rowkey, rowkey, rowkey, rowkey);
    assertTrue(Filter.ReturnCode.NEXT_ROW == filterMPALL.filterKeyValue(kv));

    // Both filters in Set should be satisfied by now
    assertTrue(filterMPALL.filterRow());
  }

  /**
   * Test list ordering
   * @throws Exception
   */
  @Test
  public void testOrdering() throws Exception {
    orderingTest(getOrderingFilter());
  }

  public Filter getOrderingFilter() {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new PrefixFilter(Bytes.toBytes("yyy")));
    filters.add(new PageFilter(MAX_PAGES));
    Filter filterMPONE =
      new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
    return filterMPONE;
  }

  public void orderingTest(Filter filterMPONE) throws Exception {
    /* Filter must do all below steps:
     * <ul>
     * <li>{@link #reset()}</li>
     * <li>{@link #filterAllRemaining()} -> true indicates scan is over, false, keep going on.</li>
     * <li>{@link #filterRowKey(byte[],int,int)} -> true to drop this row,
     * if false, we will also call</li>
     * <li>{@link #filterKeyValue(org.apache.hadoop.hbase.KeyValue)} -> true to drop this key/value</li>
     * <li>{@link #filterRow()} -> last chance to drop entire row based on the sequence of
     * filterValue() calls. Eg: filter a row if it doesn't contain a specified column.
     * </li>
     * </ul>
    */
    filterMPONE.reset();
    assertFalse(filterMPONE.filterAllRemaining());

    /* We should be able to fill MAX_PAGES without incrementing page counter */
    byte [] rowkey = Bytes.toBytes("yyyyyyyy");
    for (int i = 0; i < MAX_PAGES; i++) {
      assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
          Bytes.toBytes(i));
        assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
      assertFalse(filterMPONE.filterRow());
    }

    /* Now let's fill the page filter */
    rowkey = Bytes.toBytes("xxxxxxx");
    for (int i = 0; i < MAX_PAGES; i++) {
      assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
          Bytes.toBytes(i));
        assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
      assertFalse(filterMPONE.filterRow());
    }

    /* We should still be able to include even though page filter is at max */
    rowkey = Bytes.toBytes("yyy");
    for (int i = 0; i < MAX_PAGES; i++) {
      assertFalse(filterMPONE.filterRowKey(rowkey, 0, rowkey.length));
      KeyValue kv = new KeyValue(rowkey, rowkey, Bytes.toBytes(i),
          Bytes.toBytes(i));
        assertTrue(Filter.ReturnCode.INCLUDE == filterMPONE.filterKeyValue(kv));
      assertFalse(filterMPONE.filterRow());
    }
  }

  /**
   * Test serialization
   * @throws Exception
   */
  @Test
  public void testSerialization() throws Exception {
    List<Filter> filters = new ArrayList<Filter>();
    filters.add(new PageFilter(MAX_PAGES));
    filters.add(new WhileMatchFilter(new PrefixFilter(Bytes.toBytes("yyy"))));
    Filter filterMPALL =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);

    // Decompose filterMPALL to bytes.
    byte[] buffer = filterMPALL.toByteArray();

    // Recompose filterMPALL.
    FilterList newFilter = FilterList.parseFrom(buffer);

    // Run tests
    mpOneTest(ProtobufUtil.toFilter(ProtobufUtil.toFilter(getFilterMPONE())));
    mpAllTest(ProtobufUtil.toFilter(ProtobufUtil.toFilter(getMPALLFilter())));
    orderingTest(ProtobufUtil.toFilter(ProtobufUtil.toFilter(getOrderingFilter())));
  }

  /**
   * Test filterKeyValue logic.
   * @throws Exception
   */
  public void testFilterKeyValue() throws Exception {
    Filter includeFilter = new FilterBase() {
      @Override
      public Filter.ReturnCode filterKeyValue(KeyValue v) {
        return Filter.ReturnCode.INCLUDE;
      }
    };

    Filter alternateFilter = new FilterBase() {
      boolean returnInclude = true;

      @Override
      public Filter.ReturnCode filterKeyValue(KeyValue v) {
        Filter.ReturnCode returnCode = returnInclude ? Filter.ReturnCode.INCLUDE :
                                                       Filter.ReturnCode.SKIP;
        returnInclude = !returnInclude;
        return returnCode;
      }
    };

    Filter alternateIncludeFilter = new FilterBase() {
      boolean returnIncludeOnly = false;

      @Override
      public Filter.ReturnCode filterKeyValue(KeyValue v) {
        Filter.ReturnCode returnCode = returnIncludeOnly ? Filter.ReturnCode.INCLUDE :
                                                           Filter.ReturnCode.INCLUDE_AND_NEXT_COL;
        returnIncludeOnly = !returnIncludeOnly;
        return returnCode;
      }
    };

    // Check must pass one filter.
    FilterList mpOnefilterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(new Filter[] { includeFilter, alternateIncludeFilter, alternateFilter }));
    // INCLUDE, INCLUDE, INCLUDE_AND_NEXT_COL.
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL, mpOnefilterList.filterKeyValue(null));
    // INCLUDE, SKIP, INCLUDE. 
    assertEquals(Filter.ReturnCode.INCLUDE, mpOnefilterList.filterKeyValue(null));

    // Check must pass all filter.
    FilterList mpAllfilterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter[] { includeFilter, alternateIncludeFilter, alternateFilter }));
    // INCLUDE, INCLUDE, INCLUDE_AND_NEXT_COL.
    assertEquals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL, mpAllfilterList.filterKeyValue(null));
    // INCLUDE, SKIP, INCLUDE. 
    assertEquals(Filter.ReturnCode.SKIP, mpAllfilterList.filterKeyValue(null));
  }

  /**
   * Test pass-thru of hints.
   */
  @Test
  public void testHintPassThru() throws Exception {

    final KeyValue minKeyValue = new KeyValue(Bytes.toBytes(0L), null, null);
    final KeyValue maxKeyValue = new KeyValue(Bytes.toBytes(Long.MAX_VALUE),
        null, null);

    Filter filterNoHint = new FilterBase() {
      @Override
      public byte [] toByteArray() {return null;}
    };

    Filter filterMinHint = new FilterBase() {
      @Override
      public KeyValue getNextKeyHint(KeyValue currentKV) {
        return minKeyValue;
      }

      @Override
      public byte [] toByteArray() {return null;}
    };

    Filter filterMaxHint = new FilterBase() {
      @Override
      public KeyValue getNextKeyHint(KeyValue currentKV) {
        return new KeyValue(Bytes.toBytes(Long.MAX_VALUE), null, null);
      }

      @Override
      public byte [] toByteArray() {return null;}
    };

    // MUST PASS ONE

    // Should take the min if given two hints
    FilterList filterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(new Filter [] { filterMinHint, filterMaxHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        minKeyValue));

    // Should have no hint if any filter has no hint
    filterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(
            new Filter [] { filterMinHint, filterMaxHint, filterNoHint } ));
    assertNull(filterList.getNextKeyHint(null));
    filterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(new Filter [] { filterNoHint, filterMaxHint } ));
    assertNull(filterList.getNextKeyHint(null));

    // Should give max hint if its the only one
    filterList = new FilterList(Operator.MUST_PASS_ONE,
        Arrays.asList(new Filter [] { filterMaxHint, filterMaxHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        maxKeyValue));

    // MUST PASS ALL

    // Should take the max if given two hints
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter [] { filterMinHint, filterMaxHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        maxKeyValue));

    // Should have max hint even if a filter has no hint
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(
            new Filter [] { filterMinHint, filterMaxHint, filterNoHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        maxKeyValue));
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter [] { filterNoHint, filterMaxHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        maxKeyValue));
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter [] { filterNoHint, filterMinHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        minKeyValue));

    // Should give min hint if its the only one
    filterList = new FilterList(Operator.MUST_PASS_ALL,
        Arrays.asList(new Filter [] { filterNoHint, filterMinHint } ));
    assertEquals(0, KeyValue.COMPARATOR.compare(filterList.getNextKeyHint(null),
        minKeyValue));
  }

}

