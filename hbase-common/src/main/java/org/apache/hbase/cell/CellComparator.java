/*
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

package org.apache.hbase.cell;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.Cell;

import com.google.common.primitives.Longs;

/**
 * Compare two traditional HBase cells.
 *
 * Note: This comparator is not valid for -ROOT- and .META. tables.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CellComparator implements Comparator<Cell>, Serializable{
  private static final long serialVersionUID = -8760041766259623329L;

  @Override
  public int compare(Cell a, Cell b) {
    return compareStatic(a, b);
  }


  public static int compareStatic(Cell a, Cell b) {
    //row
    int c = Bytes.compareTo(
        a.getRowArray(), a.getRowOffset(), a.getRowLength(),
        b.getRowArray(), b.getRowOffset(), b.getRowLength());
    if (c != 0) return c;

    //family
    c = Bytes.compareTo(
      a.getFamilyArray(), a.getFamilyOffset(), a.getFamilyLength(),
      b.getFamilyArray(), b.getFamilyOffset(), b.getFamilyLength());
    if (c != 0) return c;

    //qualifier
    c = Bytes.compareTo(
        a.getQualifierArray(), a.getQualifierOffset(), a.getQualifierLength(),
        b.getQualifierArray(), b.getQualifierOffset(), b.getQualifierLength());
    if (c != 0) return c;

    //timestamp: later sorts first
    c = -Longs.compare(a.getTimestamp(), b.getTimestamp());
    if (c != 0) return c;

    //type
    c = (0xff & a.getTypeByte()) - (0xff & b.getTypeByte());
    if (c != 0) return c;

    //mvccVersion: later sorts first
    return -Longs.compare(a.getMvccVersion(), b.getMvccVersion());
  }


  /**************** equals ****************************/

  public static boolean equals(Cell a, Cell b){
    if (!areKeyLengthsEqual(a, b)) {
      return false;
    }
    //TODO compare byte[]'s in reverse since later bytes more likely to differ
    return 0 == compareStatic(a, b);
  }

  public static boolean equalsRow(Cell a, Cell b){
    if(!areRowLengthsEqual(a, b)){
      return false;
    }
    return 0 == Bytes.compareTo(
      a.getRowArray(), a.getRowOffset(), a.getRowLength(),
      b.getRowArray(), b.getRowOffset(), b.getRowLength());
  }


  /********************* hashCode ************************/

  /**
   * Returns a hash code that is always the same for two Cells having a matching equals(..) result.
   * Currently does not guard against nulls, but it could if necessary.
   */
  public static int hashCode(Cell cell){
    if (cell == null) {// return 0 for empty Cell
      return 0;
    }

    //pre-calculate the 3 hashes made of byte ranges
    int rowHash = Bytes.hashCode(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    int familyHash = Bytes.hashCode(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
    int qualifierHash = Bytes.hashCode(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

    //combine the 6 sub-hashes
    int hash = 31 * rowHash + familyHash;
    hash = 31 * hash + qualifierHash;
    hash = 31 * hash + (int)cell.getTimestamp();
    hash = 31 * hash + cell.getTypeByte();
    hash = 31 * hash + (int)cell.getMvccVersion();
    return hash;
  }


  /******************** lengths *************************/

  public static boolean areKeyLengthsEqual(Cell a, Cell b) {
    return a.getRowLength() == b.getRowLength()
        && a.getFamilyLength() == b.getFamilyLength()
        && a.getQualifierLength() == b.getQualifierLength();
  }

  public static boolean areRowLengthsEqual(Cell a, Cell b) {
    return a.getRowLength() == b.getRowLength();
  }


  /***************** special cases ****************************/

  /**
   * special case for KeyValue.equals
   */
  private static int compareStaticIgnoreMvccVersion(Cell a, Cell b) {
    //row
    int c = Bytes.compareTo(
        a.getRowArray(), a.getRowOffset(), a.getRowLength(),
        b.getRowArray(), b.getRowOffset(), b.getRowLength());
    if (c != 0) return c;

    //family
    c = Bytes.compareTo(
      a.getFamilyArray(), a.getFamilyOffset(), a.getFamilyLength(),
      b.getFamilyArray(), b.getFamilyOffset(), b.getFamilyLength());
    if (c != 0) return c;

    //qualifier
    c = Bytes.compareTo(
        a.getQualifierArray(), a.getQualifierOffset(), a.getQualifierLength(),
        b.getQualifierArray(), b.getQualifierOffset(), b.getQualifierLength());
    if (c != 0) return c;

    //timestamp: later sorts first
    c = -Longs.compare(a.getTimestamp(), b.getTimestamp());
    if (c != 0) return c;

    //type
    c = (0xff & a.getTypeByte()) - (0xff & b.getTypeByte());
    return c;
  }

  /**
   * special case for KeyValue.equals
   */
  public static boolean equalsIgnoreMvccVersion(Cell a, Cell b){
    return 0 == compareStaticIgnoreMvccVersion(a, b);
  }

}
