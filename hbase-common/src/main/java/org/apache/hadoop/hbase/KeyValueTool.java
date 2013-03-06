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

package org.apache.hadoop.hbase;

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.IterableUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hbase.Cell;
import org.apache.hbase.cell.CellTool;

/**
 * static convenience methods for dealing with KeyValues and collections of KeyValues
 */
@InterfaceAudience.Private
public class KeyValueTool {

  /**************** length *********************/

  public static int length(final Cell cell) {
    return (int)KeyValue.getKeyValueDataStructureSize(cell.getRowLength(), cell.getFamilyLength(),
      cell.getQualifierLength(), cell.getValueLength());
  }

  protected static int keyLength(final Cell cell) {
    return (int)KeyValue.getKeyDataStructureSize(cell.getRowLength(), cell.getFamilyLength(),
      cell.getQualifierLength());
  }

  public static int lengthWithMvccVersion(final KeyValue kv, final boolean includeMvccVersion) {
    int length = kv.getLength();
    if (includeMvccVersion) {
      length += WritableUtils.getVIntSize(kv.getMvccVersion());
    }
    return length;
  }

  public static int totalLengthWithMvccVersion(final Iterable<? extends KeyValue> kvs,
      final boolean includeMvccVersion) {
    int length = 0;
    for (KeyValue kv : IterableUtils.nullSafe(kvs)) {
      length += lengthWithMvccVersion(kv, includeMvccVersion);
    }
    return length;
  }


  /**************** copy key only *********************/

  public static KeyValue copyToNewKeyValue(final Cell cell) {
    KeyValue kvCell = new KeyValue(copyToNewByteArray(cell));
    kvCell.setMvccVersion(cell.getMvccVersion());
    return kvCell;
  }

  public static ByteBuffer copyKeyToNewByteBuffer(final Cell cell) {
    byte[] bytes = new byte[keyLength(cell)];
    appendKeyToByteArrayWithoutValue(cell, bytes, 0);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.position(buffer.limit());//make it look as if each field were appended
    return buffer;
  }

  public static byte[] copyToNewByteArray(final Cell cell) {
    int v1Length = length(cell);
    byte[] backingBytes = new byte[v1Length];
    appendToByteArray(cell, backingBytes, 0);
    return backingBytes;
  }

  protected static int appendKeyToByteArrayWithoutValue(final Cell cell, final byte[] output,
      final int offset) {
    int nextOffset = offset;
    nextOffset = Bytes.putShort(output, nextOffset, cell.getRowLength());
    nextOffset = CellTool.copyRowTo(cell, output, nextOffset);
    nextOffset = Bytes.putByte(output, nextOffset, cell.getFamilyLength());
    nextOffset = CellTool.copyFamilyTo(cell, output, nextOffset);
    nextOffset = CellTool.copyQualifierTo(cell, output, nextOffset);
    nextOffset = Bytes.putLong(output, nextOffset, cell.getTimestamp());
    nextOffset = Bytes.putByte(output, nextOffset, cell.getTypeByte());
    return nextOffset;
  }


  /**************** copy key and value *********************/

  public static int appendToByteArray(final Cell cell, final byte[] output, final int offset) {
    int pos = offset;
    pos = Bytes.putInt(output, pos, keyLength(cell));
    pos = Bytes.putInt(output, pos, cell.getValueLength());
    pos = appendKeyToByteArrayWithoutValue(cell, output, pos);
    CellTool.copyValueTo(cell, output, pos);
    return pos + cell.getValueLength();
  }

  public static ByteBuffer copyToNewByteBuffer(final Cell cell) {
    byte[] bytes = new byte[length(cell)];
    appendToByteArray(cell, bytes, 0);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.position(buffer.limit());//make it look as if each field were appended
    return buffer;
  }

  public static void appendToByteBuffer(final ByteBuffer bb, final KeyValue kv,
      final boolean includeMvccVersion) {
    // keep pushing the limit out. assume enough capacity
    bb.limit(bb.position() + kv.getLength());
    bb.put(kv.getBuffer(), kv.getOffset(), kv.getLength());
    if (includeMvccVersion) {
      int numMvccVersionBytes = WritableUtils.getVIntSize(kv.getMvccVersion());
      ByteBufferUtils.extendLimit(bb, numMvccVersionBytes);
      ByteBufferUtils.writeVLong(bb, kv.getMvccVersion());
    }
  }


  /**************** iterating *******************************/

  /**
   * Creates a new KeyValue object positioned in the supplied ByteBuffer and sets the ByteBuffer's
   * position to the start of the next KeyValue. Does not allocate a new array or copy data.
   */
  public static KeyValue nextShallowCopy(final ByteBuffer bb, final boolean includesMvccVersion) {
    if (bb.isDirect()) {
      throw new IllegalArgumentException("only supports heap buffers");
    }
    if (bb.remaining() < 1) {
      return null;
    }
    int underlyingArrayOffset = bb.arrayOffset() + bb.position();
    int keyLength = bb.getInt();
    int valueLength = bb.getInt();
    int kvLength = KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE + keyLength + valueLength;
    KeyValue keyValue = new KeyValue(bb.array(), underlyingArrayOffset, kvLength);
    ByteBufferUtils.skip(bb, keyLength + valueLength);
    if (includesMvccVersion) {
      long mvccVersion = ByteBufferUtils.readVLong(bb);
      keyValue.setMvccVersion(mvccVersion);
    }
    return keyValue;
  }


  /*************** next/previous **********************************/

  /**
   * Append single byte 0x00 to the end of the input row key
   */
  public static KeyValue createFirstKeyInNextRow(final Cell in){
    byte[] nextRow = new byte[in.getRowLength() + 1];
    System.arraycopy(in.getRowArray(), in.getRowOffset(), nextRow, 0, in.getRowLength());
    nextRow[nextRow.length - 1] = 0;//maybe not necessary
    return KeyValue.createFirstOnRow(nextRow);
  }

  /**
   * Increment the row bytes and clear the other fields
   */
  public static KeyValue createFirstKeyInIncrementedRow(final Cell in){
    byte[] thisRow = new ByteRange(in.getRowArray(), in.getRowOffset(), in.getRowLength())
        .deepCopyToNewArray();
    byte[] nextRow = Bytes.unsignedCopyAndIncrement(thisRow);
    return KeyValue.createFirstOnRow(nextRow);
  }

  /**
   * Decrement the timestamp.  For tests (currently wasteful)
   *
   * Remember timestamps are sorted reverse chronologically.
   * @param in
   * @return previous key
   */
  public static KeyValue previousKey(final KeyValue in) {
    return KeyValue.createFirstOnRow(CellTool.getRowArray(in), CellTool.getFamilyArray(in),
      CellTool.getQualifierArray(in), in.getTimestamp() - 1);
  }
}
