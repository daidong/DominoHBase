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
package org.apache.hadoop.hbase.io.hfile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.protobuf.generated.HFileProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;

import com.google.common.io.NullOutputStream;

/**
 * The {@link HFile} has a fixed trailer which contains offsets to other
 * variable parts of the file. Also includes basic metadata on this file. The
 * trailer size is fixed within a given {@link HFile} format version only, but
 * we always store the version number as the last four-byte integer of the file.
 * The version number itself is split into two portions, a major 
 * version and a minor version. 
 * The last three bytes of a file is the major
 * version and a single preceding byte is the minor number. The major version
 * determines which readers/writers to use to read/write a hfile while a minor
 * version determines smaller changes in hfile format that do not need a new
 * reader/writer type.
 */
@InterfaceAudience.Private
public class FixedFileTrailer {

  private static final Log LOG = LogFactory.getLog(FixedFileTrailer.class);

  /** HFile minor version that introduced pbuf filetrailer */
  private static final int PBUF_TRAILER_MINOR_VERSION = 2;

  /**
   * We store the comparator class name as a fixed-length field in the trailer.
   */
  private static final int MAX_COMPARATOR_NAME_LENGTH = 128;

  /**
   * Offset to the fileinfo data, a small block of vitals. Necessary in v1 but
   * only potentially useful for pretty-printing in v2.
   */
  private long fileInfoOffset;

  /**
   * In version 1, the offset to the data block index. Starting from version 2,
   * the meaning of this field is the offset to the section of the file that
   * should be loaded at the time the file is being opened, and as of the time
   * of writing, this happens to be the offset of the file info section.
   */
  private long loadOnOpenDataOffset;

  /** The number of entries in the root data index. */
  private int dataIndexCount;

  /** Total uncompressed size of all blocks of the data index */
  private long uncompressedDataIndexSize;

  /** The number of entries in the meta index */
  private int metaIndexCount;

  /** The total uncompressed size of keys/values stored in the file. */
  private long totalUncompressedBytes;

  /**
   * The number of key/value pairs in the file. This field was int in version 1,
   * but is now long.
   */
  private long entryCount;

  /** The compression codec used for all blocks. */
  private Compression.Algorithm compressionCodec = Compression.Algorithm.NONE;

  /**
   * The number of levels in the potentially multi-level data index. Used from
   * version 2 onwards.
   */
  private int numDataIndexLevels;

  /** The offset of the first data block. */
  private long firstDataBlockOffset;

  /**
   * It is guaranteed that no key/value data blocks start after this offset in
   * the file.
   */
  private long lastDataBlockOffset;

  /** Raw key comparator class name in version 2 */
  private String comparatorClassName = RawComparator.class.getName();

  /** The {@link HFile} format major version. */
  private final int majorVersion;

  /** The {@link HFile} format minor version. */
  private final int minorVersion;

  FixedFileTrailer(int majorVersion, int minorVersion) {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
    HFile.checkFormatVersion(majorVersion);
  }

  private static int[] computeTrailerSizeByVersion() {
    int versionToSize[] = new int[HFile.MAX_FORMAT_VERSION + 1];
    for (int version = HFile.MIN_FORMAT_VERSION;
         version <= HFile.MAX_FORMAT_VERSION;
         ++version) {
      FixedFileTrailer fft = new FixedFileTrailer(version, HFileBlock.MINOR_VERSION_NO_CHECKSUM);
      DataOutputStream dos = new DataOutputStream(new NullOutputStream());
      try {
        fft.serialize(dos);
      } catch (IOException ex) {
        // The above has no reason to fail.
        throw new RuntimeException(ex);
      }
      versionToSize[version] = dos.size();
    }
    return versionToSize;
  }

  private static int getMaxTrailerSize() {
    int maxSize = 0;
    for (int version = HFile.MIN_FORMAT_VERSION;
         version <= HFile.MAX_FORMAT_VERSION;
         ++version)
      maxSize = Math.max(getTrailerSize(version), maxSize);
    return maxSize;
  }

  private static final int TRAILER_SIZE[] = computeTrailerSizeByVersion();
  private static final int MAX_TRAILER_SIZE = getMaxTrailerSize();

  private static final int NOT_PB_SIZE = BlockType.MAGIC_LENGTH + Bytes.SIZEOF_INT;

  static int getTrailerSize(int version) {
    return TRAILER_SIZE[version];
  }

  public int getTrailerSize() {
    return getTrailerSize(majorVersion);
  }

  /**
   * Write the trailer to a data stream. We support writing version 1 for
   * testing and for determining version 1 trailer size. It is also easy to see
   * what fields changed in version 2.
   *
   * @param outputStream
   * @throws IOException
   */
  void serialize(DataOutputStream outputStream) throws IOException {
    HFile.checkFormatVersion(majorVersion);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream baosDos = new DataOutputStream(baos);

    BlockType.TRAILER.write(baosDos);
    if (majorVersion > 2 || (majorVersion == 2 && minorVersion >= PBUF_TRAILER_MINOR_VERSION)) {
      serializeAsPB(baosDos);
    } else {
      serializeAsWritable(baosDos);
    }

    // The last 4 bytes of the file encode the major and minor version universally
    baosDos.writeInt(materializeVersion(majorVersion, minorVersion));

    outputStream.write(baos.toByteArray());
  }

  /**
   * Write trailer data as protobuf
   * @param outputStream
   * @throws IOException
   */
  void serializeAsPB(DataOutputStream output) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    HFileProtos.FileTrailerProto.newBuilder()
      .setFileInfoOffset(fileInfoOffset)
      .setLoadOnOpenDataOffset(loadOnOpenDataOffset)
      .setUncompressedDataIndexSize(uncompressedDataIndexSize)
      .setTotalUncompressedBytes(totalUncompressedBytes)
      .setDataIndexCount(dataIndexCount)
      .setMetaIndexCount(metaIndexCount)
      .setEntryCount(entryCount)
      .setNumDataIndexLevels(numDataIndexLevels)
      .setFirstDataBlockOffset(firstDataBlockOffset)
      .setLastDataBlockOffset(lastDataBlockOffset)
      .setComparatorClassName(comparatorClassName)
      .setCompressionCodec(compressionCodec.ordinal())
      .build().writeDelimitedTo(baos);
    output.write(baos.toByteArray());
    // Pad to make up the difference between variable PB encoding length and the
    // length when encoded as writable under earlier V2 formats. Failure to pad
    // properly or if the PB encoding is too big would mean the trailer wont be read
    // in properly by HFile.
    int padding = getTrailerSize() - NOT_PB_SIZE - baos.size();
    if (padding < 0) {
      throw new IOException("Pbuf encoding size exceeded fixed trailer size limit");
    }
    for (int i = 0; i < padding; i++) {
      output.write(0);
    }
  }

  /**
   * Write trailer data as writable
   * @param outputStream
   * @throws IOException
   */
  void serializeAsWritable(DataOutputStream output) throws IOException {
    output.writeLong(fileInfoOffset);
    output.writeLong(loadOnOpenDataOffset);
    output.writeInt(dataIndexCount);

    if (majorVersion == 1) {
      // This used to be metaIndexOffset, but it was not used in version 1.
      output.writeLong(0);
    } else {
      output.writeLong(uncompressedDataIndexSize);
    }

    output.writeInt(metaIndexCount);
    output.writeLong(totalUncompressedBytes);
    if (majorVersion == 1) {
      output.writeInt((int) Math.min(Integer.MAX_VALUE, entryCount));
    } else {
      // This field is long from version 2 onwards.
      output.writeLong(entryCount);
    }
    output.writeInt(compressionCodec.ordinal());

    if (majorVersion > 1) {
      output.writeInt(numDataIndexLevels);
      output.writeLong(firstDataBlockOffset);
      output.writeLong(lastDataBlockOffset);
      Bytes.writeStringFixedSize(output, comparatorClassName, MAX_COMPARATOR_NAME_LENGTH);
    }
  }

  /**
   * Deserialize the fixed file trailer from the given stream. The version needs
   * to already be specified. Make sure this is consistent with
   * {@link #serialize(DataOutputStream)}.
   *
   * @param inputStream
   * @throws IOException
   */
  void deserialize(DataInputStream inputStream) throws IOException {
    HFile.checkFormatVersion(majorVersion);

    BlockType.TRAILER.readAndCheck(inputStream);

    if (majorVersion > 2 || (majorVersion == 2 && minorVersion >= PBUF_TRAILER_MINOR_VERSION)) {
      deserializeFromPB(inputStream);
    } else {
      deserializeFromWritable(inputStream);
    }

    // The last 4 bytes of the file encode the major and minor version universally
    int version = inputStream.readInt();
    expectMajorVersion(extractMajorVersion(version));
    expectMinorVersion(extractMinorVersion(version));
  }

  /**
   * Deserialize the file trailer as protobuf
   * @param inputStream
   * @throws IOException
   */
  void deserializeFromPB(DataInputStream inputStream) throws IOException {
    // read PB and skip padding
    int start = inputStream.available();
    HFileProtos.FileTrailerProto.Builder builder = HFileProtos.FileTrailerProto.newBuilder();
    builder.mergeDelimitedFrom(inputStream);
    int size = start - inputStream.available();
    inputStream.skip(getTrailerSize() - NOT_PB_SIZE - size);

    // process the PB
    if (builder.hasFileInfoOffset()) {
      fileInfoOffset = builder.getFileInfoOffset();
    }
    if (builder.hasLoadOnOpenDataOffset()) {
      loadOnOpenDataOffset = builder.getLoadOnOpenDataOffset();
    }
    if (builder.hasUncompressedDataIndexSize()) {
      uncompressedDataIndexSize = builder.getUncompressedDataIndexSize();
    }
    if (builder.hasTotalUncompressedBytes()) {
      totalUncompressedBytes = builder.getTotalUncompressedBytes();
    }
    if (builder.hasDataIndexCount()) {
      dataIndexCount = builder.getDataIndexCount();
    }
    if (builder.hasMetaIndexCount()) {
      metaIndexCount = builder.getMetaIndexCount();
    }
    if (builder.hasEntryCount()) {
      entryCount = builder.getEntryCount();
    }
    if (builder.hasNumDataIndexLevels()) {
      numDataIndexLevels = builder.getNumDataIndexLevels();
    }
    if (builder.hasFirstDataBlockOffset()) {
      firstDataBlockOffset = builder.getFirstDataBlockOffset();
    }
    if (builder.hasLastDataBlockOffset()) {
      lastDataBlockOffset = builder.getLastDataBlockOffset();
    }
    if (builder.hasComparatorClassName()) {
      comparatorClassName = builder.getComparatorClassName();
    }
    if (builder.hasCompressionCodec()) {
      compressionCodec = Compression.Algorithm.values()[builder.getCompressionCodec()];
    } else {
      compressionCodec = Compression.Algorithm.NONE;
    }
  }

  /**
   * Deserialize the file trailer as writable data
   * @param input
   * @throws IOException
   */
  void deserializeFromWritable(DataInput input) throws IOException {
    fileInfoOffset = input.readLong();
    loadOnOpenDataOffset = input.readLong();
    dataIndexCount = input.readInt();
    if (majorVersion == 1) {
      input.readLong(); // Read and skip metaIndexOffset.
    } else {
      uncompressedDataIndexSize = input.readLong();
    }
    metaIndexCount = input.readInt();

    totalUncompressedBytes = input.readLong();
    entryCount = majorVersion == 1 ? input.readInt() : input.readLong();
    compressionCodec = Compression.Algorithm.values()[input.readInt()];
    if (majorVersion > 1) {
      numDataIndexLevels = input.readInt();
      firstDataBlockOffset = input.readLong();
      lastDataBlockOffset = input.readLong();
      comparatorClassName = Bytes.readStringFixedSize(input, MAX_COMPARATOR_NAME_LENGTH);
    }
  }
  
  private void append(StringBuilder sb, String s) {
    if (sb.length() > 0)
      sb.append(", ");
    sb.append(s);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    append(sb, "fileinfoOffset=" + fileInfoOffset);
    append(sb, "loadOnOpenDataOffset=" + loadOnOpenDataOffset);
    append(sb, "dataIndexCount=" + dataIndexCount);
    append(sb, "metaIndexCount=" + metaIndexCount);
    append(sb, "totalUncomressedBytes=" + totalUncompressedBytes);
    append(sb, "entryCount=" + entryCount);
    append(sb, "compressionCodec=" + compressionCodec);
    if (majorVersion == 2) {
      append(sb, "uncompressedDataIndexSize=" + uncompressedDataIndexSize);
      append(sb, "numDataIndexLevels=" + numDataIndexLevels);
      append(sb, "firstDataBlockOffset=" + firstDataBlockOffset);
      append(sb, "lastDataBlockOffset=" + lastDataBlockOffset);
      append(sb, "comparatorClassName=" + comparatorClassName);
    }
    append(sb, "majorVersion=" + majorVersion);
    append(sb, "minorVersion=" + minorVersion);

    return sb.toString();
  }

  /**
   * Reads a file trailer from the given file.
   *
   * @param istream the input stream with the ability to seek. Does not have to
   *          be buffered, as only one read operation is made.
   * @param fileSize the file size. Can be obtained using
   *          {@link org.apache.hadoop.fs.FileSystem#getFileStatus(
   *          org.apache.hadoop.fs.Path)}.
   * @return the fixed file trailer read
   * @throws IOException if failed to read from the underlying stream, or the
   *           trailer is corrupted, or the version of the trailer is
   *           unsupported
   */
  public static FixedFileTrailer readFromStream(FSDataInputStream istream,
      long fileSize) throws IOException {
    int bufferSize = MAX_TRAILER_SIZE;
    long seekPoint = fileSize - bufferSize;
    if (seekPoint < 0) {
      // It is hard to imagine such a small HFile.
      seekPoint = 0;
      bufferSize = (int) fileSize;
    }

    istream.seek(seekPoint);
    ByteBuffer buf = ByteBuffer.allocate(bufferSize);
    istream.readFully(buf.array(), buf.arrayOffset(),
        buf.arrayOffset() + buf.limit());

    // Read the version from the last int of the file.
    buf.position(buf.limit() - Bytes.SIZEOF_INT);
    int version = buf.getInt();

    // Extract the major and minor versions.
    int majorVersion = extractMajorVersion(version);
    int minorVersion = extractMinorVersion(version);

    HFile.checkFormatVersion(majorVersion); // throws IAE if invalid

    int trailerSize = getTrailerSize(majorVersion);

    FixedFileTrailer fft = new FixedFileTrailer(majorVersion, minorVersion);
    fft.deserialize(new DataInputStream(new ByteArrayInputStream(buf.array(),
        buf.arrayOffset() + bufferSize - trailerSize, trailerSize)));
    return fft;
  }

  public void expectMajorVersion(int expected) {
    if (majorVersion != expected) {
      throw new IllegalArgumentException("Invalid HFile major version: "
          + majorVersion 
          + " (expected: " + expected + ")");
    }
  }

  public void expectMinorVersion(int expected) {
    if (minorVersion != expected) {
      throw new IllegalArgumentException("Invalid HFile minor version: "
          + minorVersion + " (expected: " + expected + ")");
    }
  }

  public void expectAtLeastMajorVersion(int lowerBound) {
    if (majorVersion < lowerBound) {
      throw new IllegalArgumentException("Invalid HFile major version: "
          + majorVersion
          + " (expected: " + lowerBound + " or higher).");
    }
  }

  public long getFileInfoOffset() {
    return fileInfoOffset;
  }

  public void setFileInfoOffset(long fileInfoOffset) {
    this.fileInfoOffset = fileInfoOffset;
  }

  public long getLoadOnOpenDataOffset() {
    return loadOnOpenDataOffset;
  }

  public void setLoadOnOpenOffset(long loadOnOpenDataOffset) {
    this.loadOnOpenDataOffset = loadOnOpenDataOffset;
  }

  public int getDataIndexCount() {
    return dataIndexCount;
  }

  public void setDataIndexCount(int dataIndexCount) {
    this.dataIndexCount = dataIndexCount;
  }

  public int getMetaIndexCount() {
    return metaIndexCount;
  }

  public void setMetaIndexCount(int metaIndexCount) {
    this.metaIndexCount = metaIndexCount;
  }

  public long getTotalUncompressedBytes() {
    return totalUncompressedBytes;
  }

  public void setTotalUncompressedBytes(long totalUncompressedBytes) {
    this.totalUncompressedBytes = totalUncompressedBytes;
  }

  public long getEntryCount() {
    return entryCount;
  }

  public void setEntryCount(long newEntryCount) {
    if (majorVersion == 1) {
      int intEntryCount = (int) Math.min(Integer.MAX_VALUE, newEntryCount);
      if (intEntryCount != newEntryCount) {
        LOG.info("Warning: entry count is " + newEntryCount + " but writing "
            + intEntryCount + " into the version " + majorVersion + " trailer");
      }
      entryCount = intEntryCount;
      return;
    }
    entryCount = newEntryCount;
  }

  public Compression.Algorithm getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec(Compression.Algorithm compressionCodec) {
    this.compressionCodec = compressionCodec;
  }

  public int getNumDataIndexLevels() {
    expectAtLeastMajorVersion(2);
    return numDataIndexLevels;
  }

  public void setNumDataIndexLevels(int numDataIndexLevels) {
    expectAtLeastMajorVersion(2);
    this.numDataIndexLevels = numDataIndexLevels;
  }

  public long getLastDataBlockOffset() {
    expectAtLeastMajorVersion(2);
    return lastDataBlockOffset;
  }

  public void setLastDataBlockOffset(long lastDataBlockOffset) {
    expectAtLeastMajorVersion(2);
    this.lastDataBlockOffset = lastDataBlockOffset;
  }

  public long getFirstDataBlockOffset() {
    expectAtLeastMajorVersion(2);
    return firstDataBlockOffset;
  }

  public void setFirstDataBlockOffset(long firstDataBlockOffset) {
    expectAtLeastMajorVersion(2);
    this.firstDataBlockOffset = firstDataBlockOffset;
  }

  public String getComparatorClassName() {
    return comparatorClassName;
  }

  /**
   * Returns the major version of this HFile format
   */
  public int getMajorVersion() {
    return majorVersion;
  }

  /**
   * Returns the minor version of this HFile format
   */
  int getMinorVersion() {
    return minorVersion;
  }

  @SuppressWarnings("rawtypes")
  public void setComparatorClass(Class<? extends RawComparator> klass) {
    expectAtLeastMajorVersion(2);
    comparatorClassName = klass.getName();
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends RawComparator<byte[]>> getComparatorClass(
      String comparatorClassName) throws IOException {
    try {
      return (Class<? extends RawComparator<byte[]>>)
          Class.forName(comparatorClassName);
    } catch (ClassNotFoundException ex) {
      throw new IOException(ex);
    }
  }

  public static RawComparator<byte[]> createComparator(
      String comparatorClassName) throws IOException {
    try {
      return getComparatorClass(comparatorClassName).newInstance();
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  RawComparator<byte[]> createComparator() throws IOException {
    expectAtLeastMajorVersion(2);
    return createComparator(comparatorClassName);
  }

  public long getUncompressedDataIndexSize() {
    if (majorVersion == 1)
      return 0;
    return uncompressedDataIndexSize;
  }

  public void setUncompressedDataIndexSize(
      long uncompressedDataIndexSize) {
    expectAtLeastMajorVersion(2);
    this.uncompressedDataIndexSize = uncompressedDataIndexSize;
  }

  /**
   * Extracts the major version for a 4-byte serialized version data.
   * The major version is the 3 least significant bytes
   */
  private static int extractMajorVersion(int serializedVersion) {
    return (serializedVersion & 0x00ffffff);
  }

  /**
   * Extracts the minor version for a 4-byte serialized version data.
   * The major version are the 3 the most significant bytes
   */
  private static int extractMinorVersion(int serializedVersion) {
    return (serializedVersion >>> 24);
  }

  /**
   * Create a 4 byte serialized version number by combining the
   * minor and major version numbers.
   */
  private static int materializeVersion(int majorVersion, int minorVersion) {
    return ((majorVersion & 0x00ffffff) | (minorVersion << 24));
  }
}
