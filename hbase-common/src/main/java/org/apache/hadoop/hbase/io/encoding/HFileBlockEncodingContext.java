/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.encoding;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.BlockType;

/**
 * An encoding context that is created by a writer's encoder, and is shared
 * across the writer's whole lifetime.
 *
 * @see HFileBlockDecodingContext for decoding
 *
 */
public interface HFileBlockEncodingContext {

  /**
   * @return OutputStream to which encoded data is written
   */
  public OutputStream getOutputStreamForEncoder();

  /**
   * @return encoded and compressed bytes with header which are ready to write
   *         out to disk
   */
  public byte[] getOnDiskBytesWithHeader();

  /**
   * @return encoded but not heavily compressed bytes with header which can be
   *         cached in block cache
   */
  public byte[] getUncompressedBytesWithHeader();

  /**
   * @return the block type after encoding
   */
  public BlockType getBlockType();

  /**
   * @return the compression algorithm used by this encoding context
   */
  public Compression.Algorithm getCompression();

  /**
   * @return the header size used
   */
  public int getHeaderSize();

  /**
   * @return the {@link DataBlockEncoding} encoding used
   */
  public DataBlockEncoding getDataBlockEncoding();

  /**
   * Do any action that needs to be performed after the encoding.
   * Compression is also included if {@link #getCompression()} returns non-null
   * compression algorithm
   *
   * @param blockType
   * @throws IOException
   */
  public void postEncoding(BlockType blockType) throws IOException;

  /**
   * Releases the resources used.
   */
  public void close();

}
