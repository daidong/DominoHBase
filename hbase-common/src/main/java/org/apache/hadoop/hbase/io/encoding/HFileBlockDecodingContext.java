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
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.compress.Compression;

/**
 * A decoding context that is created by a reader's encoder, and is shared
 * across the reader's all read operations.
 *
 * @see HFileBlockEncodingContext for encoding
 */
public interface HFileBlockDecodingContext {

  /**
   * @return the compression algorithm used by this decoding context
   */
  public Compression.Algorithm getCompression();

  /**
   * Perform all actions that need to be done before the encoder's real decoding process.
   * Decompression needs to be done if {@link #getCompression()} returns a valid compression
   * algorithm.
   *
   * @param onDiskSizeWithoutHeader numBytes after block and encoding headers
   * @param uncompressedSizeWithoutHeader numBytes without header required to store the block after
   *          decompressing (not decoding)
   * @param blockBufferWithoutHeader ByteBuffer pointed after the header but before the data
   * @param onDiskBlock on disk bytes to be decoded
   * @param offset data start offset in onDiskBlock
   * @throws IOException
   */
  public void prepareDecoding(int onDiskSizeWithoutHeader, int uncompressedSizeWithoutHeader,
      ByteBuffer blockBufferWithoutHeader, byte[] onDiskBlock, int offset) throws IOException;

}
