/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.rados;

import alluxio.underfs.MultiRangeObjectInputStream;

import com.ceph.rados.IoCTX;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for reading a file from OSS. This input stream returns 0 when calling read with an empty
 * buffer.
 */
@NotThreadSafe
public class RadosInputStream extends MultiRangeObjectInputStream {

  /** The path of the object to read. */
  private final String mKey;
  /** The IoCtx for object operations. */
  private final IoCTX mIoCtx;

  /**
   * Creates a new instance of {@link RadosInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for OSS
   */
  public RadosInputStream(String key, IoCTX ioctx) throws IOException {
    this(key, ioctx, 0L);
  }

  /**
   * Creates a new instance of {@link RadosInputStream}.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the client for OSS
   * @param position the position to begin reading from
   */
  public RadosInputStream(String key, IoCTX ioctx, long position)
      throws IOException {
    mKey = key;
    mIoCtx = ioctx;
    mPos = position;
  }

  @Override
  protected InputStream createStream(long startPos, long endPos) throws IOException {
    int len = (int)(endPos - startPos + 1);
    byte[] buf = new byte[len];

    int r = mIoCtx.read(mKey, len, startPos, buf);
    return new ByteArrayInputStream(buf, 0, r);
  }
}
