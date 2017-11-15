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

import alluxio.util.io.PathUtils;

import com.ceph.rados.IoCTX;
import com.ceph.rados.exceptions.RadosException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for writing a file into Ceph RADOS. The data will be persisted to a temporary directory on the
 * local disk and copied as a complete file when the {@link #close()} method is called.
 */
@NotThreadSafe
public final class RadosOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(RadosOutputStream.class);

  /** The path of the object to write. */
  private final String mKey;
  /** The IoCtx for object operations. */
  private final IoCTX mIoCtx;
  /** The local file that will be uploaded when the stream is closed. */
  private final File mFile;

  /** The outputstream to a local file where the file will be buffered until closed. */
  private OutputStream mLocalOutputStream;

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private AtomicBoolean mClosed = new AtomicBoolean(false);

  /**
   * Creates a name instance of {@link RadosOutputStream}.
   *
   * @param key the key of the file
   * @param ioctx the IoCtx for Ceph pool
   */
  public RadosOutputStream(String key, IoCTX ioctx) throws IOException {
    Preconditions.checkArgument(key != null && !key.isEmpty(),
        "Ceph Rados path must not be null or empty.");
    Preconditions.checkArgument(ioctx != null, "IOCtx must not be null.");
    mKey = key;
    mIoCtx = ioctx;

    mFile = new File(PathUtils.concatPath("/tmp", UUID.randomUUID()));
    mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
  }

  /**
   * Writes the given bytes to this output stream. Before close, the bytes are all written to local
   * file.
   *
   * @param b the bytes to write
   */
  @Override
  public void write(int b) throws IOException {
    mLocalOutputStream.write(b);
  }

  /**
   * Writes the given byte array to this output stream. Before close, the bytes are all written to
   * local file.
   *
   * @param b the byte array
   */
  @Override
  public void write(byte[] b) throws IOException {
    mLocalOutputStream.write(b, 0, b.length);
  }

  /**
   * Writes the given number of bytes from the given byte array starting at the given offset to this
   * output stream. Before close, the bytes are all written to local file.
   *
   * @param b the byte array
   * @param off the start offset in the data
   * @param len the number of bytes to write
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mLocalOutputStream.write(b, off, len);
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be written out. Before
   * close, the data are flushed to local file.
   */
  @Override
  public void flush() throws IOException {
    mLocalOutputStream.flush();
  }

  /**
   * Closes this output stream. When an output stream is closed, the local temporary file is
   * uploaded to OSS Service. Once the file is uploaded, the temporary file is deleted.
   */
  @Override
  public void close() throws IOException {
    if (mClosed.getAndSet(true)) {
      return;
    }
    mLocalOutputStream.close();
    try {
      BufferedInputStream in = new BufferedInputStream(
          new FileInputStream(mFile));

      int len = (int)mFile.length();
      byte[] buf = new byte[len];
      int r = in.read(buf);

      mIoCtx.writeFull(mKey, buf, r);
      mFile.delete();
    } catch (RadosException e) {
      LOG.error("Failed to upload {}. Temporary file @ {}", mKey, mFile.getPath());
      throw new IOException(e);
    }
  }
}
