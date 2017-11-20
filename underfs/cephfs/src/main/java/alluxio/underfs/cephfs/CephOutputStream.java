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

package alluxio.underfs.cephfs;

import com.ceph.fs.CephMount;

import java.io.IOException;
import java.io.OutputStream;

/**
 * <p>
 * An {@link OutputStream} for a CephFileSystem and corresponding
 * Ceph instance.
 *
 * TODO(runsisi):
 *  - When libcephfs-jni supports ByteBuffer interface we can get rid of the
 *  use of the buffer here to reduce memory copies and just use buffers in
 *  libcephfs. Currently it might be useful to reduce JNI crossings, but not
 *  much more.
 */
public class CephOutputStream extends OutputStream {
  private boolean mClosed;

  private CephMount mMount;

  private int mFileHandle;

  private byte[] mBuffer;
  private int mBufUsed = 0;

  /**
   * Construct the CephOutputStream.
   * @param cephfs CephFileSystem
   * @param fh The Ceph filehandle to connect to
   * @param bufferSize Buffer size
   */
  public CephOutputStream(CephMount cephfs, int fh, int bufferSize) {
    mMount = cephfs;
    mFileHandle = fh;
    mClosed = false;
    mBuffer = new byte[bufferSize];
  }

  /**
   * Ensure that the stream is opened.
   */
  private synchronized void checkOpen() throws IOException {
    if (mClosed) {
      throw new IOException("operation on closed stream (fd=" + mFileHandle + ")");
    }
  }

  /**
   * Get the current position in the file.
   * @return The file offset in bytes
   */
  public synchronized long getPos() throws IOException {
    checkOpen();
    return mMount.lseek(mFileHandle, 0, CephMount.SEEK_CUR);
  }

  @Override
  public synchronized void write(int b) throws IOException {
    byte[] buf = new byte[1];
    buf[0] = (byte) b;
    write(buf, 0, 1);
  }

  @Override
  public synchronized void write(byte[] buf, int off, int len) throws IOException {
    checkOpen();

    while (len > 0) {
      int remaining = Math.min(len, mBuffer.length - mBufUsed);
      System.arraycopy(buf, off, mBuffer, mBufUsed, remaining);

      mBufUsed += remaining;
      off += remaining;
      len -= remaining;

      if (mBuffer.length == mBufUsed) {
        flushBuffer();
      }
    }
  }

  /*
   * Moves data from the buffer into libcephfs.
   */
  private synchronized void flushBuffer() throws IOException {
    if (mBufUsed == 0) {
      return;
    }

    while (mBufUsed > 0) {
      int ret = (int) mMount.write(mFileHandle, mBuffer, mBufUsed, -1);
      if (ret < 0) {
        throw new IOException("ceph.mount.write: ret=" + ret);
      }

      if (ret == mBufUsed) {
        mBufUsed = 0;
        return;
      }

      assert (ret > 0);
      assert (ret < mBufUsed);

      /*
       * TODO(runsisi): handle a partial write by shifting the remainder of the data in
       * the buffer back to the beginning and retrying the write. It would
       * probably be better to use a ByteBuffer 'view' here, and I believe
       * using a ByteBuffer has some other performance benefits but we'll
       * likely need to update the libcephfs-jni implementation.
       */
      int remaining = mBufUsed - ret;
      System.arraycopy(mBuffer, ret, mBuffer, 0, remaining);
      mBufUsed -= ret;
    }

    assert (mBufUsed == 0);
  }

  @Override
  public synchronized void flush() throws IOException {
    checkOpen();
    flushBuffer(); // buffer -> libcephfs
    mMount.fsync(mFileHandle, false); // libcephfs -> cluster
  }

  @Override
  public synchronized void close() throws IOException {
    checkOpen();
    flush();
    mMount.close(mFileHandle);
    mClosed = true;
  }
}
