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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.io.IOException;

/**
 * <p>
 * An {@link InputStream} for a CephFileSystem and corresponding
 * Ceph instance.
 */
public class CephInputStream extends InputStream {
  private static final Log LOG = LogFactory.getLog(CephInputStream.class);
  private boolean mClosed;

  private int mFileHandle;

  private long mFileLength;

  private CephMount mMount;

  private byte[] mBuffer;
  private int mBufPos = 0;
  private int mBufValid = 0;
  private long mCephPos = 0;

  /**
   * Create a new CephInputStream.
   * @param cephfs CephFileSystem
   * @param fh The filehandle provided by Ceph to reference
   * @param flength The current length of the file. If the length changes
   *                you will need to close and re-open it to access the new data
   * @param bufferSize Buffer size
   */
  public CephInputStream(CephMount cephfs,
      int fh, long flength, int bufferSize) {
    // Whoever's calling the constructor is responsible for doing the actual ceph_open
    // call and providing the file handle.
    mFileLength = flength;
    mFileHandle = fh;
    mClosed = false;
    mMount = cephfs;
    mBuffer = new byte[bufferSize];
    LOG.debug(
        "CephInputStream constructor: initializing stream with fh " + fh
        + " and file length " + flength);

  }

  private synchronized boolean fillBuffer() throws IOException {
    mBufValid = (int) mMount.read(mFileHandle, mBuffer, mBuffer.length, -1);
    mBufPos = 0;
    if (mBufValid < 0) {
      int err = mBufValid;

      mBufValid = 0;
      // attempt to reset to old position. If it fails, too bad.
      mMount.lseek(mFileHandle, mCephPos, CephMount.SEEK_SET);
      throw new IOException("Failed to fill read buffer! Error code:" + err);
    }
    mCephPos += mBufValid;
    return (mBufValid != 0);
  }

  /**
   * Get the current position of the stream.
   * @return Current position
   * @throws IOException throws
   */
  public synchronized long getPos() throws IOException {
    return mCephPos - mBufValid + mBufPos;
  }

  /**
   * Find the number of bytes remaining in the file.
   */
  @Override
  public synchronized int available() throws IOException {
    if (mClosed) {
      throw new IOException("file is closed");
    }
    return (int) (mFileLength - getPos());
  }

  /**
   * Seek.
   * @param targetPos Position
   * @throws IOException throws
   */
  public synchronized void seek(long targetPos) throws IOException {
    LOG.trace(
        "CephInputStream.seek: Seeking to position " + targetPos + " on fd "
        + mFileHandle);
    if (targetPos > mFileLength) {
      throw new IOException(
          "CephInputStream.seek: failed seek to position " + targetPos
          + " on fd " + mFileHandle + ": Cannot seek after EOF " + mFileLength);
    }
    long oldPos = mCephPos;

    mCephPos = mMount.lseek(mFileHandle, targetPos, CephMount.SEEK_SET);
    mBufValid = 0;
    mBufPos = 0;
    if (mCephPos < 0) {
      mCephPos = oldPos;
      throw new IOException("Ceph failed to seek to new position!");
    }
  }

  /**
   * Failovers are handled by the Ceph code at a very low level;
   * if there are issues that can be solved by changing sources
   * they'll be dealt with before anybody even tries to call this method!
   * @param targetPos Position
   * @return false
   */
  public synchronized boolean seekToNewSource(long targetPos) {
    return false;
  }

  /**
   * Read a byte from the file.
   * @return the next byte
   */
  @Override
  public synchronized int read() throws IOException {
    LOG.trace(
        "CephInputStream.read: Reading a single byte from fd " + mFileHandle
        + " by calling general read function");

    byte[] result = new byte[1];

    if (getPos() >= mFileLength) {
      return -1;
    }
    if (-1 == read(result, 0, 1)) {
      return -1;
    }
    if (result[0] < 0) {
      return 256 + (int) result[0];
    } else {
      return result[0];
    }
  }

  /**
   * Read a specified number of bytes from the file into a byte[].
   * @param buf the byte array to read into
   * @param off the offset to start at in the file
   * @param len the number of bytes to read
   * @return 0 if successful, otherwise an error code
   * @throws IOException on bad input
   */
  @Override
  public synchronized int read(byte[] buf, int off, int len)
    throws IOException {
    LOG.trace(
        "CephInputStream.read: Reading " + len + " bytes from fd " + mFileHandle);

    if (mClosed) {
      throw new IOException(
          "CephInputStream.read: cannot read " + len + " bytes from fd "
          + mFileHandle + ": stream closed");
    }

    // ensure we're not past the end of the file
    if (getPos() >= mFileLength) {
      LOG.debug(
          "CephInputStream.read: cannot read " + len + " bytes from fd "
          + mFileHandle + ": current position is " + getPos()
          + " and file length is " + mFileLength);
      return -1;
    }

    int totalRead = 0;
    int initialLen = len;
    int read;

    do {
      read = Math.min(len, mBufValid - mBufPos);
      try {
        System.arraycopy(mBuffer, mBufPos, buf, off, read);
      } catch (IndexOutOfBoundsException ie) {
        throw new IOException(
            "CephInputStream.read: Indices out of bounds:" + "read length is "
            + len + ", buffer offset is " + off + ", and buffer size is "
            + buf.length);
      } catch (ArrayStoreException ae) {
        throw new IOException(
            "Uh-oh, CephInputStream failed to do an array"
                + "copy due to type mismatch...");
      } catch (NullPointerException ne) {
        throw new IOException(
            "CephInputStream.read: cannot read " + len + "bytes from fd:"
            + mFileHandle + ": buf is null");
      }
      mBufPos += read;
      len -= read;
      off += read;
      totalRead += read;
    } while (len > 0 && fillBuffer());

    LOG.trace(
        "CephInputStream.read: Reading " + initialLen + " bytes from fd "
        + mFileHandle + ": succeeded in reading " + totalRead + " bytes");
    return totalRead;
  }

  /**
   * Close the CephInputStream and release the associated filehandle.
   */
  @Override
  public void close() throws IOException {
    LOG.trace("CephOutputStream.close:enter");
    if (!mClosed) {
      mMount.close(mFileHandle);

      mClosed = true;
      LOG.trace("CephOutputStream.close:exit");
    }
  }
}
