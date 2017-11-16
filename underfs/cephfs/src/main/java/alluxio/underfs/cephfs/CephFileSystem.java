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

import com.ceph.crush.Bucket;
import com.ceph.fs.CephFileAlreadyExistsException;
import com.ceph.fs.CephFileExtent;
import com.ceph.fs.CephMount;
import com.ceph.fs.CephNotDirectoryException;
import com.ceph.fs.CephStat;
import com.ceph.fs.CephStatVFS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;

class CephFileSystem {
  private CephMount mMount;

  /**
   * Create a new CephFileSystem.
   */
  CephFileSystem(CephMount mount) {
    mMount = mount;
  }

  /**
   * Open a Ceph file and attach the file handle to an InputStream.
   * @param path The file to open
   * @return InputStream reading from the given path
   * @throws IOException if the path DNE or is a
   * directory, or there is an error getting data to set up the InputStream
   */
  CephInputStream hl_open(String path) throws IOException {
    int bufferSize = 1 << 22;

    return hl_open(path, bufferSize);
  }

  /**
   * Open a Ceph file and attach the file handle to an InputStream.
   * @param path The file to open
   * @param bufferSize Ceph does internal buffering; but you can buffer in
   *   the Java code too if you like
   * @return InputStream reading from the given path
   * @throws IOException if the path DNE or is a
   * directory, or there is an error getting data to set up the InputStream
   */
  CephInputStream hl_open(String path, int bufferSize) throws IOException {
    // throws FileNotFoundException if path is a directory
    int fd = open(path, CephMount.O_RDONLY, 0);

    /* get file size */
    CephStat stat = new CephStat();
    fstat(fd, stat);

    return new CephInputStream(this, fd,
        stat.size, bufferSize);
  }

  /**
   * Get an FSDataOutputStream to append onto a file.
   * @param path The File you want to append onto
   * @return An FSDataOutputStream that connects to the file on Ceph
   * @throws IOException If the file cannot be found or appended to
   */
  CephOutputStream hl_append(String path) throws IOException {
    int bufferSize = 1 << 22;

    return hl_append(path, bufferSize);
  }

  /**
   * Get an FSDataOutputStream to append onto a file.
   * @param path The File you want to append onto
   * @param bufferSize Ceph does internal buffering but you can buffer in the
   *                   Java code as well if you like
   * Reporting is limited but exists
   * @return An FSDataOutputStream that connects to the file on Ceph
   * @throws IOException If the file cannot be found or appended to
   */
  CephOutputStream hl_append(String path, int bufferSize) throws IOException {
    int fd = open(path, CephMount.O_WRONLY | CephMount.O_APPEND, 0);

    return new CephOutputStream(this, fd, bufferSize);
  }

  boolean hl_exists(String path) throws IOException {
    try {
      CephStat stat = new CephStat();
      lstat(path, stat);
      return true;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Create a new file and open an CephOutputStream that's connected to it.
   * @param path The file to create
   * @param mode The permissions to apply to the file
   * @return An FSDataOutputStream pointing to the created file
   * @throws IOException if the path is an
   * existing directory, or there is a failure in attempting to open for
   * append with Ceph
   */
  CephOutputStream hl_create(String path, int mode) throws IOException {
    int flags = CephMount.O_WRONLY | CephMount.O_CREAT | CephMount.O_TRUNC;
    int bufferSize = 1 << 22;

    int fd = open(path, flags, mode);

    return new CephOutputStream(this, fd, bufferSize);
  }

  /**
   * Create a directory and any nonexistent parents. Any portion
   * of the directory tree can exist without error.
   * @param path The directory path to create
   * @param mode The permissions to apply to the created directories
   * @return true if successful, false otherwise
   * @throws IOException if the path is a child of a file
   */
  boolean hl_mkdirs(String path, int mode) throws IOException {
    boolean result = false;
    try {
      mkdirs(path, mode);
      result = true;
    } catch (CephFileAlreadyExistsException e) {
      result = true;
    }

    return result;
  }

  boolean isDirectory(String path) throws IOException {
    CephStat stat = new CephStat();
    lstat(path, stat);

    return stat.isDir();
  }

  boolean isFile(String path) throws IOException {
    CephStat stat = new CephStat();
    lstat(path, stat);

    return stat.isFile();
  }

  /*
   * Open a file. Allows directories to be opened. used internally to get the
   * pool name.
   */
  int __open(String path, int flags, int mode) throws IOException {
    return mMount.open(path, flags, mode);
  }

  /*
   * Open a file. Ceph will not complain if we open a directory, but this
   * isn't something that Hadoop expects and we should throw an exception in
   * this case.
   */
  int open(String path, int flags, int mode) throws IOException {
    int fd = __open(path, flags, mode);
    CephStat stat = new CephStat();
    fstat(fd, stat);
    if (stat.isDir()) {
      mMount.close(fd);
      throw new FileNotFoundException();
    }
    return fd;
  }

  /*
   * Same as open(path, flags, mode) alternative, but takes custom striping
   * parameters that are used when a file is being created.
   */
  int open(String path, int flags, int mode, int stripe_unit, int stripe_count,
           int object_size, String data_pool) throws IOException {
    int fd = mMount.open(path, flags, mode, stripe_unit,
        stripe_count, object_size, data_pool);
    CephStat stat = new CephStat();
    fstat(fd, stat);
    if (stat.isDir()) {
      mMount.close(fd);
      throw new FileNotFoundException();
    }
    return fd;
  }

  void fstat(int fd, CephStat stat) throws IOException {
    mMount.fstat(fd, stat);
  }

  void lstat(String path, CephStat stat) throws IOException {
    try {
      mMount.lstat(path, stat);
    } catch (CephNotDirectoryException e) {
      throw new FileNotFoundException();
    }
  }

  void statfs(String path, CephStatVFS stat) throws IOException {
    try {
      mMount.statfs(path, stat);
    } catch (FileNotFoundException e) {
      throw new FileNotFoundException();
    }
  }

  void rmdir(String path) throws IOException {
    mMount.rmdir(path);
  }

  void unlink(String path) throws IOException {
    mMount.unlink(path);
  }

  void rename(String src, String dst) throws IOException {
    mMount.rename(src, dst);
  }

  String[] listdir(String path) throws IOException {
    CephStat stat = new CephStat();
    try {
      mMount.lstat(path, stat);
    } catch (FileNotFoundException e) {
      return null;
    }
    if (!stat.isDir()) {
      return null;
    }
    return mMount.listdir(path);
  }

  void mkdirs(String path, int mode) throws IOException {
    mMount.mkdirs(path, mode);
  }

  void close(int fd) throws IOException {
    mMount.close(fd);
  }

  void chmod(String path, int mode) throws IOException {
    mMount.chmod(path, mode);
  }

  void shutdown() throws IOException {
    if (null != mMount) {
      mMount.unmount();
    }
    mMount = null;
  }

  void setattr(String path, CephStat stat, int mask) throws IOException {
    mMount.setattr(path, stat, mask);
  }

  void fsync(int fd) throws IOException {
    mMount.fsync(fd, false);
  }

  long lseek(int fd, long offset, int whence) throws IOException {
    return mMount.lseek(fd, offset, whence);
  }

  int write(int fd, byte[] buf, long size, long offset) throws IOException {
    return (int) mMount.write(fd, buf, size, offset);
  }

  int read(int fd, byte[] buf, long size, long offset) throws IOException {
    return (int) mMount.read(fd, buf, size, offset);
  }

  InetAddress get_osd_address(int osd) throws IOException {
    return mMount.get_osd_address(osd);
  }

  Bucket[] get_osd_crush_location(int osd) throws IOException {
    return mMount.get_osd_crush_location(osd);
  }

  CephFileExtent get_file_extent(int fd, long offset) throws IOException {
    return mMount.get_file_extent(fd, offset);
  }
}
