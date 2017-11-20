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

import alluxio.AlluxioURI;
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.AtomicFileOutputStream;
import alluxio.underfs.AtomicFileOutputStreamCallback;
import alluxio.underfs.BaseUnderFileSystem;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import alluxio.util.io.PathUtils;

import com.ceph.crush.Bucket;
import com.ceph.fs.CephFileAlreadyExistsException;
import com.ceph.fs.CephFileExtent;
import com.ceph.fs.CephMount;
import com.ceph.fs.CephNotDirectoryException;
import com.ceph.fs.CephStat;
import com.ceph.fs.CephStatVFS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * CephFS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class CephFSUnderFileSystem extends BaseUnderFileSystem
    implements AtomicFileOutputStreamCallback {
  private static final Logger LOG = LoggerFactory.getLogger(CephFSUnderFileSystem.class);
  private static final int MAX_TRY = 5;

  private CephMount mMount;

  /**
   * Factory method to constructs a new HDFS {@link UnderFileSystem} instance.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for Hadoop
   * @return a new CephFS {@link UnderFileSystem} instance
   */
  static CephFSUnderFileSystem createInstance(
      AlluxioURI ufsUri, UnderFileSystemConfiguration conf) {
    return new CephFSUnderFileSystem(ufsUri, conf);
  }

  /**
   * Constructs a new CephFS {@link UnderFileSystem}.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   */
  CephFSUnderFileSystem(AlluxioURI ufsUri, UnderFileSystemConfiguration conf) {
    super(ufsUri, conf);

    try {
      /*
       * Create mount with auth user id
       */
      String userId = conf.getValue(PropertyKey.UNDERFS_CEPHFS_AUTH_ID);
      mMount = new CephMount(userId);

      /*
       * Load a configuration file if specified
       */
      String configfile = conf.getValue(PropertyKey.UNDERFS_CEPHFS_CONF_FILE);
      if (configfile != null) {
        mMount.conf_read_file(configfile);
      }

      /* Set auth keyfile */
      String keyfile = conf.getValue(PropertyKey.UNDERFS_CEPHFS_AUTH_KEYFILE);
      if (keyfile != null) {
        mMount.conf_set("keyfile", keyfile);
      }

      /* Set auth keyring */
      String keyring = conf.getValue(PropertyKey.UNDERFS_CEPHFS_AUTH_KEYRING);
      if (keyring != null) {
        mMount.conf_set("keyring", keyring);
      }

      /* Set monitor */
      String monAddr;
      String monHost = ufsUri.getHost();
      int monPort = ufsUri.getPort();
      if (monHost != null && monPort != -1) {
        monAddr = monHost + ":" + monPort;
      } else {
        monAddr = conf.getValue(PropertyKey.UNDERFS_CEPHFS_MON_ADDR);
      }
      if (monAddr != null) {
        mMount.conf_set("mon_host", monAddr);
      }

      /*
       * Parse and set Ceph configuration options
       */
      String configopts = conf.getValue(PropertyKey.UNDERFS_CEPHFS_CONF_OPTS);
      if (configopts != null) {
        String[] options = configopts.split(",");
        for (String option : options) {
          String[] keyval = option.split("=");
          if (keyval.length != 2) {
            throw new IllegalArgumentException("Invalid Ceph option: " + option);
          }
          String key = keyval[0];
          String val = keyval[1];
          try {
            mMount.conf_set(key, val);
          } catch (Exception e) {
            throw new IOException("Error setting Ceph option " + key + " = " + val);
          }
        }
      }

      /*
       * Use a different root?
       */
      String root = conf.getValue(PropertyKey.UNDERFS_CEPHFS_ROOT_DIR);

      /* Actually mount the file system */
      mMount.mount(root);

      /*
       * Allow reads from replica objects?
       */
      String localizeReads = conf.getValue(PropertyKey.UNDERFS_CEPHFS_LOCALIZE_READS);
      boolean bLocalize = Boolean.parseBoolean(localizeReads);
      mMount.localize_reads(bLocalize);

      mMount.chdir("/");
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to get CephFS FileSystem client for %s", ufsUri), e);
    }
  }

  @Override
  public String getUnderFSType() {
    return "cephfs";
  }

  @Override
  public void close() throws IOException {
    if (null != mMount) {
      mMount.unmount();
    }
    mMount = null;
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    if (!options.isEnsureAtomic()) {
      return createDirect(path, options);
    }
    return new AtomicFileOutputStream(path, this, options);
  }

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
    path = stripPath(path);
    String parentPath;
    try {
      parentPath = PathUtils.getParent(path);
    } catch (InvalidPathException e) {
      throw new IOException("Invalid path");
    }

    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        // TODO(runsisi): support creating CephFS files with specified block size and replication.
        if (options.getCreateParent()) {
          if (mkdirs(parentPath, MkdirsOptions.defaults()) && !isDirectory(parentPath)) {
            throw new IOException(ExceptionMessage.PARENT_CREATION_FAILED.getMessage(path));
          }
        }

        int flags = CephMount.O_WRONLY | CephMount.O_CREAT | CephMount.O_TRUNC;
        short mode = options.getMode().toShort();
        int bufferSize = 1 << 22;

        int fd = openFile(path, flags, mode);

        return new CephOutputStream(mMount, fd, bufferSize);
      } catch (IOException e) {
        LOG.warn("Retry count {} : {} ", retryPolicy.getRetryCount(), e.getMessage());
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    path = stripPath(path);
    if (isDirectory(path)) {
      IOException te = null;
      RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
      while (retryPolicy.attemptRetry()) {
        try {
          return delete(path, options.isRecursive());
        } catch (IOException e) {
          LOG.warn("Retry count {} : {}", retryPolicy.getRetryCount(), e.getMessage());
          te = e;
        }
      }
      throw te;
    }
    return false;
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    path = stripPath(path);
    if (isFile(path)) {
      IOException te = null;
      RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
      while (retryPolicy.attemptRetry()) {
        try {
          return delete(path, false);
        } catch (IOException e) {
          LOG.warn("Retry count {} : {}", retryPolicy.getRetryCount(), e.getMessage());
          te = e;
        }
      }
      throw te;
    }
    return false;
  }

  @Override
  public boolean exists(String path) throws IOException {
    path = stripPath(path);
    try {
      CephStat stat = new CephStat();
      lstat(path, stat);
      return true;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    path = stripPath(path);
    CephStat stat = new CephStat();
    lstat(path, stat);

    return stat.blksize;
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    path = stripPath(path);
    CephStat stat = new CephStat();
    lstat(path, stat);

    return new UfsDirectoryStatus(path, null, null, (short) stat.mode);
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return getFileLocations(path, FileLocationOptions.defaults());
  }

  @Override
  @Nullable
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    // If the user has hinted the underlying storage nodes are not co-located with Alluxio
    // workers, short circuit without querying the locations
    if (Boolean.valueOf(mUfsConf.getValue(PropertyKey.UNDERFS_HDFS_REMOTE))) {
      return null;
    }
    List<String> ret = new ArrayList<>();
    try {
      List<String> loc = getFileBlockLocations(path, options.getOffset(), 1);
      if (loc != null) {
        ret.addAll(loc);
      }
    } catch (IOException e) {
      LOG.warn("Unable to get file location for {} : {}", path, e.getMessage());
    }
    return ret;
  }

  /**
   * Get a BlockLocation object for each block in a file.
   *
   * @param path A FileStatus object corresponding to the file you want locations for
   * @param start The offset of the first part of the file you are interested in
   * @param len The amount of the file past the offset you are interested in
   * @return Where each object corresponds to a block within the given range
   */
  private List<String> getFileBlockLocations(String path, long start, long len) throws IOException {
    path = stripPath(path);
    int fh = openFile(path, CephMount.O_RDONLY, 0);
    if (fh < 0) {
      LOG.error("getFileBlockLocations:got error " + fh + ", exiting and returning null!");
      return null;
    }

    List<String> ret = new ArrayList<>();

    long curPos = start;
    long endOff = curPos + len;
    do {
      CephFileExtent extent = get_file_extent(fh, curPos);

      int[] osds = extent.getOSDs();
      String[] hosts = new String[osds.length];

      for (int i = 0; i < osds.length; i++) {
        /*
         * Grab the hostname and rack from the crush hierarchy. Current we
         * hard code the item types. For a more general treatment, we'll need
         * a new configuration option that allows users to map their custom
         * crush types to hosts and topology.
         */
        Bucket[] loc = get_osd_crush_location(osds[i]);
        for (Bucket bucket : loc) {
          String type = bucket.getType();
          if (type.compareTo("host") == 0) {
            hosts[i] = bucket.getName();
          }
        }
      }

      Collections.addAll(ret, hosts);

      curPos += extent.getLength();
    } while (curPos < endOff);

    mMount.close(fh);

    return ret;
  }

  /**
   * Get stat information on a file. This does not fill owner or group, as
   * Ceph's support for these is a bit different.
   * @param path The path to stat
   * @return FileStatus object containing the stat information
   * @throws FileNotFoundException if the path could not be resolved
   */
  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    path = stripPath(path);
    CephStat stat = new CephStat();
    lstat(path, stat);

    return new UfsFileStatus(path, stat.size, stat.m_time,
        null, null, (short) stat.mode);
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    path = stripPath(path);
    CephStatVFS stat = new CephStatVFS();
    statfs(path, stat);

    // Ignoring the path given, will give information for entire cluster
    // as Alluxio can load/store data out of entire CephFS cluster
    switch (type) {
      case SPACE_TOTAL:
        return stat.bsize * stat.blocks;
      case SPACE_USED:
        return stat.bsize * (stat.blocks - stat.bavail);
      case SPACE_FREE:
        return stat.bsize * stat.bavail;
      default:
        throw new IOException("Unknown space type: " + type);
    }
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    path = stripPath(path);
    CephStat stat = new CephStat();
    lstat(path, stat);

    return stat.isDir();
  }

  @Override
  public boolean isFile(String path) throws IOException {
    path = stripPath(path);
    CephStat stat = new CephStat();
    lstat(path, stat);

    return stat.isFile();
  }

  /**
   * Get the UfsStatus for each listing in a directory.
   * @param path The directory to get listings from
   * @return FileStatus[] containing one FileStatus for each directory listing;
   *         null if path does not exist.
   */
  @Override
  @Nullable
  public UfsStatus[] listStatus(String path) throws IOException {
    path = stripPath(path);
    String[] dirlist = listDirectory(path);
    if (dirlist != null) {
      UfsStatus[] status = new UfsStatus[dirlist.length];

      for (int i = 0; i < status.length; i++) {
        CephStat stat = new CephStat();
        lstat(PathUtils.concatPath(path, dirlist[i]), stat);

        if (!stat.isDir()) {
          status[i] = new UfsFileStatus(dirlist[i], stat.size, stat.m_time,
              null, null, (short) stat.mode);
        } else {
          status[i] = new UfsDirectoryStatus(dirlist[i], null, null,
              (short) stat.mode);
        }
      }
      return status;
    }
    return null;
  }

  @Override
  public void connectFromMaster(String host) throws IOException {
    // no-op
  }

  @Override
  public void connectFromWorker(String host) throws IOException {
    // no-op
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    path = stripPath(path);
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        if (exists(path)) {
          LOG.debug("Trying to create existing directory at {}", path);
          return false;
        }
        // Create directories one by one with explicit permissions to ensure no umask is applied,
        // using mkdirs will apply the permission only to the last directory
        Stack<String> dirsToMake = new Stack<>();
        dirsToMake.push(path);
        String parent = getParentPath(path);
        while (!exists(parent)) {
          dirsToMake.push(parent);
          parent = getParentPath(parent);
        }
        while (!dirsToMake.empty()) {
          String dirToMake = dirsToMake.pop();
          try {
            mMount.mkdirs(dirToMake, options.getMode().toShort());
          } catch (CephFileAlreadyExistsException e) {
            // can be ignored safely
          }
          // Set the owner to the Alluxio client user to achieve permission delegation.
          // Alluxio server-side user is required to be a HDFS superuser. If it fails to set owner,
          // proceeds with mkdirs and print out an warning message.
          try {
            setOwner(dirToMake, options.getOwner(), options.getGroup());
          } catch (IOException e) {
            LOG.warn("Failed to update the ufs dir ownership, default values will be used. " + e);
          }
        }
        return true;
      } catch (IOException e) {
        LOG.warn("{} try to make directory for {} : {}", retryPolicy.getRetryCount(), path,
            e.getMessage());
        te = e;
      }
    }
    throw te;
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    path = stripPath(path);
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    int bufferSize = 1 << 22;
    while (retryPolicy.attemptRetry()) {
      try {
        int fd = openFile(path, CephMount.O_RDONLY, 0);

        /* get file size */
        CephStat stat = new CephStat();
        mMount.fstat(fd, stat);

        CephInputStream inputStream = new CephInputStream(mMount, fd, stat.size, bufferSize);

        try {
          inputStream.seek(options.getOffset());
        } catch (IOException e) {
          inputStream.close();
          throw e;
        }
        return inputStream;
      } catch (IOException e) {
        LOG.warn("{} try to open {} : {}", retryPolicy.getRetryCount(), path, e.getMessage());
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    if (!isDirectory(src)) {
      LOG.warn("Unable to rename {} to {} because source does not exist or is a file", src, dst);
      return false;
    }
    return rename(src, dst);
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    if (!isFile(src)) {
      LOG.warn("Unable to rename {} to {} because source does not exist or is a directory", src,
          dst);
      return false;
    }
    return rename(src, dst);
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    // no-op, Ceph's support for these is a bit different
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    path = stripPath(path);
    mMount.chmod(path, mode);
  }

  @Override
  public boolean supportsFlush() {
    return true;
  }

  /**
   * @param path the path to strip the scheme from
   * @return the path, with the optional scheme stripped away
   */
  private String stripPath(String path) {
    return new AlluxioURI(path).getPath();
  }

  private String getParentPath(String path) throws IOException {
    try {
      return PathUtils.getParent(path);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  private String getFileName(String path) throws IOException {
    try {
      String parent = PathUtils.getParent(path);
      return PathUtils.subtractPaths(path, parent);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }

  /**
   * Get the file or directory for each listing in a directory.
   * @param path The directory to get listings from
   * @return FileStatus[] containing one FileStatus for each directory listing;
   *         null if path does not exist.
   */
  private String[] listDirectory(String path) throws IOException {
    if (isFile(path)) {
      return new String[]{path};
    }

    String[] dirlist = listdir(path);
    if (dirlist != null) {
      String[] list = new String[dirlist.length];
      for (int i = 0; i < list.length; i++) {
        list[i] = PathUtils.concatPath(path, dirlist[i]);
      }
      return list;
    } else {
      throw new FileNotFoundException("File " + path + " does not exist.");
    }
  }

  /**
   * Rename a file or folder to a file or folder.
   *
   * @param src path of source file or directory
   * @param dst path of destination file or directory
   * @return true if rename succeeds
   */
  private boolean rename(String src, String dst) throws IOException {
    src = stripPath(src);
    dst = stripPath(dst);
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        try {
          CephStat stat = new CephStat();
          lstat(dst, stat);
          if (stat.isDir()) {
            String fileName = getFileName(src);
            mMount.rename(src, PathUtils.concatPath(dst, fileName));
            return true;
          }
          return false;
        } catch (FileNotFoundException e) {
          // can be ignored safely
        }

        mMount.rename(src, dst);
        return true;
      } catch (IOException e) {
        LOG.warn("{} try to rename {} to {} : {}", retryPolicy.getRetryCount(), src, dst,
            e.getMessage());
        te = e;
      }
    }
    throw te;
  }

  /*
   * Open a file. Ceph will not complain if we open a directory, but this
   * isn't something that Hadoop expects and we should throw an exception in
   * this case.
   */
  private int openFile(String path, int flags, int mode) throws IOException {
    int fd = mMount.open(path, flags, mode);
    CephStat stat = new CephStat();
    mMount.fstat(fd, stat);
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
  private int openFile(String path, int flags, int mode, int stripe_unit, int stripe_count,
           int object_size, String data_pool) throws IOException {
    int fd = mMount.open(path, flags, mode, stripe_unit,
        stripe_count, object_size, data_pool);
    CephStat stat = new CephStat();
    mMount.fstat(fd, stat);
    if (stat.isDir()) {
      mMount.close(fd);
      throw new FileNotFoundException();
    }
    return fd;
  }

  private void lstat(String path, CephStat stat) throws IOException {
    try {
      mMount.lstat(path, stat);
    } catch (CephNotDirectoryException e) {
      throw new FileNotFoundException();
    }
  }

  private void statfs(String path, CephStatVFS stat) throws IOException {
    try {
      mMount.statfs(path, stat);
    } catch (FileNotFoundException e) {
      throw new FileNotFoundException();
    }
  }

  private String[] listdir(String path) throws IOException {
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

  private boolean delete(String path, boolean recursive) throws IOException {
    /* path exists? */
    CephStat stat = new CephStat();

    try {
      lstat(path, stat);
    } catch (FileNotFoundException e) {
      return false;
    }

    /* we're done if its a file */
    if (stat.isFile()) {
      mMount.unlink(path);
      return true;
    }

    /* get directory contents */
    String[] dirlist = listDirectory(path);
    if (dirlist == null) {
      return false;
    }

    if (!recursive && dirlist.length > 0) {
      throw new IOException("Directory " + path + "is not empty.");
    }

    for (String fs : dirlist) {
      if (!delete(fs, recursive)) {
        return false;
      }
    }

    mMount.rmdir(path);
    return true;
  }

  private Bucket[] get_osd_crush_location(int osd) throws IOException {
    return mMount.get_osd_crush_location(osd);
  }

  private CephFileExtent get_file_extent(int fd, long offset) throws IOException {
    return mMount.get_file_extent(fd, offset);
  }
}
