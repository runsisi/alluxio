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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.ceph.rados.jna.RadosObjectInfo;
import com.ceph.rados.Rados;
import com.ceph.rados.IoCTX;
import com.ceph.rados.ListCtx;
import com.ceph.rados.exceptions.RadosException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.io.File;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Ceph RADOS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class RadosUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(RadosUnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Ceph RADOS client. */
  private final String mPoolName;
  private final Rados mRados;
  private final IoCTX mIoCtx;

  /**
   * Constructs a new instance of {@link RadosUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link RadosUnderFileSystem} instance
   */
  public static RadosUnderFileSystem createInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) throws Exception {
    // rados://poolName
    String poolName = UnderFileSystemUtils.getBucketName(uri);

    // construct RADOS client, IoCtx, TODO: get params from conf
    Rados rados = new Rados("admin");
    IoCTX ioctx;
    try {
      rados.confReadFile(new File("/etc/ceph/ceph.conf"));
      rados.connect();
      ioctx = rados.ioCtxCreate(poolName);
    } catch (RadosException e) {
      throw new IOException("RADOS init failed: ", e);
    }

    return new RadosUnderFileSystem(uri, conf, rados, ioctx, poolName);
  }

  /**
   * Constructor for {@link RadosUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param ossClient Aliyun OSS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param conf configuration for this UFS
   */
  protected RadosUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration conf,
                                 Rados rados, IoCTX ioctx, String poolName) {
    super(uri, conf);
    mPoolName = poolName;
    mRados = rados;
    mIoCtx = ioctx;
  }

  @Override
  public String getUnderFSType() {
    return "rados";
  }

  // No ACL integration currently, no-op
  @Override
  public void setOwner(String path, String user, String group) {}

  // No ACL integration currently, no-op
  @Override
  public void setMode(String path, short mode) throws IOException {}

  @Override
  protected boolean copyObject(String src, String dst) {
    try {
      LOG.info("Copying {} to {}", src, dst);
      RadosObjectInfo stat = mIoCtx.stat(src);
      int len = (int)stat.getSize();
      byte[] buf = new byte[len];
      int r = mIoCtx.read(src, len, 0, buf);
      mIoCtx.writeFull(dst, buf, r);

      return true;
    } catch (RadosException e) {
      LOG.error("Failed to rename file {} to {}", src, dst, e);
      return false;
    }
  }

  @Override
  protected boolean createEmptyObject(String key) {
    try {
      mIoCtx.writeFull(key, new byte[0], 0);
      return true;
    } catch (RadosException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new RadosOutputStream(key, mIoCtx);
  }

  @Override
  protected boolean deleteObject(String key) {
    try {
      mIoCtx.remove(key);
    } catch (RadosException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  @Override
  protected String getFolderSuffix() {
    return FOLDER_SUFFIX;
  }

  @Override
  protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
      throws IOException {
    String delimiter = recursive ? "" : PATH_SEPARATOR;
    key = PathUtils.normalizePath(key, PATH_SEPARATOR);
    // In case key is root (empty string) do not normalize prefix
    key = key.equals(PATH_SEPARATOR) ? "" : key;
    ListObjectsRequest request = new ListObjectsRequest(mBucketName);
    request.setPrefix(key);
    request.setMaxKeys(getListingChunkLength());
    request.setDelimiter(delimiter);

    ObjectListing result = getObjectListingChunk(request);
    if (result != null) {
      return new OSSObjectListingChunk(request, result);
    }
    return null;
  }

  // Get next chunk of listing result
  private ObjectListing getObjectListingChunk(ListObjectsRequest request) {
    ObjectListing result;
    try {
      result = mClient.listObjects(request);
    } catch (ServiceException e) {
      LOG.error("Failed to list path {}", request.getPrefix(), e);
      result = null;
    }
    return result;
  }

  /**
   * Wrapper over OSS {@link ObjectListingChunk}.
   */
  private final class OSSObjectListingChunk implements ObjectListingChunk {
    final ListObjectsRequest mRequest;
    final ObjectListing mResult;

    OSSObjectListingChunk(ListObjectsRequest request, ObjectListing result) throws IOException {
      mRequest = request;
      mResult = result;
      if (mResult == null) {
        throw new IOException("OSS listing result is null");
      }
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      List<OSSObjectSummary> objects = mResult.getObjectSummaries();
      ObjectStatus[] ret = new ObjectStatus[objects.size()];
      int i = 0;
      for (OSSObjectSummary obj : objects) {
        ret[i++] = new ObjectStatus(obj.getKey(), obj.getSize(), obj.getLastModified().getTime());
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      List<String> res = mResult.getCommonPrefixes();
      return res.toArray(new String[res.size()]);
    }

    @Override
    public ObjectListingChunk getNextChunk() throws IOException {
      if (mResult.isTruncated()) {
        ObjectListing nextResult = mClient.listObjects(mRequest);
        if (nextResult != null) {
          return new OSSObjectListingChunk(mRequest, nextResult);
        }
      }
      return null;
    }
  }

  @Override
  protected ObjectStatus getObjectStatus(String key) {
    try {
      ObjectMetadata meta = mClient.getObjectMetadata(mBucketName, key);
      if (meta == null) {
        return null;
      }
      return new ObjectStatus(key, meta.getContentLength(), meta.getLastModified().getTime());
    } catch (ServiceException e) {
      LOG.warn("Failed to get Object {}, return null", key, e);
      return null;
    }
  }

  // No ACL integration currently, returns default empty value
  @Override
  protected ObjectPermissions getPermissions() {
    return new ObjectPermissions("", "", Constants.DEFAULT_FILE_SYSTEM_MODE);
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_RADOS + mPoolName;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options) throws IOException {
    try {
      return new RadosInputStream(mBucketName, key, mClient, options.getOffset());
    } catch (ServiceException e) {
      throw new IOException(e.getMessage());
    }
  }
}
