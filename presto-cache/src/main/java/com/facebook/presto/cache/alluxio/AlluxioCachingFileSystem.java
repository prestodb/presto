/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cache.alluxio;

import alluxio.hadoop.LocalCacheFileSystem;
import alluxio.wire.FileInfo;
import com.facebook.presto.cache.CachingFileSystem;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;

public class AlluxioCachingFileSystem
        extends CachingFileSystem
{
    private static final int BUFFER_SIZE = 65536;
    private final boolean cacheValidationEnabled;
    private LocalCacheFileSystem localCacheFileSystem;

    public AlluxioCachingFileSystem(ExtendedFileSystem dataTier, URI uri)
    {
        this(dataTier, uri, false);
    }

    public AlluxioCachingFileSystem(ExtendedFileSystem dataTier, URI uri, boolean cacheValidationEnabled)
    {
        super(dataTier, uri);
        this.cacheValidationEnabled = cacheValidationEnabled;
    }

    @Override
    public synchronized void initialize(URI uri, Configuration configuration)
            throws IOException
    {
        this.localCacheFileSystem = new LocalCacheFileSystem(dataTier, uriStatus -> {
            // URIStatus is the mechanism to pass the hiveFileContext to the source filesystem
            // hiveFileContext is critical to use to open file.
            checkState(uriStatus instanceof AlluxioURIStatus);
            HiveFileContext hiveFileContext = ((AlluxioURIStatus) uriStatus).getHiveFileContext();
            try {
                return dataTier.openFile(new Path(uriStatus.getPath()), hiveFileContext);
            }
            catch (Exception e) {
                throw new IOException("Failed to open file", e);
            }
        });
        localCacheFileSystem.initialize(uri, configuration);
    }

    @Override
    public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext)
            throws Exception
    {
        // Using Alluxio caching requires knowing file size for now
        if (hiveFileContext.isCacheable() && hiveFileContext.getFileSize().isPresent()) {
            // FilePath is a unique identifier for a file, however it can be a long string
            // hence using md5 hash of the file path as the identifier in the cache.
            // We don't set fileId because fileId is Alluxio specific
            FileInfo info = new FileInfo().setFileIdentifier(md5().hashString(path.toString(), UTF_8).toString())
                    .setPath(path.toString())
                    .setFolder(false)
                    .setLength(hiveFileContext.getFileSize().get());
            // URIStatus is the mechanism to pass the hiveFileContext to the source filesystem
            AlluxioURIStatus alluxioURIStatus = new AlluxioURIStatus(info, hiveFileContext);
            FSDataInputStream cachingInputStream = localCacheFileSystem.open(alluxioURIStatus, BUFFER_SIZE);
            if (cacheValidationEnabled) {
                return new CacheValidatingInputStream(
                        cachingInputStream, dataTier.openFile(path, hiveFileContext));
            }
            return cachingInputStream;
        }
        return dataTier.openFile(path, hiveFileContext);
    }
}
