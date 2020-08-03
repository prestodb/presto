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
import com.facebook.presto.cache.CachingFileSystem;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

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
        this.localCacheFileSystem = new LocalCacheFileSystem(dataTier);
        localCacheFileSystem.initialize(uri, configuration);
    }

    @Override
    public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext)
            throws Exception
    {
        if (hiveFileContext.isCacheable()) {
            FileStatus fileStatus = dataTier.getFileStatus(path);
            // URIStatus is the mechanism to pass the hiveFileContext to the source filesystem
            AlluxioURIStatus alluxioURIStatus = new AlluxioURIStatus(fileStatus, hiveFileContext);
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
