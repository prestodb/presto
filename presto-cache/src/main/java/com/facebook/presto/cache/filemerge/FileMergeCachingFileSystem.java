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
package com.facebook.presto.cache.filemerge;

import com.facebook.presto.cache.CacheManager;
import com.facebook.presto.cache.CachingFileSystem;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public final class FileMergeCachingFileSystem
        extends CachingFileSystem
{
    private final CacheManager cacheManager;
    private final boolean cacheValidationEnabled;

    public FileMergeCachingFileSystem(
            URI uri,
            Configuration configuration,
            CacheManager cacheManager,
            ExtendedFileSystem dataTier,
            boolean cacheValidationEnabled)
    {
        super(dataTier, uri);
        requireNonNull(configuration, "configuration is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.cacheValidationEnabled = cacheValidationEnabled;

        setConf(configuration);

        statistics = getStatistics(this.uri.getScheme(), getClass());
    }

    @Override
    public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext)
            throws Exception
    {
        if (hiveFileContext.isCacheable()) {
            return new FileMergeCachingInputStream(dataTier.openFile(path, hiveFileContext), cacheManager, path, hiveFileContext.getCacheQuota(), cacheValidationEnabled);
        }

        return dataTier.openFile(path, hiveFileContext);
    }

    public boolean isCacheValidationEnabled()
    {
        return cacheValidationEnabled;
    }
}
