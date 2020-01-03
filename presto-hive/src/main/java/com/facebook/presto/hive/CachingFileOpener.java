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
package com.facebook.presto.hive;

import com.facebook.presto.cache.CacheManager;
import com.facebook.presto.cache.CachingFileSystem;
import com.facebook.presto.cache.CachingInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CachingFileOpener
        implements FileOpener
{
    private final CacheManager cacheManager;
    private final FileOpener fileOpener;

    @Inject
    public CachingFileOpener(
            CacheManager cacheManager,
            @ForCachingFileOpener FileOpener fileOpener)
    {
        this.cacheManager = requireNonNull(cacheManager, "Cache Manager is null");
        this.fileOpener = requireNonNull(fileOpener, "File opener is null");
    }

    @Override
    public CachingInputStream open(FileSystem fileSystem, Path path, Optional<byte[]> extraFileInfo)
            throws IOException
    {
        checkArgument(fileSystem instanceof CachingFileSystem, "fileSystem should be of CachingFileSystem type");
        CachingFileSystem cachingFileSystem = (CachingFileSystem) fileSystem;
        FSDataInputStream fsDataInputStream = fileOpener.open(cachingFileSystem.getDataTier(), path, extraFileInfo);
        return new CachingInputStream(fsDataInputStream, cacheManager, path, cachingFileSystem.isCacheValidationEnabled());
    }
}
