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
package com.facebook.presto.cache;

import com.facebook.presto.cache.alluxio.AlluxioCachingFileSystem;
import com.facebook.presto.cache.filemerge.FileMergeCachingFileSystem;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;

import static alluxio.shaded.client.com.google.common.base.Preconditions.checkState;

public class CacheFactory
{
    public ExtendedFileSystem createCachingFileSystem(
            Configuration factoryConfig,
            URI factoryUri,
            ExtendedFileSystem fileSystem,
            CacheManager cacheManager,
            boolean cachingEnabled,
            CacheType cacheType,
            boolean validationEnabled)
            throws IOException
    {
        if (!cachingEnabled) {
            return fileSystem;
        }

        checkState(cacheType != null);

        switch (cacheType) {
            case FILE_MERGE:
                return new FileMergeCachingFileSystem(
                        factoryUri,
                        factoryConfig,
                        cacheManager,
                        fileSystem,
                        validationEnabled);
            case ALLUXIO:
                ExtendedFileSystem cachingFileSystem = new AlluxioCachingFileSystem(fileSystem, factoryUri, validationEnabled);
                cachingFileSystem.initialize(factoryUri, factoryConfig);
                return cachingFileSystem;
            default:
                throw new IllegalArgumentException("Invalid CacheType: " + cacheType.name());
        }
    }
}
