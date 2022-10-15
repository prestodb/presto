
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
package com.facebook.presto.parquet.cache;

import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.parquet.crypto.InternalFileDecryptor;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;

public class CachingParquetMetadataSource
        implements ParquetMetadataSource
{
    private final Cache<ParquetDataSourceId, ParquetFileMetadata> cache;
    private final ParquetMetadataSource delegate;

    public CachingParquetMetadataSource(Cache<ParquetDataSourceId, ParquetFileMetadata> cache, ParquetMetadataSource delegate)
    {
        this.cache = requireNonNull(cache, "cache is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public ParquetFileMetadata getParquetMetadata(
            ParquetDataSource parquetDataSource,
            long fileSize,
            boolean cacheable,
            long modificationTime,
            Optional<InternalFileDecryptor> fileDecryptor,
            boolean readMaskedValue)
            throws IOException
    {
        try {
            if (cacheable) {
                ParquetFileMetadata fileMetadataCache = cache.get(
                        parquetDataSource.getId(),
                        () -> delegate.getParquetMetadata(parquetDataSource, fileSize, cacheable, modificationTime, fileDecryptor, readMaskedValue));
                if (fileMetadataCache.getModificationTime() == modificationTime) {
                    return fileMetadataCache;
                }
                else {
                    cache.invalidate(parquetDataSource.getId());
                }
            }
            return delegate.getParquetMetadata(parquetDataSource, fileSize, cacheable, modificationTime, fileDecryptor, readMaskedValue);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), IOException.class);
            throw new IOException("Unexpected error in parquet metadata reading after cache miss", e.getCause());
        }
    }
}
