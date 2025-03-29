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
package com.facebook.presto.orc.cache;

import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcWriteValidation;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;

public class CachingOrcFileTailSource
        implements OrcFileTailSource
{
    private final Cache<OrcDataSourceId, OrcFileTail> cache;
    private final OrcFileTailSource delegate;

    public CachingOrcFileTailSource(OrcFileTailSource delegate, Cache<OrcDataSourceId, OrcFileTail> cache)
    {
        this.cache = requireNonNull(cache, "cache is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public OrcFileTail getOrcFileTail(OrcDataSource orcDataSource, MetadataReader metadataReader, Optional<OrcWriteValidation> writeValidation, boolean cacheable, long fileModificationTime)
            throws IOException
    {
        if (!cacheable) {
            return delegate.getOrcFileTail(orcDataSource, metadataReader, writeValidation, cacheable, fileModificationTime);
        }
        try {
            OrcFileTail orcFileTail = cache.getIfPresent(orcDataSource.getId());
            if (orcFileTail != null) {
                if (orcFileTail.getFileModificationTime() == fileModificationTime) {
                    return orcFileTail;
                }
                cache.invalidate(orcDataSource.getId()); // stale entry
                // This get call is to increment the miss count for invalidated entries so the stats are recorded correctly.
                cache.getIfPresent(orcDataSource.getId());
            }
            orcFileTail = delegate.getOrcFileTail(orcDataSource, metadataReader, writeValidation, cacheable, fileModificationTime);
            cache.put(orcDataSource.getId(), orcFileTail);
            return orcFileTail;
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), IOException.class);
            throw new IOException("Unexpected error in orc file tail reading after cache miss", e.getCause());
        }
    }
}
