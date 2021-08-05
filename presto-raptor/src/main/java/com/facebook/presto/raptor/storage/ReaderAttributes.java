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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.RaptorSessionProperties;
import com.facebook.presto.spi.ConnectorSession;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class ReaderAttributes
{
    private final DataSize maxMergeDistance;
    private final DataSize maxReadSize;
    private final DataSize tinyStripeThreshold;
    private final DataSize streamBufferSize;
    private final boolean lazyReadSmallRanges;
    private final boolean zstdJniDecompressionEnabled;

    @Inject
    public ReaderAttributes(StorageManagerConfig config)
    {
        this(config.getOrcMaxMergeDistance(), config.getOrcMaxReadSize(), config.getOrcStreamBufferSize(), config.getOrcTinyStripeThreshold(), config.isOrcLazyReadSmallRanges(), config.isZstdJniDecompressionEnabled());
    }

    public ReaderAttributes(DataSize maxMergeDistance, DataSize maxReadSize, DataSize streamBufferSize, DataSize tinyStripeThreshold, boolean lazyReadSmallRanges, boolean zstdJniDecompressionEnabled)
    {
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxReadSize = requireNonNull(maxReadSize, "maxReadSize is null");
        this.tinyStripeThreshold = requireNonNull(tinyStripeThreshold, "tinyStripeThreshold is null");
        this.streamBufferSize = requireNonNull(streamBufferSize, "streamBufferSize is null");
        this.lazyReadSmallRanges = lazyReadSmallRanges;
        this.zstdJniDecompressionEnabled = zstdJniDecompressionEnabled;
    }

    public DataSize getMaxMergeDistance()
    {
        return maxMergeDistance;
    }

    public DataSize getMaxReadSize()
    {
        return maxReadSize;
    }

    public DataSize getStreamBufferSize()
    {
        return streamBufferSize;
    }

    public DataSize getTinyStripeThreshold()
    {
        return tinyStripeThreshold;
    }

    public boolean isLazyReadSmallRanges()
    {
        return lazyReadSmallRanges;
    }

    public boolean isZstdJniDecompressionEnabled()
    {
        return zstdJniDecompressionEnabled;
    }

    public static ReaderAttributes from(ConnectorSession session)
    {
        return new ReaderAttributes(
                RaptorSessionProperties.getReaderMaxMergeDistance(session),
                RaptorSessionProperties.getReaderMaxReadSize(session),
                RaptorSessionProperties.getReaderStreamBufferSize(session),
                RaptorSessionProperties.getReaderTinyStripeThreshold(session),
                RaptorSessionProperties.isReaderLazyReadSmallRanges(session),
                RaptorSessionProperties.isZstdJniDecompressionEnabled(session));
    }
}
