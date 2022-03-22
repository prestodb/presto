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
package com.facebook.presto.orc;

import io.airlift.units.DataSize;

import static java.util.Objects.requireNonNull;

public class OrcReaderOptions
{
    private final DataSize maxMergeDistance;
    private final DataSize tinyStripeThreshold;
    private final DataSize maxBlockSize;
    private final boolean zstdJniDecompressionEnabled;
    private final boolean mapNullKeysEnabled;
    private final boolean enableTimestampMicroPrecision;

    public OrcReaderOptions(DataSize maxMergeDistance, DataSize tinyStripeThreshold, DataSize maxBlockSize, boolean zstdJniDecompressionEnabled)
    {
        this(maxMergeDistance, tinyStripeThreshold, maxBlockSize, zstdJniDecompressionEnabled, false, false);
    }

    public OrcReaderOptions(
            DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            boolean zstdJniDecompressionEnabled,
            boolean mapNullKeysEnabled,
            boolean enableTimestampMicroPrecision)
    {
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxBlockSize = requireNonNull(maxBlockSize, "maxBlockSize is null");
        this.tinyStripeThreshold = requireNonNull(tinyStripeThreshold, "tinyStripeThreshold is null");
        this.zstdJniDecompressionEnabled = zstdJniDecompressionEnabled;
        this.mapNullKeysEnabled = mapNullKeysEnabled;
        this.enableTimestampMicroPrecision = enableTimestampMicroPrecision;
    }

    public DataSize getMaxMergeDistance()
    {
        return maxMergeDistance;
    }

    public DataSize getMaxBlockSize()
    {
        return maxBlockSize;
    }

    public boolean isOrcZstdJniDecompressionEnabled()
    {
        return zstdJniDecompressionEnabled;
    }

    public DataSize getTinyStripeThreshold()
    {
        return tinyStripeThreshold;
    }

    public boolean mapNullKeysEnabled()
    {
        return mapNullKeysEnabled;
    }

    public boolean enableTimestampMicroPrecision()
    {
        return enableTimestampMicroPrecision;
    }
}
