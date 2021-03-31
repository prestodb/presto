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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class OrcReaderOptions
{
    private final DataSize maxMergeDistance;
    private final DataSize tinyStripeThreshold;
    private final DataSize maxBlockSize;
    private final Map<OrcReaderFeature, Boolean> featureFlags;

    public OrcReaderOptions(DataSize maxMergeDistance, DataSize tinyStripeThreshold, DataSize maxBlockSize, boolean zstdJniDecompressionEnabled)
    {
        this(maxMergeDistance, tinyStripeThreshold, maxBlockSize, ImmutableMap.of(OrcReaderFeature.ZSTD_JNI_DECOMPRESSION, zstdJniDecompressionEnabled));
    }

    public OrcReaderOptions(
            DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            Map<OrcReaderFeature, Boolean> featureFlags)
    {
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxBlockSize = requireNonNull(maxBlockSize, "maxBlockSize is null");
        this.tinyStripeThreshold = requireNonNull(tinyStripeThreshold, "tinyStripeThreshold is null");
        this.featureFlags = ImmutableMap.copyOf(requireNonNull(featureFlags, "featureFlags is null"));
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
        return isFeatureEnabled(OrcReaderFeature.ZSTD_JNI_DECOMPRESSION);
    }

    public DataSize getTinyStripeThreshold()
    {
        return tinyStripeThreshold;
    }

    public boolean mapNullKeysEnabled()
    {
        return isFeatureEnabled(OrcReaderFeature.MAP_NULL_KEYS);
    }

    public boolean enableTimestampMicroPrecision()
    {
        return isFeatureEnabled(OrcReaderFeature.TIMESTAMP_MICRO_PRECISION);
    }

    public boolean isStripeMetaCacheEnabled()
    {
        return isFeatureEnabled(OrcReaderFeature.STRIPE_META_CACHE);
    }

    private boolean isFeatureEnabled(OrcReaderFeature feature)
    {
        return featureFlags.getOrDefault(feature, false);
    }
}
