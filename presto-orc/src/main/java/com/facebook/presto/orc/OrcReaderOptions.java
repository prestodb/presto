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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class OrcReaderOptions
{
    private final DataSize maxMergeDistance;
    private final DataSize tinyStripeThreshold;
    private final DataSize maxBlockSize;
    private final boolean zstdJniDecompressionEnabled;
    private final boolean mapNullKeysEnabled;
    // if the option is set to true, OrcSelectiveReader will append a row number block at the end of the page
    private final boolean appendRowNumber;

    public OrcReaderOptions(DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            boolean zstdJniDecompressionEnabled)
    {
        this(maxMergeDistance, tinyStripeThreshold, maxBlockSize, zstdJniDecompressionEnabled, false, false);
    }

    public OrcReaderOptions(DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            boolean zstdJniDecompressionEnabled,
            boolean appendRowNumber)
    {
        this(maxMergeDistance, tinyStripeThreshold, maxBlockSize, zstdJniDecompressionEnabled, false, appendRowNumber);
    }

    public OrcReaderOptions(
            DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            boolean zstdJniDecompressionEnabled,
            boolean mapNullKeysEnabled,
            boolean appendRowNumber)
    {
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxBlockSize = requireNonNull(maxBlockSize, "maxBlockSize is null");
        this.tinyStripeThreshold = requireNonNull(tinyStripeThreshold, "tinyStripeThreshold is null");
        this.zstdJniDecompressionEnabled = zstdJniDecompressionEnabled;
        this.mapNullKeysEnabled = mapNullKeysEnabled;
        this.appendRowNumber = appendRowNumber;
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

    public boolean appendRowNumber()
    {
        return appendRowNumber;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("maxMergeDistance", maxMergeDistance)
                .add("tinyStripeThreshold", tinyStripeThreshold)
                .add("maxBlockSize", maxBlockSize)
                .add("zstdJniDecompressionEnabled", zstdJniDecompressionEnabled)
                .add("mapNullKeysEnabled", mapNullKeysEnabled)
                .add("appendRowNumber", appendRowNumber)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private DataSize maxMergeDistance;
        private DataSize tinyStripeThreshold;
        private DataSize maxBlockSize;
        private boolean zstdJniDecompressionEnabled;
        private boolean mapNullKeysEnabled;
        private boolean appendRowNumber;

        private Builder() {}

        public Builder withMaxMergeDistance(DataSize maxMergeDistance)
        {
            this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
            return this;
        }

        public Builder withTinyStripeThreshold(DataSize tinyStripeThreshold)
        {
            this.tinyStripeThreshold = requireNonNull(tinyStripeThreshold, "tinyStripeThreshold is null");
            return this;
        }

        public Builder withMaxBlockSize(DataSize maxBlockSize)
        {
            this.maxBlockSize = requireNonNull(maxBlockSize, "maxBlockSize is null");
            return this;
        }

        public Builder withZstdJniDecompressionEnabled(boolean zstdJniDecompressionEnabled)
        {
            this.zstdJniDecompressionEnabled = zstdJniDecompressionEnabled;
            return this;
        }

        public Builder withMapNullKeysEnabled(boolean mapNullKeysEnabled)
        {
            this.mapNullKeysEnabled = mapNullKeysEnabled;
            return this;
        }

        public Builder withAppendRowNumber(boolean appendRowNumber)
        {
            this.appendRowNumber = appendRowNumber;
            return this;
        }

        public OrcReaderOptions build()
        {
            return new OrcReaderOptions(
                    maxMergeDistance,
                    tinyStripeThreshold,
                    maxBlockSize,
                    zstdJniDecompressionEnabled,
                    mapNullKeysEnabled,
                    appendRowNumber);
        }
    }
}
