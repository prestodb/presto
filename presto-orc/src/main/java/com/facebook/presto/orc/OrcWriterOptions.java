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

import com.facebook.presto.orc.StreamLayout.ByStreamSize;
import com.facebook.presto.orc.metadata.DwrfStripeCacheMode;
import io.airlift.units.DataSize;

import java.util.OptionalInt;

import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.INDEX_AND_FOOTER;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class OrcWriterOptions
{
    public static final DataSize DEFAULT_STRIPE_MIN_SIZE = new DataSize(32, MEGABYTE);
    public static final DataSize DEFAULT_STRIPE_MAX_SIZE = new DataSize(64, MEGABYTE);
    public static final int DEFAULT_STRIPE_MAX_ROW_COUNT = 10_000_000;
    public static final int DEFAULT_ROW_GROUP_MAX_ROW_COUNT = 10_000;
    public static final DataSize DEFAULT_DICTIONARY_MAX_MEMORY = new DataSize(16, MEGABYTE);
    public static final DataSize DEFAULT_MAX_STRING_STATISTICS_LIMIT = new DataSize(64, BYTE);
    public static final DataSize DEFAULT_MAX_COMPRESSION_BUFFER_SIZE = new DataSize(256, KILOBYTE);
    public static final DataSize DEFAULT_DWRF_STRIPE_CACHE_MAX_SIZE = new DataSize(8, MEGABYTE);
    public static final DwrfStripeCacheMode DEFAULT_DWRF_STRIPE_CACHE_MODE = INDEX_AND_FOOTER;

    private final DataSize stripeMinSize;
    private final DataSize stripeMaxSize;
    private final int stripeMaxRowCount;
    private final int rowGroupMaxRowCount;
    private final DataSize dictionaryMaxMemory;
    private final DataSize maxStringStatisticsLimit;
    private final DataSize maxCompressionBufferSize;
    private final OptionalInt compressionLevel;
    private final StreamLayout streamLayout;
    private final boolean integerDictionaryEncodingEnabled;
    private final boolean stringDictionarySortingEnabled;
    private final boolean dwrfStripeCacheEnabled;
    private final DwrfStripeCacheMode dwrfStripeCacheMode;
    private final DataSize dwrfStripeCacheMaxSize;

    private OrcWriterOptions(
            DataSize stripeMinSize,
            DataSize stripeMaxSize,
            int stripeMaxRowCount,
            int rowGroupMaxRowCount,
            DataSize dictionaryMaxMemory,
            DataSize maxStringStatisticsLimit,
            DataSize maxCompressionBufferSize,
            OptionalInt compressionLevel,
            StreamLayout streamLayout,
            boolean integerDictionaryEncodingEnabled,
            boolean stringDictionarySortingEnabled,
            boolean dwrfStripeCacheEnabled,
            DwrfStripeCacheMode dwrfStripeCacheMode,
            DataSize dwrfStripeCacheMaxSize)
    {
        requireNonNull(stripeMinSize, "stripeMinSize is null");
        requireNonNull(stripeMaxSize, "stripeMaxSize is null");
        checkArgument(stripeMaxRowCount >= 1, "stripeMaxRowCount must be at least 1");
        checkArgument(rowGroupMaxRowCount >= 1, "rowGroupMaxRowCount must be at least 1");
        requireNonNull(dictionaryMaxMemory, "dictionaryMaxMemory is null");
        requireNonNull(maxStringStatisticsLimit, "maxStringStatisticsLimit is null");
        requireNonNull(maxCompressionBufferSize, "maxCompressionBufferSize is null");
        requireNonNull(compressionLevel, "compressionLevel is null");
        requireNonNull(streamLayout, "streamLayout is null");
        requireNonNull(dwrfStripeCacheMode, "dwrfStripeCacheMode is null");
        requireNonNull(dwrfStripeCacheMaxSize, "dwrfStripeCacheMaxSize is null");

        this.stripeMinSize = stripeMinSize;
        this.stripeMaxSize = stripeMaxSize;
        this.stripeMaxRowCount = stripeMaxRowCount;
        this.rowGroupMaxRowCount = rowGroupMaxRowCount;
        this.dictionaryMaxMemory = dictionaryMaxMemory;
        this.maxStringStatisticsLimit = maxStringStatisticsLimit;
        this.maxCompressionBufferSize = maxCompressionBufferSize;
        this.compressionLevel = compressionLevel;
        this.streamLayout = streamLayout;
        this.integerDictionaryEncodingEnabled = integerDictionaryEncodingEnabled;
        this.stringDictionarySortingEnabled = stringDictionarySortingEnabled;
        this.dwrfStripeCacheEnabled = dwrfStripeCacheEnabled;
        this.dwrfStripeCacheMode = dwrfStripeCacheMode;
        this.dwrfStripeCacheMaxSize = dwrfStripeCacheMaxSize;
    }

    public DataSize getStripeMinSize()
    {
        return stripeMinSize;
    }

    public DataSize getStripeMaxSize()
    {
        return stripeMaxSize;
    }

    public int getStripeMaxRowCount()
    {
        return stripeMaxRowCount;
    }

    public int getRowGroupMaxRowCount()
    {
        return rowGroupMaxRowCount;
    }

    public DataSize getDictionaryMaxMemory()
    {
        return dictionaryMaxMemory;
    }

    public DataSize getMaxStringStatisticsLimit()
    {
        return maxStringStatisticsLimit;
    }

    public DataSize getMaxCompressionBufferSize()
    {
        return maxCompressionBufferSize;
    }

    public OptionalInt getCompressionLevel()
    {
        return compressionLevel;
    }

    public StreamLayout getStreamLayout()
    {
        return streamLayout;
    }

    public boolean isIntegerDictionaryEncodingEnabled()
    {
        return integerDictionaryEncodingEnabled;
    }

    public boolean isStringDictionarySortingEnabled()
    {
        return stringDictionarySortingEnabled;
    }

    public boolean isDwrfStripeCacheEnabled()
    {
        return dwrfStripeCacheEnabled;
    }

    public DwrfStripeCacheMode getDwrfStripeCacheMode()
    {
        return dwrfStripeCacheMode;
    }

    public DataSize getDwrfStripeCacheMaxSize()
    {
        return dwrfStripeCacheMaxSize;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stripeMinSize", stripeMinSize)
                .add("stripeMaxSize", stripeMaxSize)
                .add("stripeMaxRowCount", stripeMaxRowCount)
                .add("rowGroupMaxRowCount", rowGroupMaxRowCount)
                .add("dictionaryMaxMemory", dictionaryMaxMemory)
                .add("maxStringStatisticsLimit", maxStringStatisticsLimit)
                .add("maxCompressionBufferSize", maxCompressionBufferSize)
                .add("compressionLevel", compressionLevel)
                .add("streamLayout", streamLayout)
                .add("integerDictionaryEncodingEnabled", integerDictionaryEncodingEnabled)
                .add("stringDictionarySortingEnabled", stringDictionarySortingEnabled)
                .add("dwrfStripeCacheEnabled", dwrfStripeCacheEnabled)
                .add("dwrfStripeCacheMode", dwrfStripeCacheMode)
                .add("dwrfStripeCacheMaxSize", dwrfStripeCacheMaxSize)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private DataSize stripeMinSize = DEFAULT_STRIPE_MIN_SIZE;
        private DataSize stripeMaxSize = DEFAULT_STRIPE_MAX_SIZE;
        private int stripeMaxRowCount = DEFAULT_STRIPE_MAX_ROW_COUNT;
        private int rowGroupMaxRowCount = DEFAULT_ROW_GROUP_MAX_ROW_COUNT;
        private DataSize dictionaryMaxMemory = DEFAULT_DICTIONARY_MAX_MEMORY;
        private DataSize maxStringStatisticsLimit = DEFAULT_MAX_STRING_STATISTICS_LIMIT;
        private DataSize maxCompressionBufferSize = DEFAULT_MAX_COMPRESSION_BUFFER_SIZE;
        private OptionalInt compressionLevel = OptionalInt.empty();
        private StreamLayout streamLayout = new ByStreamSize();
        private boolean integerDictionaryEncodingEnabled;
        private boolean stringDictionarySortingEnabled = true;
        private boolean dwrfStripeCacheEnabled;
        private DwrfStripeCacheMode dwrfStripeCacheMode = DEFAULT_DWRF_STRIPE_CACHE_MODE;
        private DataSize dwrfStripeCacheMaxSize = DEFAULT_DWRF_STRIPE_CACHE_MAX_SIZE;

        public Builder withStripeMinSize(DataSize stripeMinSize)
        {
            this.stripeMinSize = requireNonNull(stripeMinSize, "stripeMinSize is null");
            return this;
        }

        public Builder withStripeMaxSize(DataSize stripeMaxSize)
        {
            this.stripeMaxSize = requireNonNull(stripeMaxSize, "stripeMaxSize is null");
            return this;
        }

        public Builder withStripeMaxRowCount(int stripeMaxRowCount)
        {
            checkArgument(stripeMaxRowCount >= 1, "stripeMaxRowCount must be at least 1");
            this.stripeMaxRowCount = stripeMaxRowCount;
            return this;
        }

        public Builder withRowGroupMaxRowCount(int rowGroupMaxRowCount)
        {
            checkArgument(rowGroupMaxRowCount >= 1, "rowGroupMaxRowCount must be at least 1");
            this.rowGroupMaxRowCount = rowGroupMaxRowCount;
            return this;
        }

        public Builder withDictionaryMaxMemory(DataSize dictionaryMaxMemory)
        {
            this.dictionaryMaxMemory = requireNonNull(dictionaryMaxMemory, "dictionaryMaxMemory is null");
            return this;
        }

        public Builder withMaxStringStatisticsLimit(DataSize maxStringStatisticsLimit)
        {
            this.maxStringStatisticsLimit = requireNonNull(maxStringStatisticsLimit, "maxStringStatisticsLimit is null");
            return this;
        }

        public Builder withMaxCompressionBufferSize(DataSize maxCompressionBufferSize)
        {
            this.maxCompressionBufferSize = requireNonNull(maxCompressionBufferSize, "maxCompressionBufferSize is null");
            return this;
        }

        public Builder withCompressionLevel(OptionalInt compressionLevel)
        {
            this.compressionLevel = requireNonNull(compressionLevel, "compressionLevel is null");
            return this;
        }

        public Builder withStreamLayout(StreamLayout streamLayout)
        {
            this.streamLayout = requireNonNull(streamLayout, "streamLayout is null");
            return this;
        }

        public Builder withIntegerDictionaryEncodingEnabled(boolean integerDictionaryEncodingEnabled)
        {
            this.integerDictionaryEncodingEnabled = integerDictionaryEncodingEnabled;
            return this;
        }

        public Builder withStringDictionarySortingEnabled(boolean stringDictionarySortingEnabled)
        {
            this.stringDictionarySortingEnabled = stringDictionarySortingEnabled;
            return this;
        }

        public Builder withDwrfStripeCacheEnabled(boolean dwrfStripeCacheEnabled)
        {
            this.dwrfStripeCacheEnabled = dwrfStripeCacheEnabled;
            return this;
        }

        public Builder withDwrfStripeCacheMode(DwrfStripeCacheMode dwrfStripeCacheMode)
        {
            this.dwrfStripeCacheMode = requireNonNull(dwrfStripeCacheMode, "dwrfStripeCacheMode is null");
            return this;
        }

        public Builder withDwrfStripeCacheMaxSize(DataSize dwrfStripeCacheMaxSize)
        {
            this.dwrfStripeCacheMaxSize = requireNonNull(dwrfStripeCacheMaxSize, "dwrfStripeCacheMaxSize is null");
            return this;
        }

        public OrcWriterOptions build()
        {
            return new OrcWriterOptions(
                    stripeMinSize,
                    stripeMaxSize,
                    stripeMaxRowCount,
                    rowGroupMaxRowCount,
                    dictionaryMaxMemory,
                    maxStringStatisticsLimit,
                    maxCompressionBufferSize,
                    compressionLevel,
                    streamLayout,
                    integerDictionaryEncodingEnabled,
                    stringDictionarySortingEnabled,
                    dwrfStripeCacheEnabled,
                    dwrfStripeCacheMode,
                    dwrfStripeCacheMaxSize);
        }
    }
}
