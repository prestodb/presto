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

import com.facebook.presto.orc.metadata.DwrfStripeCacheMode;
import com.facebook.presto.orc.writer.StreamLayoutFactory;
import com.facebook.presto.orc.writer.StreamLayoutFactory.ColumnSizeLayoutFactory;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.INDEX_AND_FOOTER;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class OrcWriterOptions
{
    public static final int DEFAULT_ROW_GROUP_MAX_ROW_COUNT = 10_000;
    public static final DataSize DEFAULT_DICTIONARY_MAX_MEMORY = new DataSize(16, MEGABYTE);
    public static final DataSize DEFAULT_DICTIONARY_MEMORY_ALMOST_FULL_RANGE = new DataSize(4, MEGABYTE);
    public static final int DEFAULT_DICTIONARY_USEFUL_CHECK_PER_CHUNK_FREQUENCY = Integer.MAX_VALUE;
    public static final DataSize DEFAULT_DICTIONARY_USEFUL_CHECK_COLUMN_SIZE = new DataSize(6, MEGABYTE);
    public static final DataSize DEFAULT_MAX_STRING_STATISTICS_LIMIT = new DataSize(64, BYTE);
    public static final DataSize DEFAULT_MAX_COMPRESSION_BUFFER_SIZE = new DataSize(256, KILOBYTE);
    public static final DataSize DEFAULT_DWRF_STRIPE_CACHE_MAX_SIZE = new DataSize(8, MEGABYTE);
    public static final DwrfStripeCacheMode DEFAULT_DWRF_STRIPE_CACHE_MODE = INDEX_AND_FOOTER;
    public static final int DEFAULT_PRESERVE_DIRECT_ENCODING_STRIPE_COUNT = 0;

    private final OrcWriterFlushPolicy flushPolicy;
    private final int rowGroupMaxRowCount;
    private final DataSize dictionaryMaxMemory;
    private final DataSize dictionaryMemoryAlmostFullRange;
    private final int dictionaryUsefulCheckPerChunkFrequency;
    private final DataSize dictionaryUsefulCheckColumnSize;
    private final DataSize maxStringStatisticsLimit;
    private final DataSize maxCompressionBufferSize;
    private final OptionalInt compressionLevel;
    private final StreamLayoutFactory streamLayoutFactory;
    private final boolean integerDictionaryEncodingEnabled;
    private final boolean stringDictionarySortingEnabled;
    private final boolean stringDictionaryEncodingEnabled;
    // TODO: Originally the dictionary row group sizes were not included in memory accounting due
    //  to a bug. Fixing the bug causes certain queries to OOM. When enabled this flag maintains the
    //  previous behavior so previously working queries will not OOM. The OOMs caused due to the
    //  additional memory accounting need to be fixed as well as the flag removed.
    private final boolean ignoreDictionaryRowGroupSizes;
    private final Optional<DwrfStripeCacheOptions> dwrfWriterOptions;
    private final int preserveDirectEncodingStripeCount;

    /**
     * Contains indexes of columns (not nodes!) for which writer should use flattened encoding, e.g. flat maps.
     */
    private final Set<Integer> flattenedColumns;

    private OrcWriterOptions(
            OrcWriterFlushPolicy flushPolicy,
            int rowGroupMaxRowCount,
            DataSize dictionaryMaxMemory,
            DataSize dictionaryMemoryAlmostFullRange,
            int dictionaryUsefulCheckPerChunkFrequency,
            DataSize dictionaryUsefulCheckColumnSize,
            DataSize maxStringStatisticsLimit,
            DataSize maxCompressionBufferSize,
            OptionalInt compressionLevel,
            StreamLayoutFactory streamLayoutFactory,
            boolean integerDictionaryEncodingEnabled,
            boolean stringDictionarySortingEnabled,
            boolean stringDictionaryEncodingEnabled,
            Optional<DwrfStripeCacheOptions> dwrfWriterOptions,
            boolean ignoreDictionaryRowGroupSizes,
            int preserveDirectEncodingStripeCount,
            Set<Integer> flattenedColumns)
    {
        requireNonNull(flushPolicy, "flushPolicy is null");
        checkArgument(rowGroupMaxRowCount >= 1, "rowGroupMaxRowCount must be at least 1");
        requireNonNull(dictionaryMaxMemory, "dictionaryMaxMemory is null");
        requireNonNull(dictionaryMemoryAlmostFullRange, "dictionaryMemoryAlmostFullRange is null");
        requireNonNull(dictionaryUsefulCheckColumnSize, "dictionaryUsefulCheckColumnSize is null");
        requireNonNull(maxStringStatisticsLimit, "maxStringStatisticsLimit is null");
        requireNonNull(maxCompressionBufferSize, "maxCompressionBufferSize is null");
        requireNonNull(compressionLevel, "compressionLevel is null");
        requireNonNull(streamLayoutFactory, "streamLayoutFactory is null");
        requireNonNull(dwrfWriterOptions, "dwrfWriterOptions is null");
        requireNonNull(flattenedColumns, "flattenedColumns is null");

        this.flushPolicy = flushPolicy;
        this.rowGroupMaxRowCount = rowGroupMaxRowCount;
        this.dictionaryMaxMemory = dictionaryMaxMemory;
        this.dictionaryMemoryAlmostFullRange = dictionaryMemoryAlmostFullRange;
        this.dictionaryUsefulCheckPerChunkFrequency = dictionaryUsefulCheckPerChunkFrequency;
        this.dictionaryUsefulCheckColumnSize = dictionaryUsefulCheckColumnSize;
        this.maxStringStatisticsLimit = maxStringStatisticsLimit;
        this.maxCompressionBufferSize = maxCompressionBufferSize;
        this.compressionLevel = compressionLevel;
        this.streamLayoutFactory = streamLayoutFactory;
        this.integerDictionaryEncodingEnabled = integerDictionaryEncodingEnabled;
        this.stringDictionarySortingEnabled = stringDictionarySortingEnabled;
        this.stringDictionaryEncodingEnabled = stringDictionaryEncodingEnabled;
        this.dwrfWriterOptions = dwrfWriterOptions;
        this.ignoreDictionaryRowGroupSizes = ignoreDictionaryRowGroupSizes;
        this.preserveDirectEncodingStripeCount = preserveDirectEncodingStripeCount;
        this.flattenedColumns = flattenedColumns;
    }

    public OrcWriterFlushPolicy getFlushPolicy()
    {
        return flushPolicy;
    }

    public int getRowGroupMaxRowCount()
    {
        return rowGroupMaxRowCount;
    }

    public DataSize getDictionaryMaxMemory()
    {
        return dictionaryMaxMemory;
    }

    public DataSize getDictionaryMemoryAlmostFullRange()
    {
        return dictionaryMemoryAlmostFullRange;
    }

    public int getDictionaryUsefulCheckPerChunkFrequency()
    {
        return dictionaryUsefulCheckPerChunkFrequency;
    }

    public DataSize getDictionaryUsefulCheckColumnSize()
    {
        return dictionaryUsefulCheckColumnSize;
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

    public StreamLayoutFactory getStreamLayoutFactory()
    {
        return streamLayoutFactory;
    }

    public boolean isIntegerDictionaryEncodingEnabled()
    {
        return integerDictionaryEncodingEnabled;
    }

    public boolean isStringDictionarySortingEnabled()
    {
        return stringDictionarySortingEnabled;
    }

    public boolean isStringDictionaryEncodingEnabled()
    {
        return stringDictionaryEncodingEnabled;
    }

    public Optional<DwrfStripeCacheOptions> getDwrfStripeCacheOptions()
    {
        return dwrfWriterOptions;
    }

    public boolean isIgnoreDictionaryRowGroupSizes()
    {
        return ignoreDictionaryRowGroupSizes;
    }

    public int getPreserveDirectEncodingStripeCount()
    {
        return preserveDirectEncodingStripeCount;
    }

    public Set<Integer> getFlattenedColumns()
    {
        return flattenedColumns;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("flushPolicy", flushPolicy)
                .add("rowGroupMaxRowCount", rowGroupMaxRowCount)
                .add("dictionaryMaxMemory", dictionaryMaxMemory)
                .add("dictionaryMemoryAlmostFullRange", dictionaryMemoryAlmostFullRange)
                .add("dictionaryUsefulCheckPerChunkFrequency", dictionaryUsefulCheckPerChunkFrequency)
                .add("dictionaryUsefulCheckColumnSize", dictionaryUsefulCheckColumnSize)
                .add("maxStringStatisticsLimit", maxStringStatisticsLimit)
                .add("maxCompressionBufferSize", maxCompressionBufferSize)
                .add("compressionLevel", compressionLevel)
                .add("streamLayoutFactory", streamLayoutFactory)
                .add("integerDictionaryEncodingEnabled", integerDictionaryEncodingEnabled)
                .add("stringDictionarySortingEnabled", stringDictionarySortingEnabled)
                .add("stringDictionaryEncodingEnabled", stringDictionaryEncodingEnabled)
                .add("dwrfWriterOptions", dwrfWriterOptions)
                .add("ignoreDictionaryRowGroupSizes", ignoreDictionaryRowGroupSizes)
                .add("preserveDirectEncodingStripeCount", preserveDirectEncodingStripeCount)
                .add("flattenedColumns", flattenedColumns)
                .toString();
    }

    public static OrcWriterOptions getDefaultOrcWriterOptions()
    {
        return OrcWriterOptions.builder().build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private OrcWriterFlushPolicy flushPolicy = DefaultOrcWriterFlushPolicy.builder().build();
        private int rowGroupMaxRowCount = DEFAULT_ROW_GROUP_MAX_ROW_COUNT;
        private DataSize dictionaryMaxMemory = DEFAULT_DICTIONARY_MAX_MEMORY;
        private DataSize dictionaryMemoryAlmostFullRange = DEFAULT_DICTIONARY_MEMORY_ALMOST_FULL_RANGE;
        private int dictionaryUsefulCheckPerChunkFrequency = DEFAULT_DICTIONARY_USEFUL_CHECK_PER_CHUNK_FREQUENCY;
        private DataSize dictionaryUsefulCheckColumnSize = DEFAULT_DICTIONARY_USEFUL_CHECK_COLUMN_SIZE;
        private DataSize maxStringStatisticsLimit = DEFAULT_MAX_STRING_STATISTICS_LIMIT;
        private DataSize maxCompressionBufferSize = DEFAULT_MAX_COMPRESSION_BUFFER_SIZE;
        private OptionalInt compressionLevel = OptionalInt.empty();
        private StreamLayoutFactory streamLayoutFactory = new ColumnSizeLayoutFactory();
        private boolean integerDictionaryEncodingEnabled;
        private boolean stringDictionarySortingEnabled = true;
        private boolean stringDictionaryEncodingEnabled = true;
        private boolean dwrfStripeCacheEnabled;
        private DwrfStripeCacheMode dwrfStripeCacheMode = DEFAULT_DWRF_STRIPE_CACHE_MODE;
        private DataSize dwrfStripeCacheMaxSize = DEFAULT_DWRF_STRIPE_CACHE_MAX_SIZE;
        private boolean ignoreDictionaryRowGroupSizes;
        private int preserveDirectEncodingStripeCount = DEFAULT_PRESERVE_DIRECT_ENCODING_STRIPE_COUNT;
        private Set<Integer> flattenedColumns = ImmutableSet.of();

        public Builder withFlushPolicy(OrcWriterFlushPolicy flushPolicy)
        {
            this.flushPolicy = requireNonNull(flushPolicy, "flushPolicy is null");
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

        public Builder withDictionaryMemoryAlmostFullRange(DataSize dictionaryMemoryAlmostFullRange)
        {
            this.dictionaryMemoryAlmostFullRange = requireNonNull(dictionaryMemoryAlmostFullRange, "dictionaryMemoryAlmostFullRange is null");
            return this;
        }

        public Builder withDictionaryUsefulCheckPerChunkFrequency(int dictionaryUsefulCheckPerChunkFrequency)
        {
            checkArgument(dictionaryUsefulCheckPerChunkFrequency >= 0, "dictionaryUsefulCheckPerChunkFrequency is negative");
            this.dictionaryUsefulCheckPerChunkFrequency = dictionaryUsefulCheckPerChunkFrequency;
            return this;
        }

        public Builder withDictionaryUsefulCheckColumnSize(DataSize dictionaryUsefulCheckColumnSize)
        {
            this.dictionaryUsefulCheckColumnSize = requireNonNull(dictionaryUsefulCheckColumnSize, "dictionaryUsefulCheckColumnSize is null");
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

        public Builder withStreamLayoutFactory(StreamLayoutFactory streamLayoutFactory)
        {
            this.streamLayoutFactory = requireNonNull(streamLayoutFactory, "streamLayoutFactory is null");
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

        public Builder withStringDictionaryEncodingEnabled(boolean stringDictionaryEncodingEnabled)
        {
            this.stringDictionaryEncodingEnabled = stringDictionaryEncodingEnabled;
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

        public Builder withIgnoreDictionaryRowGroupSizes(boolean ignoreDictionaryRowGroupSizes)
        {
            this.ignoreDictionaryRowGroupSizes = ignoreDictionaryRowGroupSizes;
            return this;
        }

        public Builder withPreserveDirectEncodingStripeCount(int preserveDirectEncodingStripeCount)
        {
            this.preserveDirectEncodingStripeCount = preserveDirectEncodingStripeCount;
            return this;
        }

        public Builder withFlattenedColumns(Set<Integer> flattenedColumns)
        {
            this.flattenedColumns = ImmutableSet.copyOf(flattenedColumns);
            return this;
        }

        public OrcWriterOptions build()
        {
            Optional<DwrfStripeCacheOptions> dwrfWriterOptions;
            if (dwrfStripeCacheEnabled) {
                dwrfWriterOptions = Optional.of(new DwrfStripeCacheOptions(dwrfStripeCacheMode, dwrfStripeCacheMaxSize));
            }
            else {
                dwrfWriterOptions = Optional.empty();
            }

            return new OrcWriterOptions(
                    flushPolicy,
                    rowGroupMaxRowCount,
                    dictionaryMaxMemory,
                    dictionaryMemoryAlmostFullRange,
                    dictionaryUsefulCheckPerChunkFrequency,
                    dictionaryUsefulCheckColumnSize,
                    maxStringStatisticsLimit,
                    maxCompressionBufferSize,
                    compressionLevel,
                    streamLayoutFactory,
                    integerDictionaryEncodingEnabled,
                    stringDictionarySortingEnabled,
                    stringDictionaryEncodingEnabled,
                    dwrfWriterOptions,
                    ignoreDictionaryRowGroupSizes,
                    preserveDirectEncodingStripeCount,
                    flattenedColumns);
        }
    }
}
