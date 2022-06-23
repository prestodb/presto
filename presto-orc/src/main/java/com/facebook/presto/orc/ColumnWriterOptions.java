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

import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.writer.CompressionBufferPool;
import com.facebook.presto.orc.writer.CompressionBufferPool.LastUsedCompressionBufferPool;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;

import java.util.OptionalInt;
import java.util.Set;

import static com.facebook.presto.orc.OrcWriterOptions.DEFAULT_MAX_COMPRESSION_BUFFER_SIZE;
import static com.facebook.presto.orc.OrcWriterOptions.DEFAULT_MAX_STRING_STATISTICS_LIMIT;
import static com.facebook.presto.orc.OrcWriterOptions.DEFAULT_PRESERVE_DIRECT_ENCODING_STRIPE_COUNT;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ColumnWriterOptions
{
    private final CompressionKind compressionKind;
    private final OptionalInt compressionLevel;
    private final int compressionMaxBufferSize;
    private final DataSize stringStatisticsLimit;
    private final boolean integerDictionaryEncodingEnabled;
    private final boolean stringDictionarySortingEnabled;
    private final boolean stringDictionaryEncodingEnabled;
    private final boolean ignoreDictionaryRowGroupSizes;
    private final int preserveDirectEncodingStripeCount;
    private final CompressionBufferPool compressionBufferPool;
    private final Set<Integer> flattenedNodes;

    public ColumnWriterOptions(
            CompressionKind compressionKind,
            OptionalInt compressionLevel,
            DataSize compressionMaxBufferSize,
            DataSize stringStatisticsLimit,
            boolean integerDictionaryEncodingEnabled,
            boolean stringDictionarySortingEnabled,
            boolean stringDictionaryEncodingEnabled,
            boolean ignoreDictionaryRowGroupSizes,
            int preserveDirectEncodingStripeCount,
            CompressionBufferPool compressionBufferPool,
            Set<Integer> flattenedNodes)
    {
        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
        this.compressionLevel = requireNonNull(compressionLevel, "compressionLevel is null");
        requireNonNull(compressionMaxBufferSize, "compressionMaxBufferSize is null");
        this.compressionMaxBufferSize = toIntExact(compressionMaxBufferSize.toBytes());
        this.stringStatisticsLimit = requireNonNull(stringStatisticsLimit, "stringStatisticsLimit is null");
        this.integerDictionaryEncodingEnabled = integerDictionaryEncodingEnabled;
        this.stringDictionarySortingEnabled = stringDictionarySortingEnabled;
        this.stringDictionaryEncodingEnabled = stringDictionaryEncodingEnabled;
        this.ignoreDictionaryRowGroupSizes = ignoreDictionaryRowGroupSizes;
        this.preserveDirectEncodingStripeCount = preserveDirectEncodingStripeCount;
        this.compressionBufferPool = requireNonNull(compressionBufferPool, "compressionBufferPool is null");
        this.flattenedNodes = requireNonNull(flattenedNodes, "flattenedNodes is null");
    }

    public CompressionKind getCompressionKind()
    {
        return compressionKind;
    }

    public OptionalInt getCompressionLevel()
    {
        return compressionLevel;
    }

    public int getCompressionMaxBufferSize()
    {
        return compressionMaxBufferSize;
    }

    public int getStringStatisticsLimit()
    {
        return toIntExact(stringStatisticsLimit.toBytes());
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

    public boolean isIgnoreDictionaryRowGroupSizes()
    {
        return ignoreDictionaryRowGroupSizes;
    }

    public int getPreserveDirectEncodingStripeCount()
    {
        return preserveDirectEncodingStripeCount;
    }

    public CompressionBufferPool getCompressionBufferPool()
    {
        return compressionBufferPool;
    }

    public Set<Integer> getFlattenedNodes()
    {
        return flattenedNodes;
    }

    /**
     * Create a copy of this ColumnWriterOptions, but disable string and integer dictionary encodings.
     */
    public ColumnWriterOptions copyWithDisabledDictionaryEncoding()
    {
        return toBuilder()
                .setStringDictionaryEncodingEnabled(false)
                .setIntegerDictionaryEncodingEnabled(false)
                .build();
    }

    public Builder toBuilder()
    {
        return new Builder()
                .setCompressionKind(getCompressionKind())
                .setCompressionLevel(getCompressionLevel())
                .setCompressionMaxBufferSize(new DataSize(getCompressionMaxBufferSize(), BYTE))
                .setStringStatisticsLimit(new DataSize(getStringStatisticsLimit(), BYTE))
                .setIntegerDictionaryEncodingEnabled(isIntegerDictionaryEncodingEnabled())
                .setStringDictionarySortingEnabled(isStringDictionarySortingEnabled())
                .setStringDictionaryEncodingEnabled(isStringDictionaryEncodingEnabled())
                .setIgnoreDictionaryRowGroupSizes(isIgnoreDictionaryRowGroupSizes())
                .setPreserveDirectEncodingStripeCount(getPreserveDirectEncodingStripeCount())
                .setCompressionBufferPool(getCompressionBufferPool())
                .setFlattenedNodes(getFlattenedNodes());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private CompressionKind compressionKind;
        private OptionalInt compressionLevel = OptionalInt.empty();
        private DataSize compressionMaxBufferSize = DEFAULT_MAX_COMPRESSION_BUFFER_SIZE;
        private DataSize stringStatisticsLimit = DEFAULT_MAX_STRING_STATISTICS_LIMIT;
        private boolean integerDictionaryEncodingEnabled;
        private boolean stringDictionarySortingEnabled = true;
        private boolean stringDictionaryEncodingEnabled = true;
        private boolean ignoreDictionaryRowGroupSizes;
        private int preserveDirectEncodingStripeCount = DEFAULT_PRESERVE_DIRECT_ENCODING_STRIPE_COUNT;
        private CompressionBufferPool compressionBufferPool = new LastUsedCompressionBufferPool();
        private Set<Integer> flattenedNodes = ImmutableSet.of();

        private Builder() {}

        public Builder setCompressionKind(CompressionKind compressionKind)
        {
            this.compressionKind = compressionKind;
            return this;
        }

        public Builder setCompressionLevel(OptionalInt compressionLevel)
        {
            this.compressionLevel = compressionLevel;
            return this;
        }

        public Builder setCompressionMaxBufferSize(DataSize compressionMaxBufferSize)
        {
            this.compressionMaxBufferSize = compressionMaxBufferSize;
            return this;
        }

        public Builder setStringStatisticsLimit(DataSize stringStatisticsLimit)
        {
            this.stringStatisticsLimit = stringStatisticsLimit;
            return this;
        }

        public Builder setIntegerDictionaryEncodingEnabled(boolean integerDictionaryEncodingEnabled)
        {
            this.integerDictionaryEncodingEnabled = integerDictionaryEncodingEnabled;
            return this;
        }

        public Builder setStringDictionarySortingEnabled(boolean stringDictionarySortingEnabled)
        {
            this.stringDictionarySortingEnabled = stringDictionarySortingEnabled;
            return this;
        }

        public Builder setStringDictionaryEncodingEnabled(boolean stringDictionaryEncodingEnabled)
        {
            this.stringDictionaryEncodingEnabled = stringDictionaryEncodingEnabled;
            return this;
        }

        public Builder setIgnoreDictionaryRowGroupSizes(boolean ignoreDictionaryRowGroupSizes)
        {
            this.ignoreDictionaryRowGroupSizes = ignoreDictionaryRowGroupSizes;
            return this;
        }

        public Builder setPreserveDirectEncodingStripeCount(int preserveDirectEncodingStripeCount)
        {
            this.preserveDirectEncodingStripeCount = preserveDirectEncodingStripeCount;
            return this;
        }

        public Builder setCompressionBufferPool(CompressionBufferPool compressionBufferPool)
        {
            this.compressionBufferPool = requireNonNull(compressionBufferPool, "compressionBufferPool is null");
            return this;
        }

        public Builder setFlattenedNodes(Set<Integer> flattenedNodes)
        {
            this.flattenedNodes = ImmutableSet.copyOf(flattenedNodes);
            return this;
        }

        public ColumnWriterOptions build()
        {
            return new ColumnWriterOptions(
                    compressionKind,
                    compressionLevel,
                    compressionMaxBufferSize,
                    stringStatisticsLimit,
                    integerDictionaryEncodingEnabled,
                    stringDictionarySortingEnabled,
                    stringDictionaryEncodingEnabled,
                    ignoreDictionaryRowGroupSizes,
                    preserveDirectEncodingStripeCount,
                    compressionBufferPool,
                    flattenedNodes);
        }
    }
}
