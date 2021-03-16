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
import com.google.common.annotations.VisibleForTesting;
import io.airlift.units.DataSize;

import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class OrcWriterOptions
{
    private static final DataSize DEFAULT_STRIPE_MIN_SIZE = new DataSize(32, MEGABYTE);
    private static final DataSize DEFAULT_STRIPE_MAX_SIZE = new DataSize(64, MEGABYTE);
    private static final int DEFAULT_STRIPE_MAX_ROW_COUNT = 10_000_000;
    private static final int DEFAULT_ROW_GROUP_MAX_ROW_COUNT = 10_000;
    private static final DataSize DEFAULT_DICTIONARY_MAX_MEMORY = new DataSize(16, MEGABYTE);

    @VisibleForTesting
    static final DataSize DEFAULT_MAX_STRING_STATISTICS_LIMIT = new DataSize(64, BYTE);

    @VisibleForTesting
    static final DataSize DEFAULT_MAX_COMPRESSION_BUFFER_SIZE = new DataSize(256, KILOBYTE);

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

    public OrcWriterOptions()
    {
        this(
                DEFAULT_STRIPE_MIN_SIZE,
                DEFAULT_STRIPE_MAX_SIZE,
                DEFAULT_STRIPE_MAX_ROW_COUNT,
                DEFAULT_ROW_GROUP_MAX_ROW_COUNT,
                DEFAULT_DICTIONARY_MAX_MEMORY,
                DEFAULT_MAX_STRING_STATISTICS_LIMIT,
                DEFAULT_MAX_COMPRESSION_BUFFER_SIZE,
                OptionalInt.empty(),
                new ByStreamSize(),
                false);
    }

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
            boolean integerDictionaryEncodingEnabled)
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

    public OrcWriterOptions withStripeMinSize(DataSize stripeMinSize)
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
                integerDictionaryEncodingEnabled);
    }

    public OrcWriterOptions withStripeMaxSize(DataSize stripeMaxSize)
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
                integerDictionaryEncodingEnabled);
    }

    public OrcWriterOptions withStripeMaxRowCount(int stripeMaxRowCount)
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
                integerDictionaryEncodingEnabled);
    }

    public OrcWriterOptions withRowGroupMaxRowCount(int rowGroupMaxRowCount)
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
                integerDictionaryEncodingEnabled);
    }

    public OrcWriterOptions withDictionaryMaxMemory(DataSize dictionaryMaxMemory)
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
                integerDictionaryEncodingEnabled);
    }

    public OrcWriterOptions withMaxStringStatisticsLimit(DataSize maxStringStatisticsLimit)
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
                integerDictionaryEncodingEnabled);
    }

    public OrcWriterOptions withMaxCompressionBufferSize(DataSize maxCompressionBufferSize)
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
                integerDictionaryEncodingEnabled);
    }

    public OrcWriterOptions withCompressionLevel(OptionalInt compressionLevel)
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
                integerDictionaryEncodingEnabled);
    }

    public OrcWriterOptions withStreamLayout(StreamLayout streamLayout)
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
                integerDictionaryEncodingEnabled);
    }

    public OrcWriterOptions withIntegerDictionaryEncodingEnabled(boolean integerDictionaryEncodingEnabled)
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
                integerDictionaryEncodingEnabled);
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
                .toString();
    }
}
