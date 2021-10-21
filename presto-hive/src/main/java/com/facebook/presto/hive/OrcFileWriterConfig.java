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
package com.facebook.presto.hive;

import com.facebook.airlift.configuration.Config;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.orc.StreamLayout;
import com.facebook.presto.orc.metadata.DwrfStripeCacheMode;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

import static com.facebook.presto.hive.OrcFileWriterConfig.StreamLayoutType.BY_STREAM_SIZE;

@SuppressWarnings("unused")
public class OrcFileWriterConfig
{
    public enum StreamLayoutType
    {
        BY_STREAM_SIZE,
        BY_COLUMN_SIZE,
    }

    private DataSize stripeMinSize = OrcWriterOptions.DEFAULT_STRIPE_MIN_SIZE;
    private DataSize stripeMaxSize = OrcWriterOptions.DEFAULT_STRIPE_MAX_SIZE;
    private int stripeMaxRowCount = OrcWriterOptions.DEFAULT_STRIPE_MAX_ROW_COUNT;
    private int rowGroupMaxRowCount = OrcWriterOptions.DEFAULT_ROW_GROUP_MAX_ROW_COUNT;
    private DataSize dictionaryMaxMemory = OrcWriterOptions.DEFAULT_DICTIONARY_MAX_MEMORY;
    private DataSize stringStatisticsLimit = OrcWriterOptions.DEFAULT_MAX_STRING_STATISTICS_LIMIT;
    private DataSize maxCompressionBufferSize = OrcWriterOptions.DEFAULT_MAX_COMPRESSION_BUFFER_SIZE;
    private StreamLayoutType streamLayoutType = BY_STREAM_SIZE;
    private boolean isDwrfStripeCacheEnabled;
    private DataSize dwrfStripeCacheMaxSize = OrcWriterOptions.DEFAULT_DWRF_STRIPE_CACHE_MAX_SIZE;
    private DwrfStripeCacheMode dwrfStripeCacheMode = OrcWriterOptions.DEFAULT_DWRF_STRIPE_CACHE_MODE;

    public OrcWriterOptions.Builder toOrcWriterOptionsBuilder()
    {
        // Give separate copy to callers for isolation.
        return OrcWriterOptions.builder()
                .withStripeMinSize(stripeMinSize)
                .withStripeMaxSize(stripeMaxSize)
                .withStripeMaxRowCount(stripeMaxRowCount)
                .withRowGroupMaxRowCount(rowGroupMaxRowCount)
                .withDictionaryMaxMemory(dictionaryMaxMemory)
                .withMaxStringStatisticsLimit(stringStatisticsLimit)
                .withMaxCompressionBufferSize(maxCompressionBufferSize)
                .withStreamLayout(getStreamLayout(streamLayoutType))
                .withDwrfStripeCacheEnabled(isDwrfStripeCacheEnabled)
                .withDwrfStripeCacheMaxSize(dwrfStripeCacheMaxSize)
                .withDwrfStripeCacheMode(dwrfStripeCacheMode);
    }

    @NotNull
    public DataSize getStripeMinSize()
    {
        return stripeMinSize;
    }

    @Config("hive.orc.writer.stripe-min-size")
    public OrcFileWriterConfig setStripeMinSize(DataSize stripeMinSize)
    {
        this.stripeMinSize = stripeMinSize;
        return this;
    }

    @NotNull
    public DataSize getStripeMaxSize()
    {
        return this.stripeMaxSize;
    }

    @Config("hive.orc.writer.stripe-max-size")
    public OrcFileWriterConfig setStripeMaxSize(DataSize stripeMaxSize)
    {
        this.stripeMaxSize = stripeMaxSize;
        return this;
    }

    public int getStripeMaxRowCount()
    {
        return stripeMaxRowCount;
    }

    @Config("hive.orc.writer.stripe-max-rows")
    public OrcFileWriterConfig setStripeMaxRowCount(int stripeMaxRowCount)
    {
        this.stripeMaxRowCount = stripeMaxRowCount;
        return this;
    }

    public int getRowGroupMaxRowCount()
    {
        return rowGroupMaxRowCount;
    }

    @Config("hive.orc.writer.row-group-max-rows")
    public OrcFileWriterConfig setRowGroupMaxRowCount(int rowGroupMaxRowCount)
    {
        this.rowGroupMaxRowCount = rowGroupMaxRowCount;
        return this;
    }

    @NotNull
    public DataSize getDictionaryMaxMemory()
    {
        return dictionaryMaxMemory;
    }

    @Config("hive.orc.writer.dictionary-max-memory")
    public OrcFileWriterConfig setDictionaryMaxMemory(DataSize dictionaryMaxMemory)
    {
        this.dictionaryMaxMemory = dictionaryMaxMemory;
        return this;
    }

    @NotNull
    public DataSize getStringStatisticsLimit()
    {
        return stringStatisticsLimit;
    }

    @Config("hive.orc.writer.string-statistics-limit")
    public OrcFileWriterConfig setStringStatisticsLimit(DataSize stringStatisticsLimit)
    {
        this.stringStatisticsLimit = stringStatisticsLimit;
        return this;
    }

    @NotNull
    public DataSize getMaxCompressionBufferSize()
    {
        return maxCompressionBufferSize;
    }

    @Config("hive.orc.writer.max-compression-buffer-size")
    public OrcFileWriterConfig setMaxCompressionBufferSize(DataSize maxCompressionBufferSize)
    {
        this.maxCompressionBufferSize = maxCompressionBufferSize;
        return this;
    }

    @NotNull
    public StreamLayoutType getStreamLayoutType()
    {
        return streamLayoutType;
    }

    @Config("hive.orc.writer.stream-layout-type")
    public OrcFileWriterConfig setStreamLayoutType(StreamLayoutType streamLayoutType)
    {
        this.streamLayoutType = streamLayoutType;
        return this;
    }

    public boolean isDwrfStripeCacheEnabled()
    {
        return isDwrfStripeCacheEnabled;
    }

    @Config("hive.orc.writer.dwrf-stripe-cache-enabled")
    public OrcFileWriterConfig setDwrfStripeCacheEnabled(boolean isDwrfStripeCacheEnabled)
    {
        this.isDwrfStripeCacheEnabled = isDwrfStripeCacheEnabled;
        return this;
    }

    @NotNull
    public DataSize getDwrfStripeCacheMaxSize()
    {
        return dwrfStripeCacheMaxSize;
    }

    @Config("hive.orc.writer.dwrf-stripe-cache-max-size")
    public OrcFileWriterConfig setDwrfStripeCacheMaxSize(DataSize dwrfStripeCacheMaxSize)
    {
        this.dwrfStripeCacheMaxSize = dwrfStripeCacheMaxSize;
        return this;
    }

    @NotNull
    public DwrfStripeCacheMode getDwrfStripeCacheMode()
    {
        return dwrfStripeCacheMode;
    }

    @Config("hive.orc.writer.dwrf-stripe-cache-mode")
    public OrcFileWriterConfig setDwrfStripeCacheMode(DwrfStripeCacheMode dwrfStripeCacheMode)
    {
        this.dwrfStripeCacheMode = dwrfStripeCacheMode;
        return this;
    }

    private static StreamLayout getStreamLayout(StreamLayoutType type)
    {
        switch (type) {
            case BY_COLUMN_SIZE:
                return new StreamLayout.ByColumnSize();
            case BY_STREAM_SIZE:
                return new StreamLayout.ByStreamSize();
            default:
                throw new RuntimeException("Unrecognized type " + type);
        }
    }
}
