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
import io.airlift.units.DataSize;

@SuppressWarnings("unused")
public class OrcFileWriterConfig
{
    private DataSize stripeMinSize = OrcWriterOptions.DEFAULT_STRIPE_MIN_SIZE;
    private DataSize stripeMaxSize = OrcWriterOptions.DEFAULT_STRIPE_MAX_SIZE;
    private int stripeMaxRowCount = OrcWriterOptions.DEFAULT_STRIPE_MAX_ROW_COUNT;
    private int rowGroupMaxRowCount = OrcWriterOptions.DEFAULT_ROW_GROUP_MAX_ROW_COUNT;
    private DataSize dictionaryMaxMemory = OrcWriterOptions.DEFAULT_DICTIONARY_MAX_MEMORY;
    private DataSize stringStatisticsLimit = OrcWriterOptions.DEFAULT_MAX_STRING_STATISTICS_LIMIT;
    private DataSize maxCompressionBufferSize = OrcWriterOptions.DEFAULT_MAX_COMPRESSION_BUFFER_SIZE;

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
                .withMaxCompressionBufferSize(maxCompressionBufferSize);
    }

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
}
