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
package io.prestosql.plugin.hive;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.prestosql.orc.OrcWriterOptions;

@SuppressWarnings("unused")
public class OrcFileWriterConfig
{
    private OrcWriterOptions options = new OrcWriterOptions();

    public OrcWriterOptions toOrcWriterOptions()
    {
        return options;
    }

    public DataSize getStripeMinSize()
    {
        return options.getStripeMinSize();
    }

    @Config("hive.orc.writer.stripe-min-size")
    public OrcFileWriterConfig setStripeMinSize(DataSize stripeMinSize)
    {
        options = options.withStripeMinSize(stripeMinSize);
        return this;
    }

    public DataSize getStripeMaxSize()
    {
        return options.getStripeMaxSize();
    }

    @Config("hive.orc.writer.stripe-max-size")
    public OrcFileWriterConfig setStripeMaxSize(DataSize stripeMaxSize)
    {
        options = options.withStripeMaxSize(stripeMaxSize);
        return this;
    }

    public int getStripeMaxRowCount()
    {
        return options.getStripeMaxRowCount();
    }

    @Config("hive.orc.writer.stripe-max-rows")
    public OrcFileWriterConfig setStripeMaxRowCount(int stripeMaxRowCount)
    {
        options = options.withStripeMaxRowCount(stripeMaxRowCount);
        return this;
    }

    public int getRowGroupMaxRowCount()
    {
        return options.getRowGroupMaxRowCount();
    }

    @Config("hive.orc.writer.row-group-max-rows")
    public OrcFileWriterConfig setRowGroupMaxRowCount(int rowGroupMaxRowCount)
    {
        options = options.withRowGroupMaxRowCount(rowGroupMaxRowCount);
        return this;
    }

    public DataSize getDictionaryMaxMemory()
    {
        return options.getDictionaryMaxMemory();
    }

    @Config("hive.orc.writer.dictionary-max-memory")
    public OrcFileWriterConfig setDictionaryMaxMemory(DataSize dictionaryMaxMemory)
    {
        options = options.withDictionaryMaxMemory(dictionaryMaxMemory);
        return this;
    }

    public DataSize getStringStatisticsLimit()
    {
        return options.getMaxStringStatisticsLimit();
    }

    @Config("hive.orc.writer.string-statistics-limit")
    public OrcFileWriterConfig setStringStatisticsLimit(DataSize stringStatisticsLimit)
    {
        options = options.withMaxStringStatisticsLimit(stringStatisticsLimit);
        return this;
    }

    public DataSize getMaxCompressionBufferSize()
    {
        return options.getMaxCompressionBufferSize();
    }

    @Config("hive.orc.writer.max-compression-buffer-size")
    public OrcFileWriterConfig setMaxCompressionBufferSize(DataSize maxCompressionBufferSize)
    {
        options = options.withMaxCompressionBufferSize(maxCompressionBufferSize);
        return this;
    }
}
