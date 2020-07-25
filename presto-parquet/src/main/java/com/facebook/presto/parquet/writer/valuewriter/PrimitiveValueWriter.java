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
package com.facebook.presto.parquet.writer.valuewriter;

import com.facebook.presto.common.block.Block;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.schema.PrimitiveType;

import static java.util.Objects.requireNonNull;

public abstract class PrimitiveValueWriter
        extends ValuesWriter
{
    private Statistics<?> statistics;
    private final PrimitiveType parquetType;
    private final ValuesWriter valuesWriter;

    public PrimitiveValueWriter(PrimitiveType parquetType, ValuesWriter valuesWriter)
    {
        this.parquetType = requireNonNull(parquetType, "parquetType is null");
        this.valuesWriter = requireNonNull(valuesWriter, "valuesWriter is null");
        this.statistics = Statistics.createStats(parquetType);
    }

    ValuesWriter getValueWriter()
    {
        return valuesWriter;
    }

    public Statistics<?> getStatistics()
    {
        return statistics;
    }

    @Override
    public long getBufferedSize()
    {
        return valuesWriter.getBufferedSize();
    }

    @Override
    public BytesInput getBytes()
    {
        return valuesWriter.getBytes();
    }

    @Override
    public Encoding getEncoding()
    {
        return valuesWriter.getEncoding();
    }

    @Override
    public void reset()
    {
        valuesWriter.reset();
        this.statistics = Statistics.createStats(parquetType);
    }

    @Override
    public void close()
    {
        valuesWriter.close();
    }

    @Override
    public DictionaryPage toDictPageAndClose()
    {
        return valuesWriter.toDictPageAndClose();
    }

    @Override
    public void resetDictionary()
    {
        valuesWriter.resetDictionary();
    }

    @Override
    public long getAllocatedSize()
    {
        return valuesWriter.getAllocatedSize();
    }

    @Override
    public String memUsageString(String prefix)
    {
        return valuesWriter.memUsageString(prefix);
    }

    public abstract void write(Block block);
}
