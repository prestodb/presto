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
package com.facebook.presto.parquet.writer;

import io.airlift.units.DataSize;
import org.apache.parquet.column.ParquetProperties.WriterVersion;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ParquetWriterOptions
{
    protected static final DataSize DEFAULT_MAX_ROW_GROUP_SIZE = DataSize.valueOf("128MB");
    protected static final DataSize DEFAULT_MAX_PAGE_SIZE = DataSize.valueOf("1MB");
    public static final WriterVersion DEFAULT_WRITER_VERSION = WriterVersion.PARQUET_1_0;

    public static ParquetWriterOptions.Builder builder()
    {
        return new ParquetWriterOptions.Builder();
    }

    private final int maxRowGroupSize;
    private final int maxPageSize;
    private final int maxDictionaryPageSize;
    private final WriterVersion writerVersion;

    private ParquetWriterOptions(DataSize maxRowGroupSize, DataSize maxPageSize, DataSize maxDictionaryPageSize, WriterVersion writerVersion)
    {
        this.maxRowGroupSize = toIntExact(requireNonNull(maxRowGroupSize, "maxRowGroupSize is null").toBytes());
        this.maxPageSize = toIntExact(requireNonNull(maxPageSize, "maxPageSize is null").toBytes());
        this.maxDictionaryPageSize = toIntExact(requireNonNull(maxDictionaryPageSize, "maxDictionaryPageSize is null").toBytes());
        this.writerVersion = requireNonNull(writerVersion, "writerVersion is null");
    }

    public int getMaxRowGroupSize()
    {
        return maxRowGroupSize;
    }

    public int getMaxPageSize()
    {
        return maxPageSize;
    }

    public int getMaxDictionaryPageSize()
    {
        return maxDictionaryPageSize;
    }

    public WriterVersion getWriterVersion()
    {
        return writerVersion;
    }

    public static class Builder
    {
        private DataSize maxBlockSize = DEFAULT_MAX_ROW_GROUP_SIZE;
        private DataSize maxPageSize = DEFAULT_MAX_PAGE_SIZE;
        // By default, we set maxDictionaryPageSize to the same default value as maxPageSize, to keep consistent with parquet-mr.
        private DataSize maxDictionaryPageSize = DEFAULT_MAX_PAGE_SIZE;
        private WriterVersion writerVersion = DEFAULT_WRITER_VERSION;

        public Builder setMaxBlockSize(DataSize maxBlockSize)
        {
            this.maxBlockSize = maxBlockSize;
            return this;
        }

        public Builder setMaxPageSize(DataSize maxPageSize)
        {
            this.maxPageSize = maxPageSize;
            return this;
        }

        public Builder setMaxDictionaryPageSize(DataSize maxDictionaryPageSize)
        {
            this.maxDictionaryPageSize = maxDictionaryPageSize;
            return this;
        }

        public Builder setWriterVersion(WriterVersion writerVersion)
        {
            this.writerVersion = writerVersion;
            return this;
        }

        public ParquetWriterOptions build()
        {
            return new ParquetWriterOptions(maxBlockSize, maxPageSize, maxDictionaryPageSize, writerVersion);
        }
    }
}
