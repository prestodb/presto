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
import io.airlift.units.DataSize;
import org.apache.parquet.hadoop.ParquetWriter;

import static io.airlift.units.DataSize.Unit.BYTE;

public class ParquetFileWriterConfig
{
    private boolean parquetOptimizedWriterEnabled;

    private DataSize blockSize = new DataSize(ParquetWriter.DEFAULT_BLOCK_SIZE, BYTE);
    private DataSize pageSize = new DataSize(ParquetWriter.DEFAULT_PAGE_SIZE, BYTE);

    public DataSize getBlockSize()
    {
        return blockSize;
    }

    @Config("hive.parquet.writer.block-size")
    public ParquetFileWriterConfig setBlockSize(DataSize blockSize)
    {
        this.blockSize = blockSize;
        return this;
    }

    public DataSize getPageSize()
    {
        return pageSize;
    }

    @Config("hive.parquet.writer.page-size")
    public ParquetFileWriterConfig setPageSize(DataSize pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }

    public boolean isParquetOptimizedWriterEnabled()
    {
        return parquetOptimizedWriterEnabled;
    }

    @Config("hive.parquet.optimized-writer.enabled")
    public ParquetFileWriterConfig setParquetOptimizedWriterEnabled(boolean parquetOptimizedWriterEnabled)
    {
        this.parquetOptimizedWriterEnabled = parquetOptimizedWriterEnabled;
        return this;
    }
}
