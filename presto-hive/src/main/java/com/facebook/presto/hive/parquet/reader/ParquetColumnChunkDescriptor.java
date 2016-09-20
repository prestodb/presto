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
package com.facebook.presto.hive.parquet.reader;

import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;

public class ParquetColumnChunkDescriptor
{
    private final ColumnDescriptor columnDescriptor;
    private final ColumnChunkMetaData columnChunkMetaData;
    private final int size;

    public ParquetColumnChunkDescriptor(
            ColumnDescriptor columnDescriptor,
            ColumnChunkMetaData columnChunkMetaData,
            int size)
    {
        this.columnDescriptor = columnDescriptor;
        this.columnChunkMetaData = columnChunkMetaData;
        this.size = size;
    }

    public int getSize()
    {
        return size;
    }

    public ColumnDescriptor getColumnDescriptor()
    {
        return columnDescriptor;
    }

    public ColumnChunkMetaData getColumnChunkMetaData()
    {
        return columnChunkMetaData;
    }
}
