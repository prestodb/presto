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
package com.facebook.presto.parquet.selectivereader;

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.reader.PageReader;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import javax.annotation.Nullable;

import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.parquet.selectivereader.AbstractSelectiveColumnReader.ExecutionMode.ADAPTIVE;
import static java.util.Objects.requireNonNull;

public abstract class AbstractSelectiveColumnReader
        implements SelectiveColumnReader
{
    protected final ParquetDataSource dataSource;
    protected final ColumnDescriptor columnDescriptor;
    protected final boolean outputRequired;

    protected final LocalMemoryContext systemMemoryContext;
    @Nullable
    protected Field field;
    protected ColumnChunkMetaData metadata;
    protected PageReader pageReader;

    protected boolean rowgroupOpen;
    protected byte[] buffer;

    @Nullable
    protected int[] outputPositions;
    protected int outputPositionCount;
    protected boolean hasNulls;
    protected boolean allNulls;
    protected boolean valuesInUse;
    // TODO: introduce session property to choose mode for performance testing purpose.
    protected ExecutionMode executionMode;

    protected AbstractSelectiveColumnReader(
            ParquetDataSource dataSource,
            ColumnDescriptor columnDescriptor,
            @Nullable Field field,
            boolean outputRequired,
            LocalMemoryContext systemMemoryContext)
    {
        this.dataSource = requireNonNull(dataSource);
        this.columnDescriptor = requireNonNull(columnDescriptor);
        this.field = field;
        this.outputRequired = outputRequired;
        this.systemMemoryContext = requireNonNull(systemMemoryContext);
        this.allNulls = (field == null);
        this.executionMode = ADAPTIVE;
    }

    public void setColumnChunkMetadata(ColumnChunkMetaData metadata)
    {
        this.metadata = requireNonNull(metadata);
    }

    public void setExecutionMode(ExecutionMode executionMode)
    {
        this.executionMode = executionMode;
    }

    public void initializeOutputPositions(int[] positions, int positionCount)
    {
        outputPositions = ensureCapacity(outputPositions, positionCount);
        System.arraycopy(positions, 0, outputPositions, 0, positionCount);
    }

    public enum ExecutionMode
    {
        ADAPTIVE,
        BATCH,
        SKIP
    }
}
