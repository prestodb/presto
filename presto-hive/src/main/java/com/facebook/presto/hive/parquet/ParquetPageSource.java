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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import parquet.column.ColumnDescriptor;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getParquetType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ParquetPageSource
        implements ConnectorPageSource
{
    public static final int MAX_VECTOR_LENGTH = 1024;
    private static final long GUESSED_MEMORY_USAGE = new DataSize(16, DataSize.Unit.MEGABYTE).toBytes();

    private final ParquetReader parquetReader;
    private final ParquetDataSource dataSource;
    private final MessageType requestedSchema;
    private final MessageType fileSchema;
    // for debugging heap dump
    private final List<String> columnNames;
    private final List<Type> types;

    private final Block[] constantBlocks;
    private final int[] hiveColumnIndexes;

    private final long totalBytes;
    private int batchId;
    private boolean closed;
    private long readTimeNanos;
    private final boolean useParquetColumnNames;

    public ParquetPageSource(
            ParquetReader parquetReader,
            ParquetDataSource dataSource,
            MessageType fileSchema,
            MessageType requestedSchema,
            long totalBytes,
            Properties splitSchema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            TypeManager typeManager,
            boolean useParquetColumnNames)
    {
        checkArgument(totalBytes >= 0, "totalBytes is negative");
        requireNonNull(splitSchema, "splitSchema is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(effectivePredicate, "effectivePredicate is null");

        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.fileSchema = requireNonNull(fileSchema, "fileSchema is null");
        this.requestedSchema = requireNonNull(requestedSchema, "requestedSchema is null");
        this.totalBytes = totalBytes;
        this.useParquetColumnNames = useParquetColumnNames;

        int size = columns.size();
        this.constantBlocks = new Block[size];
        this.hiveColumnIndexes = new int[size];

        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            checkState(column.getColumnType() == REGULAR, "column type must be regular");

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());

            namesBuilder.add(name);
            typesBuilder.add(type);

            hiveColumnIndexes[columnIndex] = column.getHiveColumnIndex();

            if (getParquetType(column, fileSchema, useParquetColumnNames) == null) {
                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), MAX_VECTOR_LENGTH);
                for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                    blockBuilder.appendNull();
                }
                constantBlocks[columnIndex] = blockBuilder.build();
            }
        }
        types = typesBuilder.build();
        columnNames = namesBuilder.build();
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return dataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return GUESSED_MEMORY_USAGE;
    }

    @Override
    public Page getNextPage()
    {
        try {
            batchId++;
            long start = System.nanoTime();

            int batchSize = parquetReader.nextBatch();

            readTimeNanos += System.nanoTime() - start;

            if (closed || batchSize <= 0) {
                close();
                return null;
            }

            Block[] blocks = new Block[hiveColumnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                Type type = types.get(fieldId);
                if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = constantBlocks[fieldId].getRegion(0, batchSize);
                }
                else {
                    int fieldIndex;
                    if (useParquetColumnNames) {
                        fieldIndex = fileSchema.getFieldIndex(columnNames.get(fieldId));
                    }
                    else {
                        fieldIndex = hiveColumnIndexes[fieldId];
                    }

                    // Since we only support primitives in the new reader we just create the path
                    // from the field name and lookup the column descriptor with that path.
                    // With complex type support this lookup logic has to be rewritten.
                    parquet.schema.Type field = fileSchema.getFields().get(fieldIndex);
                    String[] path = new String[] {field.getName()};
                    ColumnDescriptor columnDescriptor = null;
                    for (ColumnDescriptor column : fileSchema.getColumns()) {
                        if (Arrays.equals(column.getPath(), path)) {
                            columnDescriptor = column;
                            break;
                        }
                    }
                    checkState(columnDescriptor != null, "columnDescriptor is null");
                    blocks[fieldId] = new LazyBlock(batchSize, new ParquetBlockLoader(columnDescriptor, type));
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (IOException | RuntimeException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (e != throwable) {
                throwable.addSuppressed(e);
            }
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            parquetReader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private final class ParquetBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final ColumnDescriptor columnDescriptor;
        private final Type type;
        private boolean loaded;

        public ParquetBlockLoader(ColumnDescriptor columnDescriptor, Type type)
        {
            this.columnDescriptor = columnDescriptor;
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            checkState(batchId == expectedBatchId);

            try {
                Block block = parquetReader.readBlock(columnDescriptor, type);
                lazyBlock.setBlock(block);
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_CURSOR_ERROR, e);
            }
            loaded = true;
        }
    }
}
