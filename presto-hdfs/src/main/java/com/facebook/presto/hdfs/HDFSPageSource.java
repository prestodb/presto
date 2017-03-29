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
package com.facebook.presto.hdfs;

import com.facebook.presto.hdfs.exception.HdfsCursorException;
import com.facebook.presto.hive.parquet.ParquetDataSource;
import com.facebook.presto.hive.parquet.ParquetTypeUtils;
import com.facebook.presto.hive.parquet.RichColumnDescriptor;
import com.facebook.presto.hive.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import parquet.column.ColumnDescriptor;
import parquet.io.InvalidRecordException;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSPageSource
implements ConnectorPageSource
{
    private static final int MAX_VECTOR_LENGTH = 1024;
    private static final long GUESSED_MEMORY_USAGE = new DataSize(16, DataSize.Unit.MEGABYTE).toBytes();

    private final ParquetReader parquetReader;
    private final ParquetDataSource dataSource;
    private final MessageType fileSchema;
    private final MessageType requestedSchema;
    private final long totalBytes;

    private final List<String> columnNames;
    private final List<Type> types;
    private final Block[] constantBlocks;

    private int batchId;
    private final int columnSize;
    private boolean closed;
    private long readTimeNanos;

    public HDFSPageSource(
            ParquetReader parquetReader,
            ParquetDataSource dataSource,
            MessageType fileSchema,
            MessageType requestedSchema,
            long totalBytes,
            List<HDFSColumnHandle> columns,
            TypeManager typeManager)
    {
        checkArgument(totalBytes >= 0, "totalBytes is negative");

        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.fileSchema = requireNonNull(fileSchema, "fileSchema is null");
        this.requestedSchema = requireNonNull(requestedSchema, "requestedSchema is null");
        this.totalBytes = totalBytes;

        this.columnSize = columns.size();
        this.constantBlocks = new Block[columnSize];
        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < columnSize; columnIndex++) {
            HDFSColumnHandle column = columns.get(columnIndex);
            String name = column.getName();
            Type type = typeManager.getType(column.getType().getTypeSignature());

            namesBuilder.add(name);
            typesBuilder.add(type);

            if (getParquetType(column, fileSchema) == null) {
                constantBlocks[columnIndex] = RunLengthEncodedBlock.create(type, null, MAX_VECTOR_LENGTH);
            }
        }
        columnNames = namesBuilder.build();
        types = typesBuilder.build();
    }
    /**
     * Gets the total input bytes that will be processed by this page source.
     * This is normally the same size as the split.  If size is not available,
     * this method should return zero.
     */
    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    /**
     * Gets the number of input bytes processed by this page source so far.
     * If size is not available, this method should return zero.
     */
    @Override
    public long getCompletedBytes()
    {
        return dataSource.getReadBytes();
    }

    /**
     * Gets the wall time this page source spent reading data from the input.
     * If read time is not available, this method should return zero.
     */
    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    /**
     * Will this page source product more pages?
     */
    @Override
    public boolean isFinished()
    {
        return closed;
    }

    /**
     * Gets the next page of data.  This method is allowed to return null.
     */
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

            Block[] blocks = new Block[columnSize];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = constantBlocks[fieldId].getRegion(0, batchSize);
                }
                else {
                    Type type = types.get(fieldId);
                    int fieldIndex = getFieldIndex(fileSchema, columnNames.get(fieldId));
                    if (fieldIndex == -1) {
                        blocks[fieldId] = RunLengthEncodedBlock.create(type, null, batchSize);
                        continue;
                    }

                    List<String> path = new ArrayList<>();
                    path.add(fileSchema.getFields().get(fieldIndex).getName());
                    if (StandardTypes.ROW.equals(type.getTypeSignature().getBase())) {
                        blocks[fieldId] = parquetReader.readStruct(type, path);
                    }
                    else {
                        Optional<RichColumnDescriptor> descriptor = ParquetTypeUtils.getDescriptor(fileSchema, requestedSchema, path);
                        if (descriptor.isPresent()) {
                            blocks[fieldId] = new LazyBlock(batchSize, new ParquetBlockLoader(descriptor.get(), type));
                        }
                        else {
                            blocks[fieldId] = RunLengthEncodedBlock.create(type, null, batchSize);
                        }
                    }
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (IOException e) {
            closeWithSupression(e);
            throw new HdfsCursorException();
        }
    }

    /**
     * Get the total memory that needs to be reserved in the system memory pool.
     * This memory should include any buffers, etc. that are used for reading data.
     *
     * @return the system memory used so far in table read
     */
    @Override
    public long getSystemMemoryUsage()
    {
        return GUESSED_MEMORY_USAGE;
    }

    /**
     * Immediately finishes this page source.  Presto will always call this method.
     */
    @Override
    public void close() throws IOException
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

    private void closeWithSupression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (IOException e) {
            if (e != throwable) {
                throwable.addSuppressed(e);
            }
        }
    }

    private parquet.schema.Type getParquetType(HDFSColumnHandle column, MessageType messageType)
    {
        if (messageType.containsField(column.getName())) {
            return messageType.getType(column.getName());
        }
        // parquet is case-insensitive, all hdfs-columns get converted to lowercase
        for (parquet.schema.Type type : messageType.getFields()) {
            if (type.getName().equalsIgnoreCase(column.getName())) {
                return type;
            }
        }
        return null;
    }

    public int getFieldIndex(MessageType fileSchema, String name)
    {
        try {
            return fileSchema.getFieldIndex(name);
        }
        catch (InvalidRecordException e) {
            for (parquet.schema.Type type : fileSchema.getFields()) {
                if (type.getName().equalsIgnoreCase(name)) {
                    return fileSchema.getFieldIndex(type.getName());
                }
            }
            return -1;
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
                Block block = parquetReader.readPrimitive(columnDescriptor, type);
                lazyBlock.setBlock(block);
            }
            catch (IOException e) {
                throw new HdfsCursorException();
            }
            loaded = true;
        }
    }
}
