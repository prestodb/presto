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
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.NestedField;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.ColumnarRow;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.getColumnType;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static com.facebook.presto.spi.block.ColumnarRow.toColumnarRow;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.io.ColumnIOConverter.constructField;

public class ParquetPageSource
        implements ConnectorPageSource
{
    private static final int MAX_VECTOR_LENGTH = 1024;

    private final ParquetReader parquetReader;
    private final MessageType fileSchema;
    // for debugging heap dump
    private final List<String> columnNames;
    private final List<Type> types;
    private final List<Optional<Field>> fields;
    private final List<Optional<NestedField>> nestedFields;

    private final Block[] constantBlocks;
    private final int[] hiveColumnIndexes;

    private int batchId;
    private boolean closed;
    private final boolean useParquetColumnNames;

    public ParquetPageSource(
            ParquetReader parquetReader,
            MessageType fileSchema,
            MessageColumnIO messageColumnIO,
            TypeManager typeManager,
            Properties splitSchema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            boolean useParquetColumnNames)
    {
        requireNonNull(splitSchema, "splitSchema is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.fileSchema = requireNonNull(fileSchema, "fileSchema is null");
        this.useParquetColumnNames = useParquetColumnNames;

        int size = columns.size();
        this.constantBlocks = new Block[size];
        this.hiveColumnIndexes = new int[size];
        this.nestedFields = new ArrayList<>(size);

        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            checkState(column.getColumnType() == REGULAR, "column type must be regular");

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());

            namesBuilder.add(name);
            typesBuilder.add(type);
            nestedFields.add(column.getNestedField());
            hiveColumnIndexes[columnIndex] = column.getHiveColumnIndex();

            if (getColumnType(column, fileSchema, useParquetColumnNames) == null) {
                constantBlocks[columnIndex] = RunLengthEncodedBlock.create(type, null, MAX_VECTOR_LENGTH);
                fieldsBuilder.add(Optional.empty());
            }
            else {
                if (column.getNestedField().isPresent()) {
                    NestedField nestedField = column.getNestedField().get();
                    fieldsBuilder.add(constructField(getNestedType(nestedField, type), lookupColumnByName(messageColumnIO, nestedField.getBase())));
                }
                else {
                    String columnName = useParquetColumnNames ? name : fileSchema.getFields().get(column.getHiveColumnIndex()).getName();
                    fieldsBuilder.add(constructField(type, lookupColumnByName(messageColumnIO, columnName)));
                }
            }
        }
        types = typesBuilder.build();
        fields = fieldsBuilder.build();
        columnNames = namesBuilder.build();
    }

    @Override
    public long getCompletedBytes()
    {
        return parquetReader.getDataSource().getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return parquetReader.getDataSource().getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return parquetReader.getSystemMemoryContext().getBytes();
    }

    @Override
    public Page getNextPage()
    {
        try {
            batchId++;
            int batchSize = parquetReader.nextBatch();

            if (closed || batchSize <= 0) {
                close();
                return null;
            }

            Block[] blocks = new Block[hiveColumnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = constantBlocks[fieldId].getRegion(0, batchSize);
                }
                else {
                    Optional<Field> field = fields.get(fieldId);
                    if (field.isPresent()) {
                        if (nestedFields.get(fieldId).isPresent()) {
                            blocks[fieldId] = new LazyBlock(batchSize, new ParquetNestedBlockLoader(field.get(), types.get(fieldId)));
                        }
                        else {
                            blocks[fieldId] = new LazyBlock(batchSize, new ParquetBlockLoader(field.get()));
                        }
                    }
                    else {
                        Type type = types.get(fieldId);
                        blocks[fieldId] = RunLengthEncodedBlock.create(type, null, batchSize);
                    }
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    private void closeWithSuppression(Throwable throwable)
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
            throw new UncheckedIOException(e);
        }
    }

    private final class ParquetBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final Field field;
        private boolean loaded;

        public ParquetBlockLoader(Field field)
        {
            this.field = requireNonNull(field, "field is null");
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            checkState(batchId == expectedBatchId);

            try {
                Block block = parquetReader.readBlock(field);
                lazyBlock.setBlock(block);
            }
            catch (ParquetCorruptionException e) {
                throw new PrestoException(HIVE_BAD_DATA, e);
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_CURSOR_ERROR, e);
            }
            loaded = true;
        }
    }

    private final class ParquetNestedBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final Field field;
        private final Type leafType;
        private boolean loaded;

        public ParquetNestedBlockLoader(Field field, Type leafType)
        {
            this.field = requireNonNull(field, "field is null");
            this.leafType = requireNonNull(leafType, "leafType is null");
        }

        private int getDepth(Type rootType, Type leafType)
        {
            int depth = 0;
            Type type = rootType;
            while (!type.equals(leafType)) {
                type = type.getTypeParameters().get(0);
                ++depth;
            }
            return depth;
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            checkState(batchId == expectedBatchId);

            try {
                Block block = parquetReader.readBlock(field);

                int size = block.getPositionCount();
                boolean[] isNulls = new boolean[size];

                for (int level = 0; level < getDepth(field.getType(), leafType); ++level) {
                    ColumnarRow rowBlock = toColumnarRow(block);
                    int index = 0;
                    for (int j = 0; j < size; ++j) {
                        if (!isNulls[j]) {
                            isNulls[j] = rowBlock.isNull(index);
                            ++index;
                        }
                    }
                    block = rowBlock.getField(0);
                }

                BlockBuilder blockBuilder = leafType.createBlockBuilder(null, size);
                int position = 0;
                for (int i = 0; i < size; ++i) {
                    if (isNulls[i]) {
                        blockBuilder.appendNull();
                    }
                    else {
                        checkArgument(position < block.getPositionCount(), "current position cannot exceed total position count");
                        leafType.appendTo(block, position, blockBuilder);
                        position++;
                    }
                }
                lazyBlock.setBlock(blockBuilder.build());
            }
            catch (ParquetCorruptionException e) {
                throw new PrestoException(HIVE_BAD_DATA, e);
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_CURSOR_ERROR, e);
            }
            loaded = true;
        }
    }

    private Type getNestedType(NestedField nestedField, Type leafType)
    {
        Type type = leafType;
        List<String> names = nestedField.getRemaining();
        for (int i = names.size() - 1; i >= 0; --i) {
            type = RowType.from(ImmutableList.of(RowType.field(names.get(i), type)));
        }
        return type;
    }
}
