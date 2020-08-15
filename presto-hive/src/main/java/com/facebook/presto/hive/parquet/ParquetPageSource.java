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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LazyBlock;
import com.facebook.presto.common.block.LazyBlockLoader;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HiveColumnHandle.getPushedDownSubfield;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.getParquetType;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static com.facebook.presto.parquet.ParquetTypeUtils.nestedColumnPath;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.io.ColumnIOConverter.constructField;
import static org.apache.parquet.io.ColumnIOConverter.findNestedColumnIO;

public class ParquetPageSource
        implements ConnectorPageSource
{
    private final ParquetReader parquetReader;
    // for debugging heap dump
    private final List<String> columnNames;
    private final List<Type> types;
    private final List<Optional<Field>> fields;

    private int batchId;
    private long completedPositions;
    private boolean closed;

    public ParquetPageSource(
            ParquetReader parquetReader,
            MessageType fileSchema,
            MessageColumnIO messageColumnIO,
            TypeManager typeManager,
            List<HiveColumnHandle> columns,
            boolean useParquetColumnNames,
            SchemaTableName tableName,
            Path path)
    {
        requireNonNull(columns, "columns is null");
        requireNonNull(fileSchema, "fileSchema is null");
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");

        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
        for (HiveColumnHandle column : columns) {
            checkArgument(column.getColumnType() == REGULAR || column.getColumnType() == SYNTHESIZED, "column type must be regular or synthesized column");

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());

            namesBuilder.add(name);
            typesBuilder.add(type);

            if (column.getColumnType() == SYNTHESIZED) {
                Subfield pushedDownSubfield = getPushedDownSubfield(column);
                List<String> nestedColumnPath = nestedColumnPath(pushedDownSubfield);
                Optional<ColumnIO> columnIO = findNestedColumnIO(lookupColumnByName(messageColumnIO, pushedDownSubfield.getRootName()), nestedColumnPath);
                if (columnIO.isPresent()) {
                    fieldsBuilder.add(constructField(type, columnIO.get()));
                }
                else {
                    fieldsBuilder.add(Optional.empty());
                }
            }
            else if (getParquetType(type, fileSchema, useParquetColumnNames, column, tableName, path).isPresent()) {
                String columnName = useParquetColumnNames ? name : fileSchema.getFields().get(column.getHiveColumnIndex()).getName();
                fieldsBuilder.add(constructField(type, lookupColumnByName(messageColumnIO, columnName)));
            }
            else {
                fieldsBuilder.add(Optional.empty());
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
    public long getCompletedPositions()
    {
        return completedPositions;
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

            completedPositions += batchSize;

            Block[] blocks = new Block[fields.size()];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                Optional<Field> field = fields.get(fieldId);
                if (field.isPresent()) {
                    blocks[fieldId] = new LazyBlock(batchSize, new ParquetBlockLoader(field.get()));
                }
                else {
                    blocks[fieldId] = RunLengthEncodedBlock.create(types.get(fieldId), null, batchSize);
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
}
