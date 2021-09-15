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
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LazyBlock;
import com.facebook.presto.common.block.LazyBlockLoader;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

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

    private final RuntimeStats runtimeStats;

    public ParquetPageSource(
            ParquetReader parquetReader,
            List<Type> types,
            List<Optional<Field>> fields,
            List<String> columnNames,
            RuntimeStats runtimeStats)
    {
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
        this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
    }

    @Override
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
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
