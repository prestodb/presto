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
package com.facebook.presto.hive.rcfile;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.rcfile.RcFileCorruptionException;
import com.facebook.presto.rcfile.RcFileReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class RcFilePageSource
        implements ConnectorPageSource
{
    private static final long GUESSED_MEMORY_USAGE = new DataSize(16, DataSize.Unit.MEGABYTE).toBytes();

    private static final int NULL_ENTRY_SIZE = 0;
    private final RcFileReader rcFileReader;

    private final List<String> columnNames;
    private final List<Type> types;

    private final Block[] constantBlocks;
    private final int[] hiveColumnIndexes;

    private int pageId;

    private boolean closed;

    public RcFilePageSource(
            RcFileReader rcFileReader,
            List<HiveColumnHandle> columns,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager)
    {
        requireNonNull(rcFileReader, "rcReader is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(typeManager, "typeManager is null");

        this.rcFileReader = rcFileReader;

        int size = columns.size();

        this.constantBlocks = new Block[size];
        this.hiveColumnIndexes = new int[size];

        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        ImmutableList.Builder<HiveType> hiveTypesBuilder = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());

            namesBuilder.add(name);
            typesBuilder.add(type);
            hiveTypesBuilder.add(column.getHiveType());

            hiveColumnIndexes[columnIndex] = column.getHiveColumnIndex();

            if (hiveColumnIndexes[columnIndex] >= rcFileReader.getColumnCount()) {
                // this file may contain fewer fields than what's declared in the schema
                // this happens when additional columns are added to the hive table after files have been created
                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1, NULL_ENTRY_SIZE);
                blockBuilder.appendNull();
                constantBlocks[columnIndex] = blockBuilder.build();
            }
        }
        types = typesBuilder.build();
        columnNames = namesBuilder.build();
    }

    @Override
    public long getTotalBytes()
    {
        return rcFileReader.getLength();
    }

    @Override
    public long getCompletedBytes()
    {
        return rcFileReader.getBytesRead();
    }

    @Override
    public long getReadTimeNanos()
    {
        return rcFileReader.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        try {
            // advance in the current batch
            pageId++;

            // if the batch has been consumed, read the next batch
            int currentPageSize = rcFileReader.advance();
            if (currentPageSize < 0) {
                close();
                return null;
            }

            Block[] blocks = new Block[hiveColumnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = new RunLengthEncodedBlock(constantBlocks[fieldId], currentPageSize);
                }
                else {
                    blocks[fieldId] = createBlock(currentPageSize, fieldId);
                }
            }

            Page page = new Page(currentPageSize, blocks);

            return page;
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        rcFileReader.close();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnNames", columnNames)
                .add("types", types)
                .toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return GUESSED_MEMORY_USAGE;
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (Exception e) {
            if (e != throwable) {
                throwable.addSuppressed(e);
            }
        }
    }

    private Block createBlock(int currentPageSize, int fieldId)
    {
        int hiveColumnIndex = hiveColumnIndexes[fieldId];

        return new LazyBlock(
                currentPageSize,
                new RcFileBlockLoader(hiveColumnIndex));
    }

    private final class RcFileBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = pageId;
        private final int columnIndex;
        private boolean loaded;

        public RcFileBlockLoader(int columnIndex)
        {
            this.columnIndex = columnIndex;
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded) {
                return;
            }

            checkState(pageId == expectedBatchId);

            try {
                Block block = rcFileReader.readBlock(columnIndex);
                lazyBlock.setBlock(block);
            }
            catch (IOException e) {
                if (e instanceof RcFileCorruptionException) {
                    throw new PrestoException(HIVE_BAD_DATA, e);
                }
                throw new PrestoException(HIVE_CURSOR_ERROR, e);
            }

            loaded = true;
        }
    }
}
