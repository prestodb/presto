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
package com.facebook.presto.metadata;

import com.facebook.presto.block.Block;
import com.facebook.presto.operator.Page;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.serde.BlocksFileWriter;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.base.Throwables;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.block.BlockUtils.toTupleIterable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Files.newOutputStreamSupplier;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class ColumnFileHandle
{
    private static final DataSize OUTPUT_BUFFER_SIZE = new DataSize(64, KILOBYTE);

    private final UUID shardUuid;
    private final Map<ColumnHandle, File> files;
    private final Map<ColumnHandle, BlocksFileWriter> writers;

    private final AtomicBoolean committed = new AtomicBoolean();

    public static Builder builder(UUID shardUuid)
    {
        return new Builder(shardUuid);
    }

    private ColumnFileHandle(Builder builder)
    {
        this.shardUuid = builder.getShardUuid();
        this.files = new LinkedHashMap<>(builder.getFiles());
        this.writers = new LinkedHashMap<>(builder.getWriters());
    }

    public int getFieldCount()
    {
        return files.size();
    }

    public Map<ColumnHandle, File> getFiles()
    {
        return files;
    }

    public UUID getShardUuid()
    {
        return shardUuid;
    }

    public int append(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!committed.get(), "already committed: %s", shardUuid);

        Block[] blocks = page.getBlocks();
        int[] tupleCount = new int[blocks.length];

        checkState(blocks.length == writers.size(), "Block count does not match writer count (%s vs %s)!", blocks.length, writers.size());

        int i = 0;
        for (BlocksFileWriter writer : writers.values()) {
            Block block = blocks[i];
            writer.append(toTupleIterable(block));
            tupleCount[i] = block.getPositionCount();
            if (i > 0) {
                checkState(tupleCount[i] == tupleCount[i - 1], "different tuple count (%s vs. %s) for block!", tupleCount[i], tupleCount[i - 1]);
            }
            i++;
        }

        return tupleCount[0]; // they are all the same. And [0] is guaranteed to exist...
    }

    public void commit()
            throws IOException
    {
        Throwable firstThrowable = null;

        checkState(!committed.getAndSet(true), "already committed: %s", shardUuid);

        for (BlocksFileWriter writer : writers.values()) {
            try {
                writer.close();
            }
            catch (Throwable t) {
                if (firstThrowable == null) {
                    firstThrowable = t;
                }
            }
        }

        if (firstThrowable != null) {
            Throwables.propagateIfInstanceOf(firstThrowable, IOException.class);
            throw Throwables.propagate(firstThrowable);
        }
    }

    public static class Builder
    {
        private final UUID shardUuid;
        // both of these Maps are ordered by the column handles. The writer map
        // may contain less writers than files.
        private final Map<ColumnHandle, File> files = new LinkedHashMap<>();
        private final Map<ColumnHandle, BlocksFileWriter> writers = new LinkedHashMap<>();

        public Builder(UUID shardUuid)
        {
            this.shardUuid = checkNotNull(shardUuid, "shardUuid is null");
        }

        /**
         * Register a file as part of the column set with a given encoding.
         */
        public Builder addColumn(ColumnHandle columnHandle, File targetFile, BlocksFileEncoding encoding)
        {
            checkNotNull(columnHandle, "columnHandle is null");
            checkNotNull(targetFile, "targetFile is null");
            checkNotNull(encoding, "encoding is null");

            // This is not a 100% check because it is still possible that some other thread creates the file right between
            // the check and the actual opening of the file for writing (which might or might not be deferred by the buffered
            // output stream. But it works as a reasonable sanity check.
            checkState(!targetFile.exists(), "Can not write to existing file %s", targetFile.getAbsolutePath());

            files.put(columnHandle, targetFile);
            writers.put(columnHandle, new BlocksFileWriter(encoding, new BufferedOutputSupplier(newOutputStreamSupplier(targetFile), OUTPUT_BUFFER_SIZE)));

            return this;
        }

        /**
         * Register a file as part of the column set which does not get written.
         */
        public Builder addColumn(ColumnHandle columnHandle, File targetFile)
        {
            checkNotNull(columnHandle, "columnHandle is null");
            checkNotNull(targetFile, "targetFile is null");

            files.put(columnHandle, targetFile);

            return this;
        }

        public ColumnFileHandle build()
        {
            checkArgument(!files.isEmpty(), "must have at least one column");
            return new ColumnFileHandle(this);
        }

        private UUID getShardUuid()
        {
            return shardUuid;
        }

        private Map<ColumnHandle, File> getFiles()
        {
            return files;
        }

        private Map<ColumnHandle, BlocksFileWriter> getWriters()
        {
            return writers;
        }
    }

    private static class BufferedOutputSupplier
            implements OutputSupplier<OutputStream>
    {
        private final OutputSupplier<? extends OutputStream> supplier;
        private final long bufferSize;

        private BufferedOutputSupplier(OutputSupplier<? extends OutputStream> supplier, DataSize bufferSize)
        {
            this.supplier = checkNotNull(supplier, "supplier is null");
            this.bufferSize = checkNotNull(bufferSize, "bufferSize is null").toBytes();
        }

        @Override
        public OutputStream getOutput()
                throws IOException
        {
            return new BufferedOutputStream(supplier.getOutput(), Ints.saturatedCast(bufferSize));
        }
    }
}
