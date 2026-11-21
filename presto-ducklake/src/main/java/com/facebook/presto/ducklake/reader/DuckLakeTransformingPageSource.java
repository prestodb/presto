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
package com.facebook.presto.ducklake.reader;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.ConnectorPageSource;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.UuidType.UUID;
import static java.util.Objects.requireNonNull;

/**
 * Wraps a delegate page source and rewrites specific output channels: a constant value (used for
 * {@code $path}), a row id derived from another channel's row-position values plus the split's
 * {@code rowIdStart} (used for {@code $row_id}), or a nanosecond-to-millisecond conversion (used
 * for a {@code timestamp_ns} column, which the Parquet reader had to read back as raw {@code
 * BIGINT} nanoseconds; see {@link ParquetPageSourceFactory}). Every other channel passes through
 * the delegate's block unchanged. Only built when at least one channel actually needs a rewrite.
 */
class DuckLakeTransformingPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final List<ChannelTransform> transforms;

    DuckLakeTransformingPageSource(ConnectorPageSource delegate, List<ChannelTransform> transforms)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.transforms = ImmutableList.copyOf(requireNonNull(transforms, "transforms is null"));
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        Page page = delegate.getNextPage();
        if (page == null) {
            return null;
        }
        Block[] blocks = new Block[transforms.size()];
        for (int channel = 0; channel < blocks.length; channel++) {
            blocks[channel] = transforms.get(channel).apply(page);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    /**
     * Produces the output block for one channel out of an input page.
     */
    interface ChannelTransform
    {
        Block apply(Page inputPage);

        /** Copies one input channel through unchanged. */
        static ChannelTransform passthrough(int sourceChannel)
        {
            return inputPage -> inputPage.getBlock(sourceChannel);
        }

        /** A constant value (for example the split's file path) repeated for every position. */
        static ChannelTransform constant(Block singleValueBlock)
        {
            return inputPage -> new RunLengthEncodedBlock(singleValueBlock, inputPage.getPositionCount());
        }

        /**
         * {@code rowIdStart + position}, reading the row position from {@code sourceChannel}. All
         * nulls if {@code rowIdStart} is empty (the split does not know its row id offset).
         */
        static ChannelTransform rowId(int sourceChannel, OptionalLong rowIdStart)
        {
            return inputPage -> {
                Block source = inputPage.getBlock(sourceChannel);
                int positionCount = source.getPositionCount();
                BlockBuilder builder = BIGINT.createBlockBuilder(null, positionCount);
                for (int position = 0; position < positionCount; position++) {
                    if (!rowIdStart.isPresent() || source.isNull(position)) {
                        builder.appendNull();
                    }
                    else {
                        BIGINT.writeLong(builder, rowIdStart.getAsLong() + BIGINT.getLong(source, position));
                    }
                }
                return builder.build();
            };
        }

        /** Converts a {@code BIGINT} channel of raw nanoseconds into a {@code TIMESTAMP} of millis. */
        static ChannelTransform nanosToMillis(int sourceChannel)
        {
            return inputPage -> {
                Block source = inputPage.getBlock(sourceChannel);
                int positionCount = source.getPositionCount();
                BlockBuilder builder = TIMESTAMP.createBlockBuilder(null, positionCount);
                for (int position = 0; position < positionCount; position++) {
                    if (source.isNull(position)) {
                        builder.appendNull();
                    }
                    else {
                        long nanos = BIGINT.getLong(source, position);
                        TIMESTAMP.writeLong(builder, Math.floorDiv(nanos, 1_000_000L));
                    }
                }
                return builder.build();
            };
        }

        /**
         * Fixes up the byte order of a top-level {@code UUID} column read off a Parquet file that
         * was not written by Presto. Presto's own Parquet UUID reader ({@code BinaryColumnReader})
         * stores each 8-byte half of the on-disk value exactly as read, which round-trips correctly
         * against Presto's own Parquet writer ({@code UuidValuesWriter}) but not against the
         * canonical big-endian RFC 4122 layout that Parquet's {@code UUID} logical type (and
         * DuckDB, which writes the DuckLake fixture's files) actually uses: {@code UuidType}'s own
         * convention (see its class javadoc) requires each half's bytes reversed. Reversing each
         * half here (independently, without swapping the halves) corrects the value.
         */
        static ChannelTransform fixUuidByteOrder(int sourceChannel)
        {
            return inputPage -> {
                Block source = inputPage.getBlock(sourceChannel);
                int positionCount = source.getPositionCount();
                BlockBuilder builder = UUID.createBlockBuilder(null, positionCount);
                for (int position = 0; position < positionCount; position++) {
                    if (source.isNull(position)) {
                        builder.appendNull();
                    }
                    else {
                        long mostSignificantBits = source.getLong(position, 0);
                        long leastSignificantBits = source.getLong(position, Long.BYTES);
                        builder.writeLong(Long.reverseBytes(mostSignificantBits));
                        builder.writeLong(Long.reverseBytes(leastSignificantBits));
                        builder.closeEntry();
                    }
                }
                return builder.build();
            };
        }
    }
}
