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
package com.facebook.presto.hive.pagefile;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.hive.pagefile.PageFileWriterFactory.createPagesSerdeForPageFile;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PageFilePageSource
        implements ConnectorPageSource
{
    private final FSDataInputStream inputStream;
    private final Iterator<Page> pageReader;
    private final int[] hiveColumnIndexes;

    private boolean closed;
    private long completedPositions;
    private long completedBytes;
    private long readTimeNanos;
    private long memoryUsageBytes;

    public PageFilePageSource(
            FSDataInputStream inputStream,
            long start,
            long splitLength,
            long fileSize,
            BlockEncodingSerde blockEncodingSerde,
            List<HiveColumnHandle> columns)
            throws IOException
    {
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
        PageFileFooterReader pageFileFooterReader = new PageFileFooterReader(inputStream, fileSize);

        OffsetAndLength readStartAndLength = getReadStartAndLength(
                start,
                splitLength,
                pageFileFooterReader.getFooterOffset(),
                pageFileFooterReader.getStripeOffsets());

        pageReader = new PageFilePageReader(
                readStartAndLength.getOffset(),
                readStartAndLength.getLength(),
                inputStream,
                createPagesSerdeForPageFile(
                        blockEncodingSerde,
                        pageFileFooterReader.getCompression()));

        int size = requireNonNull(columns, "columns is null").size();
        this.hiveColumnIndexes = new int[size];

        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            hiveColumnIndexes[columnIndex] = column.getHiveColumnIndex();
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return closed || !pageReader.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }
        long start = System.nanoTime();

        Page page = pageReader.next();

        Block[] blocks = new Block[hiveColumnIndexes.length];
        for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
            if (hiveColumnIndexes[fieldId] >= page.getChannelCount()) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        "schema evolution is not supported for PageFile format");
            }
            blocks[fieldId] = page.getBlock(hiveColumnIndexes[fieldId]);
        }

        readTimeNanos += System.nanoTime() - start;
        completedPositions += page.getPositionCount();
        long pageSizeInBytes = page.getSizeInBytes();
        completedBytes += pageSizeInBytes;
        memoryUsageBytes = Math.max(memoryUsageBytes, pageSizeInBytes);
        return new Page(page.getPositionCount(), blocks);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return memoryUsageBytes;
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
        closed = true;
    }

    private static OffsetAndLength getReadStartAndLength(
            long splitStart,
            long splitLength,
            long lastStripeEnd,
            List<Long> stripeOffsets)
    {
        checkArgument(stripeOffsets != null, "stripeOffsets is null, failed to read page file footer.");
        if (stripeOffsets.isEmpty()) {
            return new OffsetAndLength(0, 0);
        }

        long readStart = 0;
        long readEnd = 0;
        boolean stripeFound = false;
        for (int i = 0; i < stripeOffsets.size(); ++i) {
            if (splitContainsStripe(splitStart, splitLength, stripeOffsets.get(i))) {
                if (!stripeFound) {
                    readStart = stripeOffsets.get(i);
                    stripeFound = true;
                }
                readEnd = i == stripeOffsets.size() - 1 ? lastStripeEnd : stripeOffsets.get(i + 1);
            }
            else if (stripeFound) {
                break;
            }
        }

        return new OffsetAndLength(readStart, readEnd - readStart);
    }

    private static boolean splitContainsStripe(long splitStart, long splitLength, long stripeOffset)
    {
        return splitStart <= stripeOffset && stripeOffset < splitStart + splitLength;
    }

    private static class OffsetAndLength
    {
        private final long offset;
        private final long length;

        OffsetAndLength(long offset, long length)
        {
            this.offset = offset;
            this.length = length;
        }

        private long getOffset()
        {
            return offset;
        }

        private long getLength()
        {
            return length;
        }
    }
}
