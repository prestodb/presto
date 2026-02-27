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
package com.facebook.presto.lance;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.spi.ConnectorPageSource;
import org.apache.arrow.memory.BufferAllocator;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public abstract class LanceBasePageSource
        implements ConnectorPageSource
{
    protected final LanceTableHandle tableHandle;
    protected final LanceArrowToPageScanner arrowToPageScanner;
    protected final BufferAllocator bufferAllocator;
    protected final PageBuilder pageBuilder;
    protected long readBytes;
    protected boolean finished;

    public LanceBasePageSource(
            LanceTableHandle tableHandle,
            List<LanceColumnHandle> columns,
            ScannerFactory scannerFactory)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(scannerFactory, "scannerFactory is null");

        this.bufferAllocator = LanceNamespaceHolder.getAllocator()
                .newChildAllocator(tableHandle.getTableName(), 0, Long.MAX_VALUE);

        try {
            this.arrowToPageScanner = new LanceArrowToPageScanner(
                    bufferAllocator,
                    columns,
                    scannerFactory);
        }
        catch (RuntimeException e) {
            bufferAllocator.close();
            throw e;
        }

        this.pageBuilder = new PageBuilder(
                columns.stream()
                        .map(LanceColumnHandle::getColumnType)
                        .collect(toImmutableList()));
        this.finished = false;
    }

    @Override
    public long getCompletedBytes()
    {
        return readBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }
        if (!arrowToPageScanner.read()) {
            finished = true;
            return null;
        }
        readBytes += arrowToPageScanner.getLastBatchBytes();
        arrowToPageScanner.convert(pageBuilder);
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            arrowToPageScanner.close();
        }
        finally {
            bufferAllocator.close();
        }
    }
}
