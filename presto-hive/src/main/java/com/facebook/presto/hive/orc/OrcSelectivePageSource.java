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
package com.facebook.presto.hive.orc;

import com.facebook.presto.common.InvalidFunctionArgumentException;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.RowIDCoercer;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcSelectiveRecordReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.OptionalInt;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcSelectivePageSource
        implements ConnectorPageSource
{
    private static final int ROW_ID_COLUMN_INDEX = -10;

    private final OrcSelectiveRecordReader recordReader;
    private final OrcDataSource orcDataSource;
    private final OrcAggregatedMemoryContext systemMemoryContext;
    private final FileFormatDataSourceStats stats;
    private final RuntimeStats runtimeStats;
    private final boolean appendRowNumberEnabled;
    private final RowIDCoercer coercer;
    private final boolean supplyRowIDs;
    private final OptionalInt rowIDColumnIndex;
    private boolean closed;

    OrcSelectivePageSource(
            OrcSelectiveRecordReader recordReader,
            OrcDataSource orcDataSource,
            OrcAggregatedMemoryContext systemMemoryContext,
            FileFormatDataSourceStats stats,
            RuntimeStats runtimeStats,
            boolean appendRowNumberEnabled,
            byte[] rowIDPartitionComponent,
            String rowGroupId,
            boolean supplyRowIDs)
    {
        this.recordReader = requireNonNull(recordReader, "recordReader is null");
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.runtimeStats = runtimeStats;
        this.appendRowNumberEnabled = appendRowNumberEnabled;
        this.coercer = new RowIDCoercer(rowIDPartitionComponent, rowGroupId);
        // TODO can we combine supplyRowIDs and rowIDColumnIndex by using
        // rowIDColumnIndex.isPresent() instead of a separate supplyRowIDs argument?
        this.supplyRowIDs = supplyRowIDs;
        this.rowIDColumnIndex = recordReader.toZeroBasedColumnIndex(ROW_ID_COLUMN_INDEX);
    }

    @Override
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return recordReader.getReadPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return orcDataSource.getReadTimeNanos();
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
            Page page;
            if (supplyRowIDs) {
                page = recordReader.getNextPage(true);
                page = fillInRowIDs(page);
            }
            else {
                page = recordReader.getNextPage();
            }

            if (page == null) {
                close();
            }
            return page;
        }
        catch (InvalidFunctionArgumentException e) {
            closeWithSuppression(e);
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (OrcCorruptionException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_BAD_DATA, e);
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, format("Failed to read ORC file: %s", orcDataSource.getId()), e);
        }
    }

    private Page fillInRowIDs(Page page)
    {
        if (page == null) {
            return null;
        }
        // rowNumbers is always the last block in the page
        Block rowNumbers = page.getBlock(page.getChannelCount() - 1);
        Block rowIDs = coercer.apply(rowNumbers);

        // figure out which block is the row ID and replace it
        page = page.replaceColumn(rowIDColumnIndex.getAsInt(), rowIDs);

        if (!this.appendRowNumberEnabled) {
            // remove the row number block now that the row IDs have been constructed unless it was also requested
            page = page.dropColumn(page.getChannelCount() - 1);
        }
        return page;
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        try {
            stats.addMaxCombinedBytesPerRow(recordReader.getMaxCombinedBytesPerRow());
            recordReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryContext.getBytes();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }
}
