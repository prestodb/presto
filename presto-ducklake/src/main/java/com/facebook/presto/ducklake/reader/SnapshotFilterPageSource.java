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
import com.facebook.presto.spi.ConnectorPageSource;

import java.io.IOException;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Wraps the page source of a compacted ("partial") data file and drops the rows written by a
 * later snapshot than the one this split is reading at, reading each row's snapshot id off a
 * hidden {@code _ducklake_internal_snapshot_id} channel of the delegate. Mirrors the DuckLake
 * extension's own {@code SetSnapshotFilter}/multi-file-reader behavior: a data file produced by
 * {@code ducklake_merge_adjacent_files} can carry rows from several original insert snapshots
 * (recorded per row via that hidden column), and {@code ducklake_data_file.partial_max} names the
 * greatest such snapshot in the file; a scan at a snapshot below {@code partial_max} must hide the
 * rows whose snapshot id exceeds it, or time travel would see rows that had not been inserted yet.
 * A {@code null} snapshot id (a file whose {@code partial_max} was set but which turns out to
 * carry no per-row snapshot column) is treated as "keep the row": this class only ever removes
 * rows it can positively identify as too new, never on missing information. The snapshot channel
 * is always appended to the delegate's columns solely so this class can filter on it (no query
 * ever requests {@code _ducklake_internal_snapshot_id} directly), so it is stripped from every
 * returned page, unconditionally.
 */
public class SnapshotFilterPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final long snapshotId;
    private final int snapshotChannel;

    public SnapshotFilterPageSource(ConnectorPageSource delegate, long snapshotId, int snapshotChannel)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        checkArgument(snapshotChannel >= 0, "snapshotChannel is negative: %s", snapshotChannel);
        this.snapshotId = snapshotId;
        this.snapshotChannel = snapshotChannel;
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
        return filterNewerRows(page).dropColumn(snapshotChannel);
    }

    private Page filterNewerRows(Page page)
    {
        int positionCount = page.getPositionCount();
        Block snapshotBlock = page.getBlock(snapshotChannel);
        int[] retained = new int[positionCount];
        int retainedCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (snapshotBlock.isNull(position) || BIGINT.getLong(snapshotBlock, position) <= snapshotId) {
                retained[retainedCount] = position;
                retainedCount++;
            }
        }
        return retainedCount == positionCount ? page : page.getPositions(retained, 0, retainedCount);
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
}
