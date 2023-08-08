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
package com.facebook.presto.iceberg.changelog;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSplit;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * This is a page source which provides pages for the Changelog of an iceberg
 * table.
 * <br>
 * The schema of the changelog table is always fixed to have the following columns:
 *
 * <ul>
 *     <li>operation type (varchar) - a string representation of an enum value from {@link org.apache.iceberg.ChangelogOperation}</li>
 *     <li>ordinal (int) - the order in which the change i applied relative to all other changes in the log</li>
 *     <li>primary key (T) - this column is the type and value of the primary key that identifies the row</li>
 *     <li>snapshot ID (long) - the snapshot id which this change belongs</li>
 *     <li>data (row) - a column containing the data corresponding to this change</li>
 * </ul>
 */
public class ChangelogPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final ConnectorSplit split;
    private final ChangelogSplitInfo changelogSplitInfo;
    private final List<IcebergColumnHandle> desiredColumns;
    private final int primaryKeyIndex;

    public ChangelogPageSource(ConnectorPageSource delegate, ChangelogSplitInfo changelogSplitInfo, ConnectorSplit split, List<IcebergColumnHandle> desiredColumns, List<IcebergColumnHandle> delegateColumns)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.split = requireNonNull(split, "split is null");
        this.changelogSplitInfo = requireNonNull(changelogSplitInfo, "changelogSplitInfo is null");
        this.desiredColumns = requireNonNull(desiredColumns, "columns is null");
        this.primaryKeyIndex = IntStream.range(0, delegateColumns.size())
                .filter(i -> delegateColumns.get(i).getName().equals(changelogSplitInfo.getPrimaryKeyColumnName())).findFirst().getAsInt();
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
        // In order to produce the correct page, we have to build the blocks that
        // make up the schema of the changelog table.
        // First, create blocks matching the selected columns in the page
        // if required, extract the block pertaining to the primary key of the
        // table then, combine all blocks into a single page.
        Page delegatePage = delegate.getNextPage();
        if (delegatePage == null) {
            return null;
        }
        Block[] columns = new Block[desiredColumns.size()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = ChangelogSchemaColumns.valueOf(desiredColumns.get(i).getName().toUpperCase()).getBlock(this, delegatePage);
        }

        return new Page(columns);
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

    public enum ChangelogSchemaColumns
    {
        OPERATION((source, page) -> RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice(source.changelogSplitInfo.getOperation().toString()), page.getPositionCount())),
        ORDINAL((source, page) -> RunLengthEncodedBlock.create(BIGINT, source.changelogSplitInfo.getChangeOrdinal(), page.getPositionCount())),
        SNAPSHOT_ID((source, page) -> RunLengthEncodedBlock.create(BIGINT, source.changelogSplitInfo.getSnapshotId(), page.getPositionCount())),
        PRIMARY_KEY((source, page) -> page.getBlock(source.primaryKeyIndex));

        private final ChangelogUtil.Function2<ChangelogPageSource, Page, Block> blockSupplier;

        ChangelogSchemaColumns(ChangelogUtil.Function2<ChangelogPageSource, Page, Block> blockSupplier)
        {
            this.blockSupplier = blockSupplier;
        }

        public Block getBlock(ChangelogPageSource pageSource, Page page)
        {
            return blockSupplier.apply(pageSource, page);
        }
    }
}
