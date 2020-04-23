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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.spi.UpdatablePageSource;
import io.airlift.slice.Slice;

import java.util.BitSet;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class OrcUpdatablePageSource
        implements UpdatablePageSource
{
    private final Optional<ShardRewriter> shardRewriter;
    private final OrcPageSource parentPageSource;
    private final BitSet rowsToDelete;

    public OrcUpdatablePageSource(
            Optional<ShardRewriter> shardRewriter,
            OrcBatchRecordReader recordReader,
            OrcPageSource parentPageSource)
    {
        this.shardRewriter = requireNonNull(shardRewriter, "shardRewriter is null");
        requireNonNull(recordReader, "recordReader is null");
        this.parentPageSource = requireNonNull(parentPageSource, "parentPageSource is null");
        this.rowsToDelete = new BitSet(toIntExact(recordReader.getFileRowCount()));
    }

    @Override
    public long getCompletedBytes()
    {
        return parentPageSource.getCompletedBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return parentPageSource.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return parentPageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return parentPageSource.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return parentPageSource.getNextPage();
    }

    @Override
    public void close()
    {
        parentPageSource.close();
    }

    @Override
    public String toString()
    {
        return parentPageSource.toString();
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        for (int i = 0; i < rowIds.getPositionCount(); i++) {
            long rowId = BIGINT.getLong(rowIds, i);
            rowsToDelete.set(toIntExact(rowId));
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        checkState(shardRewriter.isPresent(), "shardRewriter is missing");
        return shardRewriter.get().rewrite(rowsToDelete);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return parentPageSource.getSystemMemoryUsage();
    }
}
