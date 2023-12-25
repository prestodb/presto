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
package com.facebook.presto.iceberg;

import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.hive.HiveCommonSessionProperties.getNodeSelectionStrategy;
import static com.facebook.presto.iceberg.IcebergUtil.getDataSequenceNumber;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionKeys;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.limit;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class IcebergSplitSource
        implements ConnectorSplitSource
{
    private CloseableIterator<FileScanTask> fileScanTaskIterator;

    private final TableScan tableScan;
    private final Closer closer = Closer.create();
    private final double minimumAssignedSplitWeight;
    private final ConnectorSession session;

    public IcebergSplitSource(
            ConnectorSession session,
            TableScan tableScan,
            CloseableIterable<FileScanTask> fileScanTaskIterable,
            double minimumAssignedSplitWeight)
    {
        this.session = requireNonNull(session, "session is null");
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.fileScanTaskIterator = fileScanTaskIterable.iterator();
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        closer.register(fileScanTaskIterator);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        // TODO: move this to a background thread
        List<ConnectorSplit> splits = new ArrayList<>();
        Iterator<FileScanTask> iterator = limit(fileScanTaskIterator, maxSize);
        while (iterator.hasNext()) {
            FileScanTask task = iterator.next();
            splits.add(toIcebergSplit(task));
        }
        return completedFuture(new ConnectorSplitBatch(splits, isFinished()));
    }

    @Override
    public boolean isFinished()
    {
        return !fileScanTaskIterator.hasNext();
    }

    @Override
    public void close()
    {
        try {
            closer.close();
            // TODO: remove this after org.apache.iceberg.io.CloseableIterator'withClose
            //  correct release resources holds by iterator.
            fileScanTaskIterator = CloseableIterator.empty();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ConnectorSplit toIcebergSplit(FileScanTask task)
    {
        // TODO: We should leverage residual expression and convert that to TupleDomain.
        //       The predicate here is used by readers for predicate push down at reader level,
        //       so when we do not use residual expression, we are just wasting CPU cycles
        //       on reader side evaluating a condition that we know will always be true.

        return new IcebergSplit(
                task.file().path().toString(),
                task.start(),
                task.length(),
                task.file().format(),
                ImmutableList.of(),
                getPartitionKeys(task),
                getNodeSelectionStrategy(session),
                SplitWeight.fromProportion(Math.min(Math.max((double) task.length() / tableScan.targetSplitSize(), minimumAssignedSplitWeight), 1.0)),
                task.deletes().stream().map(DeleteFile::fromIceberg).collect(toImmutableList()),
                Optional.empty(),
                getDataSequenceNumber(task.file()));
    }
}
