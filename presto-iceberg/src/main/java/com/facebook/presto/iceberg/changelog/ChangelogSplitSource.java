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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergSplit;
import com.facebook.presto.iceberg.PartitionData;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.hive.HiveCommonSessionProperties.getNodeSelectionStrategy;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getDataSequenceNumber;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionKeys;
import static com.facebook.presto.iceberg.IcebergUtil.partitionDataFromStructLike;
import static com.facebook.presto.iceberg.changelog.ChangelogOperation.fromIcebergChangelogOperation;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.collect.Iterators.limit;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ChangelogSplitSource
        implements ConnectorSplitSource
{
    private final Closer closer = Closer.create();
    private CloseableIterable<ChangelogScanTask> fileScanTaskIterable;
    private CloseableIterator<ChangelogScanTask> fileScanTaskIterator;
    private final IncrementalChangelogScan tableScan;
    private final double minimumAssignedSplitWeight;
    private final ConnectorSession session;
    private final List<IcebergColumnHandle> columnHandles;

    public ChangelogSplitSource(
            ConnectorSession session,
            TypeManager typeManager,
            Table table,
            IncrementalChangelogScan tableScan,
            double minimumAssignedSplitWeight)
    {
        this.session = requireNonNull(session, "session is null");
        requireNonNull(typeManager, "typeManager is null");
        this.columnHandles = getColumns(table.schema(), table.spec(), typeManager);
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        this.fileScanTaskIterable = closer.register(tableScan.planFiles());
        this.fileScanTaskIterator = closer.register(fileScanTaskIterable.iterator());
    }

    @Override
    public boolean isFinished()
    {
        return !fileScanTaskIterator.hasNext();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        List<ConnectorSplit> splits = new ArrayList<>();
        Iterator<ChangelogScanTask> iterator = limit(fileScanTaskIterator, maxSize);
        while (iterator.hasNext()) {
            ChangelogScanTask task = iterator.next();
            splits.add(toIcebergSplit(task));
        }
        return completedFuture(new ConnectorSplitBatch(splits, isFinished()));
    }

    @Override
    public void close()
    {
        try {
            closer.close();
            // TODO: remove this after org.apache.iceberg.io.CloseableIterator'withClose
            //  correct release resources holds by iterator.
            fileScanTaskIterable = CloseableIterable.empty();
            fileScanTaskIterator = CloseableIterator.empty();
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private ConnectorSplit toIcebergSplit(ChangelogScanTask task)
    {
        if (task instanceof AddedRowsScanTask || task instanceof DeletedRowsScanTask || task instanceof DeletedDataFileScanTask) {
            ContentScanTask<DataFile> scanTask = (ContentScanTask<DataFile>) task;
            return splitFromContentScanTask(scanTask, task);
        }
        else {
            throw new PrestoException(ICEBERG_CANNOT_OPEN_SPLIT, "unsupported task type " + task.getClass().getCanonicalName());
        }
    }

    private IcebergSplit splitFromContentScanTask(ContentScanTask<DataFile> task, ChangelogScanTask changeTask)
    {
        PartitionSpec spec = task.spec();
        Optional<PartitionData> partitionData = partitionDataFromStructLike(spec, task.file().partition());

        return new IcebergSplit(
                task.file().path().toString(),
                task.start(),
                task.length(),
                FileFormat.fromIcebergFileFormat(task.file().format()),
                ImmutableList.of(),
                getPartitionKeys(task),
                PartitionSpecParser.toJson(spec),
                partitionData.map(PartitionData::toJson),
                getNodeSelectionStrategy(session),
                SplitWeight.fromProportion(Math.min(Math.max((double) task.length() / tableScan.targetSplitSize(), minimumAssignedSplitWeight), 1.0)),
                ImmutableList.of(),
                Optional.of(new ChangelogSplitInfo(fromIcebergChangelogOperation(changeTask.operation()),
                        changeTask.changeOrdinal(),
                        changeTask.commitSnapshotId(),
                        columnHandles)),
                getDataSequenceNumber(task.file()));
    }
}
