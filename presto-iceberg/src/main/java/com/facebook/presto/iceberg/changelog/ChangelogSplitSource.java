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
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergSplit;
import com.facebook.presto.iceberg.IcebergTableProperties;
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
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getNodeSelectionStrategy;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionKeys;
import static com.google.common.collect.Iterators.limit;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ChangelogSplitSource
        implements ConnectorSplitSource
{
    private CloseableIterator<ChangelogScanTask> fileScanTaskIterator;

    private final Table table;
    private final IncrementalChangelogScan tableScan;
    private final Closer closer = Closer.create();
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
        this.columnHandles = getColumns(table.schema(), typeManager);
        this.table = requireNonNull(table, "table is null");
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        this.fileScanTaskIterator = tableScan.planFiles().iterator();
        closer.register(fileScanTaskIterator);
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
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
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
        String primaryKeyColumnName = IcebergTableProperties.getSampleTablePrimaryKey((Map) table.properties());
        return new IcebergSplit(
                task.file().path().toString(),
                task.start(),
                task.length(),
                task.file().format(),
                ImmutableList.of(),
                getPartitionKeys(task),
                getNodeSelectionStrategy(session),
                SplitWeight.fromProportion(Math.min(Math.max((double) task.length() / tableScan.targetSplitSize(), minimumAssignedSplitWeight), 1.0)),
                Collections.emptyList(),
                Optional.of(new ChangelogSplitInfo(changeTask.operation(),
                        changeTask.changeOrdinal(),
                        changeTask.commitSnapshotId(),
                        primaryKeyColumnName,
                        columnHandles.stream().filter(x -> x.getName().equalsIgnoreCase(primaryKeyColumnName)).collect(Collectors.toList()))));
    }
}
