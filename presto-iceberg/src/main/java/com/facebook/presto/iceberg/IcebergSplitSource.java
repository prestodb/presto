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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.hive.HiveCommonSessionProperties.getAffinitySchedulingFileSectionSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getNodeSelectionStrategy;
import static com.facebook.presto.iceberg.FileFormat.fromIcebergFileFormat;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getMinimumAssignedSplitWeight;
import static com.facebook.presto.iceberg.IcebergUtil.buildLastUpdatedSequenceNumberEvaluator;
import static com.facebook.presto.iceberg.IcebergUtil.getDataSequenceNumber;
import static com.facebook.presto.iceberg.IcebergUtil.getFirstRowId;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionKeys;
import static com.facebook.presto.iceberg.IcebergUtil.getTargetSplitSize;
import static com.facebook.presto.iceberg.IcebergUtil.metadataColumnsMatchPredicates;
import static com.facebook.presto.iceberg.IcebergUtil.partitionDataFromStructLike;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.limit;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.iceberg.util.TableScanUtil.splitFiles;

public class IcebergSplitSource
        implements ConnectorSplitSource
{
    private CloseableIterator<FileScanTask> fileScanTaskIterator;

    private final Closer closer = Closer.create();
    private final double minimumAssignedSplitWeight;
    private final long targetSplitSize;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final long affinitySchedulingFileSectionSize;

    private final TupleDomain<IcebergColumnHandle> metadataColumnConstraints;
    private final InclusiveMetricsEvaluator lineageEvaluator;
    // Preferred Presto FileFormat the table is configured to write
    // (`write.format.default`), or null when the property is absent /
    // unrecognized. Used to disambiguate the iceberg-api wire format on
    // splits whose `task.file().format()` is ORC — because iceberg-api has no
    // NIMBLE / DWRF enum values, both NIMBLE and DWRF data files appear on
    // the manifest as `Iceberg.ORC` and the worker can't tell them apart
    // without help. See `toIcebergSplit()` for the override logic.
    private final FileFormat tableWriteFormat;

    public IcebergSplitSource(
            ConnectorSession session,
            TableScan tableScan,
            TupleDomain<IcebergColumnHandle> metadataColumnConstraints)
    {
        this(session, getTargetSplitSize(session, tableScan).toBytes(), tableScan.planFiles(), metadataColumnConstraints, parseTableWriteFormat(tableScan));
    }

    public IcebergSplitSource(
            ConnectorSession session,
            long targetSplitSize,
            CloseableIterable<FileScanTask> fileScanTasks,
            TupleDomain<IcebergColumnHandle> metadataColumnConstraints)
    {
        this(session, targetSplitSize, fileScanTasks, metadataColumnConstraints, null);
    }

    private IcebergSplitSource(
            ConnectorSession session,
            long targetSplitSize,
            CloseableIterable<FileScanTask> fileScanTasks,
            TupleDomain<IcebergColumnHandle> metadataColumnConstraints,
            FileFormat tableWriteFormat)
    {
        requireNonNull(session, "session is null");
        this.metadataColumnConstraints = requireNonNull(metadataColumnConstraints, "metadataColumnConstraints is null");
        this.lineageEvaluator = buildLastUpdatedSequenceNumberEvaluator(metadataColumnConstraints);
        this.targetSplitSize = targetSplitSize;
        this.minimumAssignedSplitWeight = getMinimumAssignedSplitWeight(session);
        this.nodeSelectionStrategy = getNodeSelectionStrategy(session);
        this.affinitySchedulingFileSectionSize = getAffinitySchedulingFileSectionSize(session).toBytes();
        this.tableWriteFormat = tableWriteFormat;
        this.fileScanTaskIterator = closer.register(
                splitFiles(
                        closer.register(fileScanTasks),
                        targetSplitSize)
                        .iterator());
    }

    // Reads the table's `write.format.default` property (e.g. "NIMBLE",
    // "DWRF", "PARQUET") and returns the matching Presto FileFormat, or
    // null on absent / unrecognized values. Called once per split source
    // construction so we don't re-read properties per file.
    private static FileFormat parseTableWriteFormat(TableScan tableScan)
    {
        try {
            String prop = tableScan.table().properties().get(TableProperties.DEFAULT_FILE_FORMAT);
            if (prop == null || prop.isEmpty()) {
                return null;
            }
            return FileFormat.valueOf(prop.toUpperCase(java.util.Locale.ROOT));
        }
        catch (RuntimeException ignored) {
            // Unknown enum value, missing table property, or transient
            // metadata access error — fall back to the iceberg-api format.
            return null;
        }
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        // TODO: move this to a background thread
        List<ConnectorSplit> splits = new ArrayList<>();
        Iterator<FileScanTask> iterator = limit(fileScanTaskIterator, maxSize);
        while (iterator.hasNext()) {
            FileScanTask task = iterator.next();
            IcebergSplit icebergSplit = (IcebergSplit) toIcebergSplit(task);
            if (metadataColumnsMatchPredicates(
                    metadataColumnConstraints,
                    icebergSplit.getPath(),
                    icebergSplit.getDataSequenceNumber(),
                    task.file(),
                    lineageEvaluator)) {
                splits.add(icebergSplit);
            }
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
        PartitionSpec spec = task.spec();
        Optional<PartitionData> partitionData = partitionDataFromStructLike(spec, task.file().partition());

        // TODO: We should leverage residual expression and convert that to TupleDomain.
        //       The predicate here is used by readers for predicate push down at reader level,
        //       so when we do not use residual expression, we are just wasting CPU cycles
        //       on reader side evaluating a condition that we know will always be true.

        // Iceberg-api has no NIMBLE / DWRF enum values; both formats appear
        // on the manifest as `Iceberg.ORC`. When the table's preferred write
        // format is NIMBLE or DWRF, override here so the worker routes to
        // the right reader. True ORC files keep their format-on-wire when
        // the table prop is null / PARQUET / ORC.
        org.apache.iceberg.FileFormat icebergFormat = task.file().format();
        FileFormat splitFileFormat;
        if (icebergFormat == org.apache.iceberg.FileFormat.ORC
                && (tableWriteFormat == FileFormat.NIMBLE
                        || tableWriteFormat == FileFormat.DWRF)) {
            splitFileFormat = tableWriteFormat;
        }
        else {
            splitFileFormat = fromIcebergFileFormat(icebergFormat);
        }

        return new IcebergSplit(
                task.file().path().toString(),
                task.start(),
                task.length(),
                splitFileFormat,
                ImmutableList.of(),
                getPartitionKeys(task),
                PartitionSpecParser.toJson(spec),
                partitionData.map(PartitionData::toJson),
                nodeSelectionStrategy,
                SplitWeight.fromProportion(Math.min(Math.max((double) task.length() / targetSplitSize, minimumAssignedSplitWeight), 1.0)),
                task.deletes().stream().map(DeleteFile::fromIceberg).collect(toImmutableList()),
                Optional.empty(),
                getDataSequenceNumber(task.file()),
                getFirstRowId(task.file()),
                affinitySchedulingFileSectionSize);
    }
}
