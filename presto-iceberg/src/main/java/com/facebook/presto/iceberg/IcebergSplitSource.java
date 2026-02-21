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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.DynamicFilter;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Closer;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_CONSTRAINT_COLUMNS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PUSHED_INTO_SCAN;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_BEFORE_FILTER;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_PROCESSED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_WITHOUT_FILTER;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_WAIT_TIME_NANOS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getAffinitySchedulingFileSectionSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getNodeSelectionStrategy;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.FileFormat.fromIcebergFileFormat;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getMinimumAssignedSplitWeight;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isDynamicFilterExtendedMetrics;
import static com.facebook.presto.iceberg.IcebergUtil.getDataSequenceNumber;
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
    private static final ConnectorSplitBatch EMPTY_BATCH_NOT_FINISHED =
            new ConnectorSplitBatch(ImmutableList.of(), false);

    private CloseableIterator<FileScanTask> fileScanTaskIterator;

    private final Closer closer = Closer.create();
    private final double minimumAssignedSplitWeight;
    private final long targetSplitSize;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final long affinitySchedulingFileSectionSize;

    private final TupleDomain<IcebergColumnHandle> metadataColumnConstraints;

    private final DynamicFilter dynamicFilter;
    private TableScan tableScan;
    private boolean scanInitialized;

    private final RuntimeStats runtimeStats;
    private final boolean dynamicFilterActive;
    private final boolean extendedMetrics;
    private long splitsExamined;
    private boolean filterWaitTimeRecorded;
    private boolean filterWaitStarted;
    private long filterWaitStartNanos;
    private boolean dynamicFilterApplied;
    private boolean closed;

    public IcebergSplitSource(
            ConnectorSession session,
            TableScan tableScan,
            TupleDomain<IcebergColumnHandle> metadataColumnConstraints,
            DynamicFilter dynamicFilter)
    {
        requireNonNull(session, "session is null");
        this.metadataColumnConstraints = requireNonNull(metadataColumnConstraints, "metadataColumnConstraints is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.targetSplitSize = getTargetSplitSize(session, tableScan).toBytes();
        this.minimumAssignedSplitWeight = getMinimumAssignedSplitWeight(session);
        this.nodeSelectionStrategy = getNodeSelectionStrategy(session);
        this.affinitySchedulingFileSectionSize = getAffinitySchedulingFileSectionSize(session).toBytes();

        if (dynamicFilter.isComplete()) {
            dynamicFilterApplied = true;
            initializeScan();
        }

        this.runtimeStats = session.getRuntimeStats();
        this.dynamicFilterActive = dynamicFilter.getWaitTimeout().toMillis() > 0;
        this.extendedMetrics = isDynamicFilterExtendedMetrics(session);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        if (!scanInitialized) {
            if (dynamicFilter.isComplete()) {
                recordFilterWaitTime();
                dynamicFilterApplied = true;
                initializeScanWithDynamicFilter(dynamicFilter.getCurrentPredicate());
                return completedFuture(enumerateSplitBatch(maxSize));
            }

            if (!filterWaitStarted) {
                filterWaitStartNanos = System.nanoTime();
                filterWaitStarted = true;
            }

            CompletableFuture<?> blocked = dynamicFilter.isBlocked();
            if (!blocked.isDone()) {
                return blocked.thenApply(v -> EMPTY_BATCH_NOT_FINISHED);
            }

            recordFilterWaitTime();
            dynamicFilterApplied = dynamicFilter.isComplete();
            initializeScanWithDynamicFilter(dynamicFilter.getCurrentPredicate());
        }

        return completedFuture(enumerateSplitBatch(maxSize));
    }

    private void recordFilterWaitTime()
    {
        if (!filterWaitTimeRecorded && filterWaitStarted) {
            long filterWaitNanos = System.nanoTime() - filterWaitStartNanos;
            runtimeStats.addMetricValue(DYNAMIC_FILTER_WAIT_TIME_NANOS, NANO, filterWaitNanos);
            filterWaitTimeRecorded = true;
        }
    }

    private void initializeScan()
    {
        this.fileScanTaskIterator = closer.register(
                splitFiles(
                        closer.register(tableScan.planFiles()),
                        targetSplitSize)
                        .iterator());
        this.scanInitialized = true;
    }

    private void initializeScanWithDynamicFilter(TupleDomain<ColumnHandle> dynamicFilterConstraint)
    {
        if (!dynamicFilterConstraint.isAll()) {
            if (extendedMetrics && dynamicFilterConstraint.isNone()) {
                runtimeStats.addMetricValue("dynamicFilterConstraintIsNone", NONE, 1);
            }

            TupleDomain<IcebergColumnHandle> icebergConstraint = dynamicFilterConstraint
                    .transform(columnHandle -> (IcebergColumnHandle) columnHandle);

            if (extendedMetrics) {
                if (icebergConstraint.isNone()) {
                    runtimeStats.addMetricValue("dynamicFilterIcebergConstraintIsNone", NONE, 1);
                }
                if (icebergConstraint.isAll()) {
                    runtimeStats.addMetricValue("dynamicFilterIcebergConstraintIsAll", NONE, 1);
                }
                recordDomainDetails(icebergConstraint);
            }

            Expression dfExpression = toIcebergExpression(icebergConstraint);

            if (extendedMetrics) {
                // Check if expression collapsed to always-false or always-true
                runtimeStats.addMetricValue("dynamicFilterExpressionOp", NONE, dfExpression.op().ordinal());

                long baselineCount = countPlanFiles(tableScan);
                runtimeStats.addMetricValue(DYNAMIC_FILTER_SPLITS_WITHOUT_FILTER, NONE, baselineCount);

                // Count files after filter separately to isolate Iceberg evaluation
                long postFilterCount = countPlanFiles(tableScan.filter(dfExpression));
                runtimeStats.addMetricValue("dynamicFilterSplitsAfterIcebergFilter", NONE, postFilterCount);
            }
            tableScan = tableScan.filter(dfExpression);
            runtimeStats.addMetricValue(DYNAMIC_FILTER_PUSHED_INTO_SCAN, NONE, 1);
            icebergConstraint.getDomains().ifPresent(domains ->
                    runtimeStats.addMetricValue(DYNAMIC_FILTER_CONSTRAINT_COLUMNS, NONE, domains.size()));
        }
        initializeScan();
    }

    private void recordDomainDetails(TupleDomain<IcebergColumnHandle> constraint)
    {
        constraint.getDomains().ifPresent(domains -> {
            for (Map.Entry<IcebergColumnHandle, Domain> entry : domains.entrySet()) {
                String colName = entry.getKey().getName();
                Type prestoType = entry.getKey().getType();
                Domain domain = entry.getValue();

                if (domain.isNone()) {
                    runtimeStats.addMetricValue("dynamicFilterDomainIsNone[" + colName + "]", NONE, 1);
                    continue;
                }
                if (domain.isAll()) {
                    runtimeStats.addMetricValue("dynamicFilterDomainIsAll[" + colName + "]", NONE, 1);
                    continue;
                }

                long rangeCount = domain.getValues().getRanges().getRangeCount();
                runtimeStats.addMetricValue("dynamicFilterConnectorRangeCount[" + colName + "]", NONE, rangeCount);

                // Presto type ordinal helps detect type mismatches
                // (e.g., integer domain pushed to a bigint partition column)
                runtimeStats.addMetricValue("dynamicFilterDomainPrestoTypeId[" + colName + "]", NONE, prestoType.getTypeSignature().hashCode());

                // Min/max of the domain — verifies the actual filter values reaching Iceberg
                if (domain.getValues() instanceof SortedRangeSet) {
                    List<Range> ranges = ((SortedRangeSet) domain.getValues()).getOrderedRanges();
                    if (!ranges.isEmpty()) {
                        Range first = ranges.get(0);
                        Range last = ranges.get(ranges.size() - 1);
                        if (first.getLow().getValueBlock().isPresent() && first.getLow().getValue() instanceof Number) {
                            runtimeStats.addMetricValue("dynamicFilterDomainMin[" + colName + "]", NONE,
                                    ((Number) first.getLow().getValue()).longValue());
                        }
                        if (last.getHigh().getValueBlock().isPresent() && last.getHigh().getValue() instanceof Number) {
                            runtimeStats.addMetricValue("dynamicFilterDomainMax[" + colName + "]", NONE,
                                    ((Number) last.getHigh().getValue()).longValue());
                        }
                    }
                }
            }
        });
    }

    private long countPlanFiles(TableScan scan)
    {
        try (CloseableIterable<FileScanTask> iterable = splitFiles(scan.planFiles(), targetSplitSize)) {
            return Iterables.size(iterable);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ConnectorSplitBatch enumerateSplitBatch(int maxSize)
    {
        List<ConnectorSplit> splits = new ArrayList<>();
        Iterator<FileScanTask> iterator = limit(fileScanTaskIterator, maxSize);
        while (iterator.hasNext()) {
            FileScanTask task = iterator.next();
            splitsExamined++;

            IcebergSplit icebergSplit = (IcebergSplit) toIcebergSplit(task);
            if (metadataColumnsMatchPredicates(metadataColumnConstraints, icebergSplit.getPath(), icebergSplit.getDataSequenceNumber())) {
                splits.add(icebergSplit);
            }
        }
        return new ConnectorSplitBatch(splits, isFinished());
    }

    @Override
    public boolean isFinished()
    {
        return scanInitialized && !fileScanTaskIterator.hasNext();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        if (dynamicFilterActive) {
            runtimeStats.addMetricValue(DYNAMIC_FILTER_SPLITS_PROCESSED, NONE, splitsExamined);

            long splitsBeforeFilter = dynamicFilterApplied ? 0 : splitsExamined;
            runtimeStats.addMetricValue(DYNAMIC_FILTER_SPLITS_BEFORE_FILTER, NONE, splitsBeforeFilter);
        }

        if (dynamicFilterActive && !filterWaitTimeRecorded) {
            // Filter resolved synchronously — record 0 wait time so the metric is
            // always present when DPP is enabled (distinguishes "no wait needed" from "DPP off")
            runtimeStats.addMetricValue(DYNAMIC_FILTER_WAIT_TIME_NANOS, NANO, 0);
        }

        try {
            closer.close();
            // TODO: remove this after org.apache.iceberg.io.CloseableIterator'withClose
            //  correct release resources holds by iterator.
            if (fileScanTaskIterator != null) {
                fileScanTaskIterator = CloseableIterator.empty();
            }
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

        return new IcebergSplit(
                task.file().path().toString(),
                task.start(),
                task.length(),
                fromIcebergFileFormat(task.file().format()),
                ImmutableList.of(),
                getPartitionKeys(task),
                PartitionSpecParser.toJson(spec),
                partitionData.map(PartitionData::toJson),
                nodeSelectionStrategy,
                SplitWeight.fromProportion(Math.min(Math.max((double) task.length() / targetSplitSize, minimumAssignedSplitWeight), 1.0)),
                task.deletes().stream().map(DeleteFile::fromIceberg).collect(toImmutableList()),
                Optional.empty(),
                getDataSequenceNumber(task.file()),
                affinitySchedulingFileSectionSize);
    }
}
