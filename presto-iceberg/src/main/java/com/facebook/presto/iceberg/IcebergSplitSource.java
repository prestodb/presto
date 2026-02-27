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
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_COLUMNS_RELEVANT;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_COLUMNS_SKIPPED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_CONSTRAINT_COLUMNS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PUSHED_INTO_SCAN;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPECULATIVE_BUFFER_OVERFLOW;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_BEFORE_FILTER;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_PROCESSED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_RETROACTIVELY_PRUNED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_SPECULATIVELY_BUFFERED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_WITHOUT_FILTER;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_WAIT_TIME_NANOS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getAffinitySchedulingFileSectionSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getNodeSelectionStrategy;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.FileFormat.fromIcebergFileFormat;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getDynamicFilterMaxSpeculativeSplits;
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
    private enum State
    {
        WAITING_FOR_FILTER,
        SPECULATING,
        SCANNING,
    }

    private static final ConnectorSplitBatch EMPTY_BATCH_NOT_FINISHED =
            new ConnectorSplitBatch(ImmutableList.of(), false);

    private static final int SPECULATIVE_DRAIN_BATCH_SIZE = 1000;

    private CloseableIterator<FileScanTask> fileScanTaskIterator;

    private final Closer closer = Closer.create();
    private final double minimumAssignedSplitWeight;
    private final long targetSplitSize;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final long affinitySchedulingFileSectionSize;

    private final TupleDomain<IcebergColumnHandle> metadataColumnConstraints;

    private final DynamicFilter dynamicFilter;
    private TableScan tableScan;
    private State state;

    private final RuntimeStats runtimeStats;
    private final boolean dynamicFilterActive;
    private final boolean extendedMetrics;
    private long splitsExamined;
    private long filterWaitStartNanos;
    private boolean dynamicFilterApplied;
    private boolean closed;

    // Speculative split enumeration
    private final int maxSpeculativeBufferSize;
    private List<FileScanTask> speculativeBuffer;
    private long speculativeTasksBuffered;
    private long speculativeTasksPruned;

    private final Optional<Set<ColumnHandle>> relevantFilterColumns;

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

        this.runtimeStats = session.getRuntimeStats();
        this.dynamicFilterActive = dynamicFilter.getWaitTimeout().toMillis() > 0;
        this.extendedMetrics = isDynamicFilterExtendedMetrics(session);
        this.maxSpeculativeBufferSize = getDynamicFilterMaxSpeculativeSplits(session);

        if (dynamicFilterActive && !dynamicFilter.isComplete()) {
            Set<ColumnHandle> relevant = computeRelevantFilterColumns(
                    dynamicFilter.getPendingFilterColumns(), tableScan);
            this.relevantFilterColumns = relevant.isEmpty() ? Optional.empty() : Optional.of(relevant);
        }
        else {
            this.relevantFilterColumns = Optional.empty();
        }

        if (dynamicFilter.isComplete()) {
            dynamicFilterApplied = true;
            initializeScan();
            state = State.SCANNING;
        }
        else if (dynamicFilterActive && maxSpeculativeBufferSize > 0) {
            // Start planFiles() immediately with static predicates
            initializeScan();
            state = State.SPECULATING;
            speculativeBuffer = new ArrayList<>();
        }
        else {
            state = State.WAITING_FOR_FILTER;
        }
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        switch (state) {
            case SCANNING:
                return completedFuture(enumerateSplitBatch(maxSize));
            case SPECULATING:
                return handleSpeculativeBatch(maxSize);
            case WAITING_FOR_FILTER:
                return handleBlockingBatch(maxSize);
            default:
                throw new IllegalStateException("Unexpected state: " + state);
        }
    }

    private CompletableFuture<ConnectorSplitBatch> handleSpeculativeBatch(int maxSize)
    {
        drainAvailableTasks();

        if (speculativeBuffer == null) {
            return handleBlockingBatch(maxSize);
        }

        startFilterWaitTimer();

        if (dynamicFilter.isComplete(relevantFilterColumns)) {
            recordFilterWaitTime();
            dynamicFilterApplied = true;
            applyRetroactiveFilter(dynamicFilter.getCurrentPredicate());
            return completedFuture(enumerateSplitBatch(maxSize));
        }

        CompletableFuture<?> blocked = dynamicFilter.isBlocked(relevantFilterColumns);

        if (!blocked.isDone()) {
            return blocked.thenApply(v -> EMPTY_BATCH_NOT_FINISHED);
        }

        // Timeout or unblocked but not complete — apply whatever we have
        recordFilterWaitTime();
        if (dynamicFilter.isComplete(relevantFilterColumns)) {
            dynamicFilterApplied = true;
            applyRetroactiveFilter(dynamicFilter.getCurrentPredicate());
        }
        else {
            // Timeout — use buffer as-is (no retroactive pruning)
            dynamicFilterApplied = false;
            applyRetroactiveFilter(TupleDomain.all());
        }
        return completedFuture(enumerateSplitBatch(maxSize));
    }

    private CompletableFuture<ConnectorSplitBatch> handleBlockingBatch(int maxSize)
    {
        if (dynamicFilter.isComplete(relevantFilterColumns)) {
            recordFilterWaitTime();
            dynamicFilterApplied = true;
            initializeScanWithDynamicFilter(dynamicFilter.getCurrentPredicate());
            state = State.SCANNING;
            return completedFuture(enumerateSplitBatch(maxSize));
        }

        startFilterWaitTimer();

        CompletableFuture<?> blocked = dynamicFilter.isBlocked(relevantFilterColumns);

        if (!blocked.isDone()) {
            return blocked.thenApply(v -> EMPTY_BATCH_NOT_FINISHED);
        }

        recordFilterWaitTime();
        dynamicFilterApplied = dynamicFilter.isComplete(relevantFilterColumns);
        initializeScanWithDynamicFilter(dynamicFilter.getCurrentPredicate());
        state = State.SCANNING;

        return completedFuture(enumerateSplitBatch(maxSize));
    }

    private void startFilterWaitTimer()
    {
        if (filterWaitStartNanos == 0) {
            filterWaitStartNanos = System.nanoTime();
        }
    }

    private void drainAvailableTasks()
    {
        int drained = 0;
        while (drained < SPECULATIVE_DRAIN_BATCH_SIZE
                && speculativeBuffer.size() < maxSpeculativeBufferSize
                && fileScanTaskIterator.hasNext()) {
            speculativeBuffer.add(fileScanTaskIterator.next());
            drained++;
        }
        speculativeTasksBuffered += drained;

        if (speculativeBuffer.size() >= maxSpeculativeBufferSize && fileScanTaskIterator.hasNext()) {
            fallBackToBlockingPath();
        }
    }

    private void fallBackToBlockingPath()
    {
        state = State.WAITING_FOR_FILTER;
        speculativeBuffer = null;
        speculativeTasksBuffered = 0;
        speculativeTasksPruned = 0;
        try {
            if (fileScanTaskIterator != null) {
                fileScanTaskIterator.close();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        runtimeStats.addMetricValue(DYNAMIC_FILTER_SPECULATIVE_BUFFER_OVERFLOW, NONE, 1);
    }

    private void applyRetroactiveFilter(TupleDomain<ColumnHandle> constraint)
    {
        while (fileScanTaskIterator.hasNext()) {
            speculativeBuffer.add(fileScanTaskIterator.next());
            speculativeTasksBuffered++;
        }

        if (constraint.isAll()) {
            switchToFilteredMode(speculativeBuffer);
            return;
        }

        if (constraint.isNone()) {
            speculativeTasksPruned = speculativeTasksBuffered;
            switchToFilteredMode(ImmutableList.of());
            return;
        }

        TupleDomain<IcebergColumnHandle> icebergConstraint = constraint
                .transform(columnHandle -> (IcebergColumnHandle) columnHandle);
        Expression dfExpression = toIcebergExpression(icebergConstraint);
        InclusiveMetricsEvaluator metricsEvaluator = new InclusiveMetricsEvaluator(tableScan.schema(), dfExpression);
        Map<Integer, Evaluator> partitionEvaluatorCache = new HashMap<>();

        List<FileScanTask> filtered = new ArrayList<>();
        for (FileScanTask task : speculativeBuffer) {
            if (taskMatchesFilter(task, dfExpression, metricsEvaluator, partitionEvaluatorCache)) {
                filtered.add(task);
            }
            else {
                speculativeTasksPruned++;
            }
        }

        runtimeStats.addMetricValue(DYNAMIC_FILTER_PUSHED_INTO_SCAN, NONE, 1);
        icebergConstraint.getDomains().ifPresent(domains ->
                runtimeStats.addMetricValue(DYNAMIC_FILTER_CONSTRAINT_COLUMNS, NONE, domains.size()));

        if (extendedMetrics) {
            runtimeStats.addMetricValue(DYNAMIC_FILTER_SPLITS_WITHOUT_FILTER, NONE, speculativeTasksBuffered);
        }

        switchToFilteredMode(filtered);
    }

    private boolean taskMatchesFilter(
            FileScanTask task,
            Expression expression,
            InclusiveMetricsEvaluator metricsEvaluator,
            Map<Integer, Evaluator> partitionEvaluatorCache)
    {
        // Level 1: Partition-level evaluation
        if (task.spec().isPartitioned()) {
            Evaluator partitionEvaluator = partitionEvaluatorCache.computeIfAbsent(
                    task.spec().specId(),
                    specId -> {
                        Expression projected = Projections.inclusive(task.spec()).project(expression);
                        return new Evaluator(task.spec().partitionType(), projected, false);
                    });
            if (!partitionEvaluator.eval(task.file().partition())) {
                return false;
            }
        }

        // Level 2: File-level column stats (min/max bounds, null counts)
        return metricsEvaluator.eval(task.file());
    }

    private void switchToFilteredMode(List<FileScanTask> filteredTasks)
    {
        state = State.SCANNING;
        fileScanTaskIterator = CloseableIterator.withClose(filteredTasks.iterator());
        speculativeBuffer = null; // allow GC
    }

    private void recordFilterWaitTime()
    {
        if (filterWaitStartNanos != 0) {
            long filterWaitNanos = System.nanoTime() - filterWaitStartNanos;
            runtimeStats.addMetricValue(DYNAMIC_FILTER_WAIT_TIME_NANOS, NANO, filterWaitNanos);
        }
    }

    private void initializeScan()
    {
        this.fileScanTaskIterator = closer.register(
                splitFiles(
                        closer.register(tableScan.planFiles()),
                        targetSplitSize)
                        .iterator());
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
                runtimeStats.addMetricValue("dynamicFilterExpressionOp", NONE, dfExpression.op().ordinal());
            }
            tableScan = tableScan.filter(dfExpression);
            runtimeStats.addMetricValue(DYNAMIC_FILTER_PUSHED_INTO_SCAN, NONE, 1);
            icebergConstraint.getDomains().ifPresent(domains ->
                    runtimeStats.addMetricValue(DYNAMIC_FILTER_CONSTRAINT_COLUMNS, NONE, domains.size()));
        }
        initializeScan();
    }

    /**
     * Determines which pending dynamic filter columns are discriminating for this table.
     * A column is discriminating if it is a partition column (any transform) or a sort column
     * in the table's sort order.
     */
    private Set<ColumnHandle> computeRelevantFilterColumns(
            Set<ColumnHandle> pendingFilterColumns,
            TableScan scan)
    {
        if (pendingFilterColumns.isEmpty()) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<Integer> discriminatingFieldIds = ImmutableSet.builder();

        // All partition columns (any transform: identity, bucket, truncate, year, etc.)
        for (PartitionSpec spec : scan.table().specs().values()) {
            for (PartitionField field : spec.fields()) {
                discriminatingFieldIds.add(field.sourceId());
            }
        }

        // Sort columns
        SortOrder sortOrder = scan.table().sortOrder();
        if (sortOrder != null && !sortOrder.isUnsorted()) {
            for (SortField field : sortOrder.fields()) {
                discriminatingFieldIds.add(field.sourceId());
            }
        }

        Set<Integer> discriminatingIds = discriminatingFieldIds.build();
        int totalPending = pendingFilterColumns.size();

        ImmutableSet.Builder<ColumnHandle> relevant = ImmutableSet.builder();
        for (ColumnHandle handle : pendingFilterColumns) {
            if (handle instanceof IcebergColumnHandle) {
                IcebergColumnHandle icebergHandle = (IcebergColumnHandle) handle;
                if (discriminatingIds.contains(icebergHandle.getId())) {
                    relevant.add(handle);
                }
            }
        }

        Set<ColumnHandle> result = relevant.build();

        if (extendedMetrics) {
            runtimeStats.addMetricValue(DYNAMIC_FILTER_COLUMNS_RELEVANT, NONE, result.size());
            runtimeStats.addMetricValue(DYNAMIC_FILTER_COLUMNS_SKIPPED, NONE, totalPending - result.size());
        }

        return result;
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

                runtimeStats.addMetricValue("dynamicFilterDomainPrestoTypeId[" + colName + "]", NONE, prestoType.getTypeSignature().hashCode());

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
        return state == State.SCANNING && !fileScanTaskIterator.hasNext();
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

            if (speculativeTasksBuffered > 0) {
                runtimeStats.addMetricValue(DYNAMIC_FILTER_SPLITS_SPECULATIVELY_BUFFERED, NONE, speculativeTasksBuffered);
                runtimeStats.addMetricValue(DYNAMIC_FILTER_SPLITS_RETROACTIVELY_PRUNED, NONE, speculativeTasksPruned);
            }
        }

        boolean waitTimeAlreadyRecorded = (state == State.SCANNING && filterWaitStartNanos != 0);
        if (dynamicFilterActive && !waitTimeAlreadyRecorded) {
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
