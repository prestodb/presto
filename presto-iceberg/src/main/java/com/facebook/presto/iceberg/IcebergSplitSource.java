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

import com.facebook.airlift.units.Duration;
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
import com.facebook.presto.spi.PrestoException;
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
import org.apache.iceberg.Schema;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_COLUMNS_RELEVANT;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_COLUMNS_SKIPPED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_CONSTRAINT_COLUMNS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PUSHED_INTO_SCAN;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_BEFORE_FILTER;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SPLITS_PROCESSED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_WAIT_TIME_NANOS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getAffinitySchedulingFileSectionSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getNodeSelectionStrategy;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.FileFormat.fromIcebergFileFormat;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getDynamicFilterWarmupWeightPerTask;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getMinimumAssignedSplitWeight;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isDynamicFilterExtendedMetrics;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isDynamicFilterWarmupEnabled;
import static com.facebook.presto.iceberg.IcebergUtil.getDataSequenceNumber;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionKeys;
import static com.facebook.presto.iceberg.IcebergUtil.getTargetSplitSize;
import static com.facebook.presto.iceberg.IcebergUtil.metadataColumnsMatchPredicates;
import static com.facebook.presto.iceberg.IcebergUtil.partitionDataFromStructLike;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
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

    private enum State
    {
        WAITING_FOR_FILTER,
        WARMUP_SCANNING,
        WARMUP_PAUSED,
        SCANNING_UNFILTERED,
        SCANNING_FILTERED
    }

    /**
     * Key for deduplicating warmup splits during filtered re-scan.
     */
    private static final class SplitKey
    {
        private final String path;
        private final long start;

        SplitKey(String path, long start)
        {
            this.path = requireNonNull(path, "path is null");
            this.start = start;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SplitKey splitKey = (SplitKey) o;
            return start == splitKey.start && path.equals(splitKey.path);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(path, start);
        }
    }

    private CloseableIterator<FileScanTask> fileScanTaskIterator;

    private final Closer closer = Closer.create();
    private final double minimumAssignedSplitWeight;
    private final long targetSplitSize;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final long affinitySchedulingFileSectionSize;

    private final TupleDomain<IcebergColumnHandle> metadataColumnConstraints;

    private final DynamicFilter dynamicFilter;
    private final TableScan tableScan;
    private final Schema tableSchema;
    private final boolean warmupEnabled;
    private State state;

    private final RuntimeStats runtimeStats;
    private final boolean dynamicFilterActive;
    private final boolean extendedMetrics;
    private long splitsExamined;
    private long filterWaitStartNanos;
    private boolean dynamicFilterApplied;
    private boolean closed;

    private final Optional<Set<ColumnHandle>> relevantFilterColumns;

    // In-line filter evaluation (set when filter resolves mid-enumeration)
    private Expression filterExpression;
    private InclusiveMetricsEvaluator metricsEvaluator;
    private Map<Integer, Evaluator> partitionEvaluatorCache;
    private boolean inlineFilterActive;

    // Warmup state
    private final long warmupMaxWeight;
    private final double warmupWeightPerTask;
    private Set<SplitKey> dispatchedSplitKeys;
    private long warmupWeightDispatched;
    private long warmupPauseStartNanos;

    // Metrics
    private long splitsBeforeFilter;
    private long splitsFilteredInline;
    private long warmupSplitsDispatched;
    private long reScanDedupSkipped;
    private long reScanSplitsProduced;
    private boolean reScanTriggered;

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
        this.tableSchema = tableScan.table().schema();
        this.targetSplitSize = getTargetSplitSize(session, tableScan).toBytes();
        this.minimumAssignedSplitWeight = getMinimumAssignedSplitWeight(session);
        this.nodeSelectionStrategy = getNodeSelectionStrategy(session);
        this.affinitySchedulingFileSectionSize = getAffinitySchedulingFileSectionSize(session).toBytes();

        this.runtimeStats = session.getRuntimeStats();
        this.dynamicFilterActive = dynamicFilter.getWaitTimeout().toMillis() > 0;
        this.extendedMetrics = isDynamicFilterExtendedMetrics(session);
        this.warmupEnabled = isDynamicFilterWarmupEnabled(session);
        this.warmupWeightPerTask = getDynamicFilterWarmupWeightPerTask(session);

        if (dynamicFilterActive && !dynamicFilter.isComplete()) {
            Set<ColumnHandle> relevant = computeRelevantFilterColumns(
                    dynamicFilter.getPendingFilterColumns(), tableScan);
            this.relevantFilterColumns = Optional.of(relevant);
        }
        else {
            this.relevantFilterColumns = Optional.empty();
        }

        // Compute warmup budget: weightPerTask * taskCount * UNIT_VALUE
        // e.g., weightPerTask=1.0, taskCount=4 → budget = 400 raw units = 4 standard splits
        int taskCountHint = dynamicFilter.getTaskCountHint();
        if (warmupEnabled && warmupWeightPerTask > 0 && taskCountHint > 0) {
            this.warmupMaxWeight = Math.round(warmupWeightPerTask * SplitWeight.rawValueForStandardSplitCount(taskCountHint));
        }
        else {
            this.warmupMaxWeight = 0;
        }

        if (dynamicFilter.isComplete()) {
            // Filter already resolved at construction — apply it to the scan upfront
            dynamicFilterApplied = true;
            initializeScanWithDynamicFilter(dynamicFilter.getCurrentPredicate());
            state = State.SCANNING_FILTERED;
        }
        else if (relevantFilterColumns.isPresent() && relevantFilterColumns.get().isEmpty()) {
            // No discriminating columns — proceed without waiting for filter
            initializeScan();
            state = State.SCANNING_FILTERED;
        }
        else if (warmupEnabled && warmupMaxWeight > 0) {
            // Warmup mode: dispatch limited initial batch, then pause for filter
            initializeScan();
            this.dispatchedSplitKeys = new HashSet<>();
            filterWaitStartNanos = System.nanoTime();
            dynamicFilter.isBlocked(relevantFilterColumns);
            state = State.WARMUP_SCANNING;
        }
        else if (warmupEnabled) {
            // warmupEnabled but no budget (warmupWeightPerTask=0 or taskCountHint=0)
            // Fall back to full eager dispatch (existing SCANNING_UNFILTERED behavior)
            initializeScan();
            filterWaitStartNanos = System.nanoTime();
            dynamicFilter.isBlocked(relevantFilterColumns);
            state = State.SCANNING_UNFILTERED;
        }
        else {
            // Don't start scan yet — wait for filter so Iceberg can skip manifests
            filterWaitStartNanos = System.nanoTime();
            dynamicFilter.isBlocked(relevantFilterColumns);
            state = State.WAITING_FOR_FILTER;
        }
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        if (state == State.WAITING_FOR_FILTER) {
            if (dynamicFilter.isComplete(relevantFilterColumns) || hasExceededFilterWaitTime()) {
                transitionFromBlocking();
            }
            else {
                // Return a not-yet-completed future so that BufferingSplitSource
                // does not recursively chain on an already-completed empty batch.
                CompletableFuture<?> blocked = dynamicFilter.isBlocked(relevantFilterColumns);
                if (!blocked.isDone()) {
                    return blocked.thenApply(ignored -> {
                        if (dynamicFilter.isComplete(relevantFilterColumns) || hasExceededFilterWaitTime()) {
                            transitionFromBlocking();
                        }
                        return state == State.SCANNING_FILTERED ? enumerateSplitBatch(maxSize) : EMPTY_BATCH_NOT_FINISHED;
                    });
                }
                // isBlocked() is already done — filter must be complete (or brief race).
                transitionFromBlocking();
            }
        }
        else if (state == State.WARMUP_SCANNING) {
            if (dynamicFilter.isComplete(relevantFilterColumns) || hasExceededFilterWaitTime()) {
                transitionToFilteredReScan();
            }
            else if (warmupWeightDispatched >= warmupMaxWeight) {
                // Budget exhausted — pause and wait for filter
                warmupPauseStartNanos = System.nanoTime();
                state = State.WARMUP_PAUSED;
                return handleWarmupPaused(maxSize);
            }
            // else: still within warmup budget, dispatch more splits
        }
        else if (state == State.WARMUP_PAUSED) {
            return handleWarmupPaused(maxSize);
        }
        else if (state == State.SCANNING_UNFILTERED) {
            if (dynamicFilter.isComplete(relevantFilterColumns) || hasExceededFilterWaitTime()) {
                transitionToFilteredScanning();
            }
        }
        return completedFuture(enumerateSplitBatch(maxSize));
    }

    private CompletableFuture<ConnectorSplitBatch> handleWarmupPaused(int maxSize)
    {
        if (dynamicFilter.isComplete(relevantFilterColumns) || hasExceededFilterWaitTime()) {
            transitionToFilteredReScan();
            return completedFuture(enumerateSplitBatch(maxSize));
        }

        CompletableFuture<?> blocked = dynamicFilter.isBlocked(relevantFilterColumns);
        if (!blocked.isDone()) {
            return blocked.thenApply(ignored -> {
                if (dynamicFilter.isComplete(relevantFilterColumns) || hasExceededFilterWaitTime()) {
                    transitionToFilteredReScan();
                }
                return state == State.SCANNING_FILTERED ? enumerateSplitBatch(maxSize) : EMPTY_BATCH_NOT_FINISHED;
            });
        }
        // isBlocked() already done
        transitionToFilteredReScan();
        return completedFuture(enumerateSplitBatch(maxSize));
    }

    private void transitionFromBlocking()
    {
        recordFilterWaitTime();
        dynamicFilterApplied = dynamicFilter.isComplete(relevantFilterColumns);
        state = State.SCANNING_FILTERED;

        if (dynamicFilterApplied) {
            // Initialize scan with filter so Iceberg can skip manifests
            initializeScanWithDynamicFilter(dynamicFilter.getCurrentPredicate());
        }
        else {
            // Timeout — start unfiltered scan
            initializeScan();
        }
    }

    private void transitionToFilteredScanning()
    {
        recordFilterWaitTime();
        dynamicFilterApplied = dynamicFilter.isComplete(relevantFilterColumns);
        state = State.SCANNING_FILTERED;

        if (dynamicFilterApplied) {
            activateInlineFilter(dynamicFilter.getCurrentPredicate());
        }
    }

    private void transitionToFilteredReScan()
    {
        recordFilterWaitTime();
        if (warmupPauseStartNanos > 0 && extendedMetrics) {
            long pauseNanos = System.nanoTime() - warmupPauseStartNanos;
            runtimeStats.addMetricValue("dynamicFilterWarmupPauseNanos", NANO, pauseNanos);
        }
        dynamicFilterApplied = dynamicFilter.isComplete(relevantFilterColumns);
        state = State.SCANNING_FILTERED;

        if (dynamicFilterApplied) {
            reScanTriggered = true;
            // Close current iterator, re-scan with manifest pruning
            closeCurrentIterator();
            initializeScanWithDynamicFilter(dynamicFilter.getCurrentPredicate());
        }
        else {
            // Timeout — resume current iterator with inline filter
            activateInlineFilter(dynamicFilter.getCurrentPredicate());
        }
    }

    private void activateInlineFilter(TupleDomain<ColumnHandle> dynamicFilterConstraint)
    {
        if (dynamicFilterConstraint.isAll()) {
            return;
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

        runtimeStats.addMetricValue(DYNAMIC_FILTER_PUSHED_INTO_SCAN, NONE, 1);
        icebergConstraint.getDomains().ifPresent(domains ->
                runtimeStats.addMetricValue(DYNAMIC_FILTER_CONSTRAINT_COLUMNS, NONE, domains.size()));

        // Set up in-line evaluators for remaining files
        this.filterExpression = dfExpression;
        this.metricsEvaluator = new InclusiveMetricsEvaluator(tableSchema, dfExpression);
        this.partitionEvaluatorCache = new HashMap<>();
        this.inlineFilterActive = true;
    }

    private boolean taskMatchesFilter(FileScanTask task)
    {
        // Level 1: Partition-level evaluation
        if (task.spec().isPartitioned()) {
            Evaluator partEval = partitionEvaluatorCache.computeIfAbsent(
                    task.spec().specId(),
                    specId -> {
                        Expression projected = Projections.inclusive(task.spec()).project(filterExpression);
                        return new Evaluator(task.spec().partitionType(), projected);
                    });
            if (!partEval.eval(task.file().partition())) {
                return false;
            }
        }
        // Level 2: File-level column stats (min/max bounds, null counts)
        return metricsEvaluator.eval(task.file());
    }

    private boolean hasExceededFilterWaitTime()
    {
        return filterWaitStartNanos > 0
                && Duration.nanosSince(filterWaitStartNanos).compareTo(dynamicFilter.getWaitTimeout()) >= 0;
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
        TableScan filteredScan = this.tableScan;
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
            filteredScan = filteredScan.filter(dfExpression);
            runtimeStats.addMetricValue(DYNAMIC_FILTER_PUSHED_INTO_SCAN, NONE, 1);
            icebergConstraint.getDomains().ifPresent(domains ->
                    runtimeStats.addMetricValue(DYNAMIC_FILTER_CONSTRAINT_COLUMNS, NONE, domains.size()));
        }
        this.fileScanTaskIterator = closer.register(
                splitFiles(
                        closer.register(filteredScan.planFiles()),
                        targetSplitSize)
                        .iterator());
    }

    private void closeCurrentIterator()
    {
        if (fileScanTaskIterator != null) {
            try {
                fileScanTaskIterator.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
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

            // Apply in-line filter if active (filter resolved mid-enumeration)
            if (inlineFilterActive && !taskMatchesFilter(task)) {
                splitsFilteredInline++;
                continue;
            }

            IcebergSplit icebergSplit = (IcebergSplit) toIcebergSplit(task);

            // Dedup: skip warmup splits during filtered re-scan
            if (dispatchedSplitKeys != null && state == State.SCANNING_FILTERED) {
                SplitKey key = new SplitKey(icebergSplit.getPath(), icebergSplit.getStart());
                if (dispatchedSplitKeys.remove(key)) {
                    reScanDedupSkipped++;
                    continue;
                }
                reScanSplitsProduced++;
                if (dispatchedSplitKeys.isEmpty()) {
                    dispatchedSplitKeys = null;
                }
            }

            if (state == State.SCANNING_UNFILTERED) {
                splitsBeforeFilter++;
            }

            if (state == State.WARMUP_SCANNING) {
                warmupSplitsDispatched++;
                warmupWeightDispatched += icebergSplit.getSplitWeight().getRawValue();
                dispatchedSplitKeys.add(new SplitKey(icebergSplit.getPath(), icebergSplit.getStart()));
                splitsBeforeFilter++;
            }

            if (metadataColumnsMatchPredicates(metadataColumnConstraints, icebergSplit.getPath(), icebergSplit.getDataSequenceNumber())) {
                splits.add(icebergSplit);
            }

            // Check warmup budget after adding the split (don't break mid-batch for partial weight)
            if (state == State.WARMUP_SCANNING && warmupWeightDispatched >= warmupMaxWeight) {
                break;
            }
        }
        return new ConnectorSplitBatch(splits, isFinished());
    }

    @Override
    public boolean isFinished()
    {
        return fileScanTaskIterator != null && !fileScanTaskIterator.hasNext();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        if (dynamicFilterActive) {
            // SPLITS_PROCESSED: files dispatched as splits (excluding inline-filtered and dedup-skipped)
            runtimeStats.addMetricValue(DYNAMIC_FILTER_SPLITS_PROCESSED, NONE,
                    splitsExamined - splitsFilteredInline - reScanDedupSkipped);

            // SPLITS_BEFORE_FILTER: files dispatched without the benefit of dynamic filter
            long effectiveSplitsBeforeFilter;
            if (!relevantFilterColumns.isPresent() || relevantFilterColumns.get().isEmpty()) {
                // No discriminating columns — filter is irrelevant for this scan
                effectiveSplitsBeforeFilter = 0;
            }
            else if (!dynamicFilterApplied) {
                // Filter was never applied (scan finished before filter, or timeout)
                effectiveSplitsBeforeFilter = splitsExamined;
            }
            else {
                // Filter was applied — count warmup or eagerly dispatched splits
                effectiveSplitsBeforeFilter = splitsBeforeFilter;
            }
            runtimeStats.addMetricValue(DYNAMIC_FILTER_SPLITS_BEFORE_FILTER, NONE, effectiveSplitsBeforeFilter);

            if (extendedMetrics) {
                runtimeStats.addMetricValue("dynamicFilterSplitsFilteredInline", NONE, splitsFilteredInline);
                runtimeStats.addMetricValue("dynamicFilterWarmupSplitsDispatched", NONE, warmupSplitsDispatched);
                runtimeStats.addMetricValue("dynamicFilterWarmupWeightDispatched", NONE, warmupWeightDispatched);
                runtimeStats.addMetricValue("dynamicFilterReScanTriggered", NONE, reScanTriggered ? 1 : 0);
                runtimeStats.addMetricValue("dynamicFilterReScanDedupSkipped", NONE, reScanDedupSkipped);
                runtimeStats.addMetricValue("dynamicFilterReScanSplitsProduced", NONE, reScanSplitsProduced);
            }
        }

        boolean waitTimeAlreadyRecorded = (state == State.SCANNING_FILTERED && filterWaitStartNanos != 0);
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

        // Validate no PUFFIN deletion vectors (Iceberg v3 feature not yet supported)
        for (org.apache.iceberg.DeleteFile deleteFile : task.deletes()) {
            if (deleteFile.format() == org.apache.iceberg.FileFormat.PUFFIN) {
                throw new PrestoException(NOT_SUPPORTED, "Iceberg deletion vectors (PUFFIN format) are not supported");
            }
        }

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
