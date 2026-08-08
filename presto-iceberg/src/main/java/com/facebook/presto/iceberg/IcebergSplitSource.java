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
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.DynamicFilter;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
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
import java.util.OptionalInt;
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
    private static final ConnectorSplitBatch EMPTY_BATCH_NOT_FINISHED =
            new ConnectorSplitBatch(ImmutableList.of(), false);

    private enum State
    {
        WAITING_FOR_FILTER,
        WARMUP_SCANNING,
        WARMUP_PAUSED,
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
    private final InclusiveMetricsEvaluator lineageEvaluator;
    // Preferred Presto FileFormat the table is configured to write
    // (`write.format.default`), or null when the property is absent /
    // unrecognized. Used to disambiguate the iceberg-api wire format on
    // splits whose `task.file().format()` is ORC — because iceberg-api has no
    // NIMBLE / DWRF enum values, both NIMBLE and DWRF data files appear on
    // the manifest as `Iceberg.ORC` and the worker can't tell them apart
    // without help. See `toIcebergSplit()` for the override logic.
    private final FileFormat tableWriteFormat;

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
    private boolean filterWaitTimeEmitted;
    private boolean dynamicFilterApplied;
    private boolean closed;

    private final Optional<Set<ColumnHandle>> relevantFilterColumns;

    private Expression filterExpression;
    private InclusiveMetricsEvaluator metricsEvaluator;
    private Map<Integer, Evaluator> partitionEvaluatorCache;
    private boolean inlineFilterActive;
    // Tracks the last predicate pushed into the scan; per-batch narrowing only re-applies
    // when a strictly newer predicate arrives.
    private TupleDomain<ColumnHandle> lastAppliedPredicate = TupleDomain.all();

    private OptionalInt warmupMaxWeight;
    private final double warmupWeightPerTask;
    private Set<SplitKey> dispatchedSplitKeys;
    private long warmupWeightDispatched;
    private long warmupPauseStartNanos;

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
        this.lineageEvaluator = buildLastUpdatedSequenceNumberEvaluator(metadataColumnConstraints);
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.tableSchema = tableScan.table().schema();
        this.tableWriteFormat = parseTableWriteFormat(tableScan);
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
                    dynamicFilter.getPendingFilterColumns());
            this.relevantFilterColumns = Optional.of(relevant);
        }
        else {
            this.relevantFilterColumns = Optional.empty();
        }

        // taskCountHint is 0 at construction; defer warmup budget computation to first getNextBatch().
        this.warmupMaxWeight = OptionalInt.empty();

        if (dynamicFilter.isComplete()) {
            dynamicFilterApplied = true;
            initializeScanWithDynamicFilter(dynamicFilter.getCurrentPredicate());
            state = State.SCANNING_FILTERED;
        }
        else if (relevantFilterColumns.isPresent() && relevantFilterColumns.get().isEmpty()) {
            initializeScan();
            state = State.SCANNING_FILTERED;
        }
        else if (warmupEnabled && warmupWeightPerTask > 0) {
            initializeScan();
            this.dispatchedSplitKeys = new HashSet<>();
            filterWaitStartNanos = System.nanoTime();
            dynamicFilter.isBlocked(relevantFilterColumns);
            state = State.WARMUP_SCANNING;
        }
        else {
            filterWaitStartNanos = System.nanoTime();
            dynamicFilter.isBlocked(relevantFilterColumns);
            state = State.WAITING_FOR_FILTER;
        }
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
        this.dynamicFilter = DynamicFilter.EMPTY;
        this.tableScan = null;
        this.tableSchema = null;
        this.targetSplitSize = targetSplitSize;
        this.minimumAssignedSplitWeight = getMinimumAssignedSplitWeight(session);
        this.nodeSelectionStrategy = getNodeSelectionStrategy(session);
        this.affinitySchedulingFileSectionSize = getAffinitySchedulingFileSectionSize(session).toBytes();
        this.tableWriteFormat = tableWriteFormat;
        this.runtimeStats = session.getRuntimeStats();
        this.dynamicFilterActive = false;
        this.extendedMetrics = false;
        this.warmupEnabled = false;
        this.warmupWeightPerTask = 0;
        this.relevantFilterColumns = Optional.empty();
        this.warmupMaxWeight = OptionalInt.of(0);
        this.fileScanTaskIterator = closer.register(
                splitFiles(
                        closer.register(fileScanTasks),
                        targetSplitSize)
                        .iterator());
        this.state = State.SCANNING_FILTERED;
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
        if (state == State.WAITING_FOR_FILTER) {
            if (shouldExitWaiting()) {
                transitionFromBlocking();
            }
            else {
                // Return a not-yet-completed future so that BufferingSplitSource
                // does not recursively chain on an already-completed empty batch.
                CompletableFuture<?> blocked = dynamicFilter.isBlocked(relevantFilterColumns);
                if (!blocked.isDone()) {
                    return blocked.thenApply(ignored -> {
                        if (closed) {
                            return EMPTY_BATCH_NOT_FINISHED;
                        }
                        if (shouldExitWaiting()) {
                            transitionFromBlocking();
                        }
                        return state == State.SCANNING_FILTERED ? enumerateSplitBatch(maxSize) : EMPTY_BATCH_NOT_FINISHED;
                    });
                }
                transitionFromBlocking();
            }
        }
        else if (state == State.WARMUP_SCANNING) {
            if (!warmupMaxWeight.isPresent() && warmupEnabled && warmupWeightPerTask > 0) {
                int taskCountHint = dynamicFilter.getTaskCountHint();
                if (taskCountHint > 0) {
                    int maxWeight = (int) Math.round(warmupWeightPerTask * SplitWeight.rawValueForStandardSplitCount(taskCountHint));
                    this.warmupMaxWeight = OptionalInt.of(maxWeight);
                }
            }

            if (shouldExitWaiting()) {
                transitionToFilteredReScan();
            }
            else if (warmupMaxWeight.isPresent() && warmupWeightDispatched >= warmupMaxWeight.getAsInt()) {
                warmupPauseStartNanos = System.nanoTime();
                state = State.WARMUP_PAUSED;
                return handleWarmupPaused(maxSize);
            }
        }
        else if (state == State.WARMUP_PAUSED) {
            return handleWarmupPaused(maxSize);
        }
        // Re-check predicate each batch; filters arriving after the warmup transition narrow inline.
        if (state == State.SCANNING_FILTERED) {
            TupleDomain<ColumnHandle> currentPredicate = dynamicFilter.getCurrentPredicate();
            if (!currentPredicate.equals(lastAppliedPredicate)) {
                activateInlineFilter(currentPredicate);
            }
        }
        return completedFuture(enumerateSplitBatch(maxSize));
    }

    private CompletableFuture<ConnectorSplitBatch> handleWarmupPaused(int maxSize)
    {
        if (shouldExitWaiting()) {
            transitionToFilteredReScan();
            return completedFuture(enumerateSplitBatch(maxSize));
        }

        CompletableFuture<?> blocked = dynamicFilter.isBlocked(relevantFilterColumns);
        if (!blocked.isDone()) {
            return blocked.thenApply(ignored -> {
                if (closed) {
                    return EMPTY_BATCH_NOT_FINISHED;
                }
                if (shouldExitWaiting()) {
                    transitionToFilteredReScan();
                }
                return state == State.SCANNING_FILTERED ? enumerateSplitBatch(maxSize) : EMPTY_BATCH_NOT_FINISHED;
            });
        }
        transitionToFilteredReScan();
        return completedFuture(enumerateSplitBatch(maxSize));
    }

    private void transitionFromBlocking()
    {
        recordFilterWaitTime();
        dynamicFilterApplied = dynamicFilter.isComplete(relevantFilterColumns)
                || dynamicFilter.hasAnyComplete(relevantFilterColumns);
        state = State.SCANNING_FILTERED;
        initializeScanWithDynamicFilter(dynamicFilter.getCurrentPredicate());
    }

    private void transitionToFilteredReScan()
    {
        recordFilterWaitTime();
        if (warmupPauseStartNanos > 0 && extendedMetrics) {
            long pauseNanos = System.nanoTime() - warmupPauseStartNanos;
            runtimeStats.addMetricValue("dynamicFilterWarmupPauseNanos", NANO, pauseNanos);
        }
        dynamicFilterApplied = dynamicFilter.isComplete(relevantFilterColumns)
                || dynamicFilter.hasAnyComplete(relevantFilterColumns);
        state = State.SCANNING_FILTERED;

        TupleDomain<ColumnHandle> currentPredicate = dynamicFilter.getCurrentPredicate();
        if (!currentPredicate.isAll()) {
            reScanTriggered = true;
            closeCurrentIterator();
            initializeScanWithDynamicFilter(currentPredicate);
        }
        else {
            activateInlineFilter(currentPredicate);
            dispatchedSplitKeys = null;
        }
    }

    private void activateInlineFilter(TupleDomain<ColumnHandle> dynamicFilterConstraint)
    {
        if (dynamicFilterConstraint.isAll()) {
            return;
        }

        TupleDomain<IcebergColumnHandle> icebergConstraint = dynamicFilterConstraint
                .transform(columnHandle -> columnHandle instanceof IcebergColumnHandle ? (IcebergColumnHandle) columnHandle : null);

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

        this.filterExpression = dfExpression;
        this.metricsEvaluator = new InclusiveMetricsEvaluator(tableSchema, dfExpression);
        this.partitionEvaluatorCache = new HashMap<>();
        this.inlineFilterActive = true;
        this.lastAppliedPredicate = dynamicFilterConstraint;
    }

    private boolean taskMatchesFilter(FileScanTask task)
    {
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
        return metricsEvaluator.eval(task.file());
    }

    private boolean hasExceededFilterWaitTime()
    {
        return filterWaitStartNanos > 0
                && Duration.nanosSince(filterWaitStartNanos).compareTo(dynamicFilter.getWaitTimeout()) >= 0;
    }

    /**
     * Whether the WAITING_FOR_FILTER / WARMUP_* state should exit and start producing
     * splits. Exits when (a) all relevant filters are complete, (b) at least one is
     * complete *and* contributes a useful (non-{@code all()}) constraint, or (c) the
     * wait timeout has elapsed.
     *
     * <p>The condition is asymmetric on purpose: a single filter resolving to
     * {@code all()} (e.g., a short-circuit) does not unblock the gate by itself —
     * we want to wait for siblings that may carry actual pruning constraints.
     * If every filter ends up trivial, {@code isComplete} eventually fires and the
     * gate exits anyway.
     */
    private boolean shouldExitWaiting()
    {
        if (hasExceededFilterWaitTime()) {
            return true;
        }
        if (dynamicFilter.isComplete(relevantFilterColumns)) {
            return true;
        }
        return dynamicFilter.hasAnyComplete(relevantFilterColumns)
                && !dynamicFilter.getCurrentPredicate().isAll();
    }

    private void recordFilterWaitTime()
    {
        if (filterWaitStartNanos != 0) {
            long filterWaitNanos = System.nanoTime() - filterWaitStartNanos;
            runtimeStats.addMetricValue(DYNAMIC_FILTER_WAIT_TIME_NANOS, NANO, filterWaitNanos);
            filterWaitStartNanos = 0;
            filterWaitTimeEmitted = true;
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
                    .transform(columnHandle -> columnHandle instanceof IcebergColumnHandle ? (IcebergColumnHandle) columnHandle : null);

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
            this.lastAppliedPredicate = dynamicFilterConstraint;
        }
        this.fileScanTaskIterator = closer.register(
                splitFiles(
                        closer.register(filteredScan.planFiles()),
                        targetSplitSize)
                        .iterator());
    }

    private void closeCurrentIterator()
    {
        // Closer owns the lifecycle; null out so enumerateSplitBatch stops reading before re-scan.
        fileScanTaskIterator = null;
    }

    /**
     * Determines which pending dynamic filter columns are relevant for this table.
     * All columns with pending dynamic filters are considered relevant because
     * Iceberg's InclusiveMetricsEvaluator can prune data files using file-level
     * column statistics (min/max bounds) for any column, not just partition or
     * sort columns.
     */
    private Set<ColumnHandle> computeRelevantFilterColumns(
            Set<ColumnHandle> pendingFilterColumns)
    {
        if (pendingFilterColumns.isEmpty()) {
            return ImmutableSet.of();
        }

        Set<ColumnHandle> result = ImmutableSet.copyOf(pendingFilterColumns);

        if (extendedMetrics) {
            runtimeStats.addMetricValue(DYNAMIC_FILTER_COLUMNS_RELEVANT, NONE, result.size());
            runtimeStats.addMetricValue(DYNAMIC_FILTER_COLUMNS_SKIPPED, NONE, 0);
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

            if (inlineFilterActive && !taskMatchesFilter(task)) {
                splitsFilteredInline++;
                continue;
            }

            IcebergSplit icebergSplit = (IcebergSplit) toIcebergSplit(task);

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

            if (state == State.WARMUP_SCANNING) {
                warmupSplitsDispatched++;
                warmupWeightDispatched += icebergSplit.getSplitWeight().getRawValue();
                dispatchedSplitKeys.add(new SplitKey(icebergSplit.getPath(), icebergSplit.getStart()));
                splitsBeforeFilter++;
            }

            if (metadataColumnsMatchPredicates(metadataColumnConstraints, icebergSplit.getPath(), icebergSplit.getDataSequenceNumber(), task.file(), lineageEvaluator)) {
                splits.add(icebergSplit);
            }

            // Check warmup budget after adding the split (don't break mid-batch for partial weight)
            if (state == State.WARMUP_SCANNING && warmupMaxWeight.isPresent() && warmupWeightDispatched >= warmupMaxWeight.getAsInt()) {
                break;
            }
        }
        return new ConnectorSplitBatch(splits, isFinished());
    }

    @Override
    public boolean isFinished()
    {
        return state == State.SCANNING_FILTERED
                && fileScanTaskIterator != null
                && !fileScanTaskIterator.hasNext();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        if (dynamicFilterActive) {
            runtimeStats.addMetricValue(DYNAMIC_FILTER_SPLITS_PROCESSED, NONE,
                    splitsExamined - splitsFilteredInline - reScanDedupSkipped);

            // Small-table scans may finish in a single batch before later-arriving filters
            // are seen by the per-batch narrowing loop; catch up here for accurate metrics.
            if (state == State.SCANNING_FILTERED) {
                TupleDomain<ColumnHandle> finalPredicate = dynamicFilter.getCurrentPredicate();
                if (!finalPredicate.isAll() && !finalPredicate.equals(lastAppliedPredicate)) {
                    lastAppliedPredicate = finalPredicate;
                }
            }

            // Emit once at close so per-batch activateInlineFilter calls don't double-count.
            if (!lastAppliedPredicate.isAll()) {
                runtimeStats.addMetricValue(DYNAMIC_FILTER_PUSHED_INTO_SCAN, NONE, 1);
                lastAppliedPredicate.getDomains().ifPresent(domains ->
                        runtimeStats.addMetricValue(DYNAMIC_FILTER_CONSTRAINT_COLUMNS, NONE, domains.size()));
            }

            long effectiveSplitsBeforeFilter;
            if (!relevantFilterColumns.isPresent() || relevantFilterColumns.get().isEmpty()) {
                effectiveSplitsBeforeFilter = 0;
            }
            else if (!dynamicFilterApplied) {
                effectiveSplitsBeforeFilter = splitsExamined;
            }
            else {
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

        if (dynamicFilterActive) {
            if (filterWaitStartNanos != 0) {
                recordFilterWaitTime();
            }
            else if (!filterWaitTimeEmitted) {
                runtimeStats.addMetricValue(DYNAMIC_FILTER_WAIT_TIME_NANOS, NANO, 0);
            }
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
