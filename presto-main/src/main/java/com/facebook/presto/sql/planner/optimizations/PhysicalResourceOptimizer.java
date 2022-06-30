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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.execution.Column;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.google.common.collect.ImmutableList;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class PhysicalResourceOptimizer
        implements PlanOptimizer
{
    private static final Logger log = Logger.get(PhysicalResourceOptimizer.class);
    private static final int PARTITION_COUNT_FOR_DIRCU_OPTIMIZED_FOR_MAX_EXECUTOR_FOR_LARGER_QUERIES = 4096;
    private static final int PARTITION_COUNT_FOR_DIRCU_OPTIMIZED_FOR_MIN_EXECUTOR_FOR_LARGER_QUERIES = 4096;
    private static final int PARTITION_COUNT_FOR_LATENCY_OPTIMIZED_FOR_MAX_EXECUTOR_FOR_LARGER_QUERIES = 4096;
    private static final int PARTITION_COUNT_FOR_LATENCY_OPTIMIZED_FOR_MIN_EXECUTOR_FOR_LARGER_QUERIES = 4096;
    private static final int PARTITION_COUNT_FOR_DIRCU_OPTIMIZED_FOR_MAX_EXECUTOR_FOR_SHORTER_QUERIES = 4096;
    private static final int PARTITION_COUNT_FOR_DIRCU_OPTIMIZED_FOR_MIN_EXECUTOR_FOR_SHORTER_QUERIES = 4096;
    private static final int PARTITION_COUNT_FOR_LATENCY_OPTIMIZED_FOR_MAX_EXECUTOR_FOR_SHORTER_QUERIES = 4096;
    private static final int PARTITION_COUNT_FOR_LATENCY_OPTIMIZED_FOR_MIN_EXECUTOR_FOR_SHORTER_QUERIES = 4096;
    // This needs to be extracted from  "mode": "T1_MIGRATION_10G",
    private static final double CONTAINER_SIZE = 10.0;
    private static final double MULTIPLIER_FOR_AUTOMATIC_RESOURCE_MANAGEMENT = 1.3;
    private static final double THRESHOLD_SIZE_FOR_LONGER_QUERIES = 600; // in GB
    private static final int MAX_EXECUTOR_FOR_LARGE_QUERIES_DIRCU_OPTIMIZED = 600;
    private static final int MIN_EXECUTOR_FOR_LARGE_QUERIES_DIRCU_OPTIMIZED = 200;
    private static final int MAX_EXECUTOR_FOR_LARGE_QUERIES_LATENCY_OPTIMIZED = 1000;
    private static final int MIN_EXECUTOR_FOR_LARGE_QUERIES_LATENCY_OPTIMIZED = 400;
    private static final int MAX_EXECUTOR_FOR_SMALL_QUERIES_DIRCU_OPTIMIZED = 200;
    private static final int MIN_EXECUTOR_FOR_SMALL_QUERIES_DIRCU_OPTIMIZED = 50;
    private static final int MAX_EXECUTOR_FOR_SMALL_QUERIES_LATENCY_OPTIMIZED = 400;
    private static final int MIN_EXECUTOR_FOR_SMALL_QUERIES_LATENCY_OPTIMIZED = 200;
    private static final double THRESHOLD_FOR_RESOURCE_OPTIMIZATION = 75;

    private final Metadata metadata;
    private int executorCount;
    private int hashPartitionCount;

    public PhysicalResourceOptimizer(Metadata metadata)
    {
        this.metadata = metadata;
        this.executorCount = 0;
        this.hashPartitionCount = 0;
    }

    public int getExecutorCount()
    {
        return this.executorCount;
    }

    public int getHashPartitionCount()
    {
        return this.hashPartitionCount;
    }

    public boolean isValid()
    {
        return ((this.executorCount > 0) && (this.hashPartitionCount > 0));
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        AbstractMap.SimpleImmutableEntry<Double, Integer> sourceStatistics = new CollectSourceStats(metadata, session).collectSourceStats(plan);

        if (0 != sourceStatistics.getValue()) {
            log.warn(String.format("Source missing statistics, skipping automatic resource tuning."));
            return plan;
        }

        double inputDataInBytes = sourceStatistics.getKey();
        if ((0 >= inputDataInBytes) || (inputDataInBytes > Double.MAX_VALUE) || (Double.isNaN(inputDataInBytes))) {
            log.warn(String.format("Failed to retrieve correct size, data read=%.2f, skipping automatic resource tuning.", inputDataInBytes));
            return plan;
        }

        log.info("From CollectSourceStats Total size in bytes->" + inputDataInBytes);
        double inputDataInGB = inputDataInBytes / 1000000000;
        boolean isCurrentQueryHourPlus = (inputDataInGB > THRESHOLD_SIZE_FOR_LONGER_QUERIES) ? true : false;
        double multiplier = MULTIPLIER_FOR_AUTOMATIC_RESOURCE_MANAGEMENT;

        log.info("From CollectSourceStats Total size in GB->" + inputDataInGB);

        if (inputDataInGB * multiplier < Double.MAX_VALUE) {
            inputDataInGB = inputDataInGB * multiplier;
        }

        boolean isRMDIRCUOptimized = true;
        this.executorCount = calculateExecutorCount(inputDataInGB, session, isCurrentQueryHourPlus, isRMDIRCUOptimized);
        this.hashPartitionCount = calculateHashPartitionCount(executorCount, isCurrentQueryHourPlus, isRMDIRCUOptimized);
        log.info("SOURAV setting the desiredExecutorCount ==> " + executorCount);
        return plan;
    }

    private int calculateHashPartitionCount(int executorCount, boolean isCurrentQueryHourPlus, boolean isRMDIRCUOptimized)
    {
        if (isCurrentQueryHourPlus) {
            return (isRMDIRCUOptimized) ? calculateDircuOptimizedPartitonCountForLongerQueries(executorCount) :
                   calculateLatencyOptimizedPartitionCountForLongerQueries(executorCount);
        }
        else {
            return (isRMDIRCUOptimized) ? calculateDircuOptimizedPartitonCountForShorterQueries(executorCount) :
                    calculateLatencyOptimizedPartitionCountForShorterQueries(executorCount);
        }
    }

    private int calculateLatencyOptimizedPartitionCountForShorterQueries(int executorCount)
    {
        int lower = Math.abs(MIN_EXECUTOR_FOR_SMALL_QUERIES_LATENCY_OPTIMIZED - executorCount);
        int upper = Math.abs(MAX_EXECUTOR_FOR_SMALL_QUERIES_LATENCY_OPTIMIZED - executorCount);

        return ((lower - upper) > 0) ? PARTITION_COUNT_FOR_LATENCY_OPTIMIZED_FOR_MAX_EXECUTOR_FOR_SHORTER_QUERIES :
                PARTITION_COUNT_FOR_LATENCY_OPTIMIZED_FOR_MAX_EXECUTOR_FOR_SHORTER_QUERIES;
    }

    private int calculateDircuOptimizedPartitonCountForShorterQueries(int executorCount)
    {
        int lower = Math.abs(MIN_EXECUTOR_FOR_SMALL_QUERIES_DIRCU_OPTIMIZED - executorCount);
        int upper = Math.abs(MAX_EXECUTOR_FOR_SMALL_QUERIES_DIRCU_OPTIMIZED - executorCount);

        return ((lower - upper) > 0) ? PARTITION_COUNT_FOR_DIRCU_OPTIMIZED_FOR_MAX_EXECUTOR_FOR_SHORTER_QUERIES :
                PARTITION_COUNT_FOR_DIRCU_OPTIMIZED_FOR_MIN_EXECUTOR_FOR_SHORTER_QUERIES;
    }

    private int calculateDircuOptimizedPartitonCountForLongerQueries(int executorCount)
    {
        int lower = Math.abs(MIN_EXECUTOR_FOR_LARGE_QUERIES_DIRCU_OPTIMIZED - executorCount);
        int upper = Math.abs(MAX_EXECUTOR_FOR_LARGE_QUERIES_DIRCU_OPTIMIZED - executorCount);

        return ((lower - upper) > 0) ? PARTITION_COUNT_FOR_DIRCU_OPTIMIZED_FOR_MAX_EXECUTOR_FOR_LARGER_QUERIES :
                PARTITION_COUNT_FOR_DIRCU_OPTIMIZED_FOR_MIN_EXECUTOR_FOR_LARGER_QUERIES;
    }

    private int calculateLatencyOptimizedPartitionCountForLongerQueries(int executorCount)
    {
        int lower = Math.abs(MIN_EXECUTOR_FOR_LARGE_QUERIES_LATENCY_OPTIMIZED - executorCount);
        int upper = Math.abs(MAX_EXECUTOR_FOR_LARGE_QUERIES_LATENCY_OPTIMIZED - executorCount);

        return ((lower - upper) > 0) ? PARTITION_COUNT_FOR_LATENCY_OPTIMIZED_FOR_MAX_EXECUTOR_FOR_LARGER_QUERIES :
                PARTITION_COUNT_FOR_LATENCY_OPTIMIZED_FOR_MIN_EXECUTOR_FOR_LARGER_QUERIES;
    }

    private int calculateExecutorCount(double inputDataInGB, Session session, boolean isCurrentQueryHourPlus, boolean isRMDIRCUOptimized)
    {
        double numberOfExecutors = inputDataInGB / CONTAINER_SIZE;
        //boolean isRMDIRCUOptimized = isResourceManagementDIRCUOptimized(session);

        if (isCurrentQueryHourPlus) {
            return (isRMDIRCUOptimized) ? calculateDircuOptimizedExecutorCountForLongerQueries((int) numberOfExecutors) :
                    calculateLatencyOptimizedExecutorCountForLongerQueries((int) numberOfExecutors);
        }
        else {
            return (isRMDIRCUOptimized) ? calculateDircuOptimizedExecutorCountForShorterQueries((int) numberOfExecutors) :
                    calculateLatencyOptimizedExecutorCountForShorterQueries((int) numberOfExecutors);
        }
    }

    private int calculateLatencyOptimizedExecutorCountForShorterQueries(int numberOfExecutors)
    {
        int updatedExecutorCount = Math.max(numberOfExecutors, MIN_EXECUTOR_FOR_SMALL_QUERIES_LATENCY_OPTIMIZED);
        int finalExecutorCount = Math.max(MIN_EXECUTOR_FOR_SMALL_QUERIES_LATENCY_OPTIMIZED, Math.min(MAX_EXECUTOR_FOR_SMALL_QUERIES_LATENCY_OPTIMIZED, updatedExecutorCount));
        return finalExecutorCount;
    }

    private int calculateDircuOptimizedExecutorCountForShorterQueries(int numberOfExecutors)
    {
        int updatedExecutorCount = Math.max(numberOfExecutors, MIN_EXECUTOR_FOR_SMALL_QUERIES_DIRCU_OPTIMIZED);
        int finalExecutorCount = Math.max(MIN_EXECUTOR_FOR_SMALL_QUERIES_DIRCU_OPTIMIZED, Math.min(MAX_EXECUTOR_FOR_SMALL_QUERIES_DIRCU_OPTIMIZED, updatedExecutorCount));
        return finalExecutorCount;
    }

    private int calculateDircuOptimizedExecutorCountForLongerQueries(int numberOfExecutors)
    {
        int updatedExecutorCount = Math.max(numberOfExecutors, MIN_EXECUTOR_FOR_LARGE_QUERIES_DIRCU_OPTIMIZED);
        int finalExecutorCount = Math.max(MIN_EXECUTOR_FOR_LARGE_QUERIES_DIRCU_OPTIMIZED, Math.min(MAX_EXECUTOR_FOR_LARGE_QUERIES_DIRCU_OPTIMIZED, updatedExecutorCount));
        return finalExecutorCount;
    }

    private int calculateLatencyOptimizedExecutorCountForLongerQueries(int numberOfExecutors)
    {
        int updatedExecutorCount = Math.max(numberOfExecutors, MIN_EXECUTOR_FOR_LARGE_QUERIES_LATENCY_OPTIMIZED);
        int finalExecutorCount = Math.max(MIN_EXECUTOR_FOR_LARGE_QUERIES_LATENCY_OPTIMIZED, Math.min(MAX_EXECUTOR_FOR_LARGE_QUERIES_LATENCY_OPTIMIZED, updatedExecutorCount));
        return finalExecutorCount;
    }
    public class CollectSourceStats
    {
        private final Metadata metadata;
        private final Session session;

        public CollectSourceStats(Metadata metadata, Session session)
        {
            this.metadata = metadata;
            this.session = session;
        }

        public AbstractMap.SimpleImmutableEntry<Double, Integer> collectSourceStats(PlanNode root)
        {
            log.info("Sourav inside collectSourceStats");
            Visitor visitor = new Visitor();
            root.accept(visitor, null);

            AbstractMap.SimpleImmutableEntry<List<TableStatistics>, Integer> sourceStatistics = visitor.getStatsForSources();

            Iterator<TableStatistics> tableStatisticsIterator = sourceStatistics.getKey().iterator();
            Double totalSourceSize = 0.0;
            int totalNumberOfSources = sourceStatistics.getKey().size();
            boolean isNaNPresent = false;
            int numberOfSourceMissingData = sourceStatistics.getValue();
            int numberOfSourcesWithNaNs = 0;
            while (tableStatisticsIterator.hasNext()) {
                TableStatistics ts = tableStatisticsIterator.next();
                log.info("iterating through table stats size -> " + ts.getTotalSize().getValue());
                if (Double.isNaN(ts.getTotalSize().getValue())) {
                    numberOfSourcesWithNaNs++;
                }
                else {
                    totalSourceSize = totalSourceSize + ts.getTotalSize().getValue();
                }
            }
            double failureRate = ((numberOfSourceMissingData + numberOfSourcesWithNaNs) * 100) / (totalNumberOfSources + numberOfSourceMissingData);

            log.info("The failute rate =>" + failureRate);
            if (failureRate > THRESHOLD_FOR_RESOURCE_OPTIMIZATION) {
                if (numberOfSourcesWithNaNs > 0) {
                    totalSourceSize = Double.NaN;
                }
            }
            else {
                numberOfSourceMissingData = 0;
            }

            log.info("In collectSourceStats totalSourceSize -> " + totalSourceSize);
            return new AbstractMap.SimpleImmutableEntry(totalSourceSize, numberOfSourceMissingData);
        }

        private class Visitor
                extends InternalPlanVisitor<Void, Void>
        {
            private final ImmutableList.Builder<TableStatistics> tableStatisticsBuilder = ImmutableList.builder();
            private Integer numberOfSourceMissingData = 0;

            public AbstractMap.SimpleImmutableEntry<List<TableStatistics>, Integer> getStatsForSources()
            {
                return new AbstractMap.SimpleImmutableEntry(tableStatisticsBuilder.build(), numberOfSourceMissingData);
            }

            private Column createColumn(ColumnMetadata columnMetadata)
            {
                return new Column(columnMetadata.getName(), columnMetadata.getType().toString());
            }

            @Override
            public Void visitTableScan(TableScanNode node, Void context)
            {
                TableHandle tableHandle = node.getTable();

                Set<Column> columns = new HashSet<>();
                for (ColumnHandle columnHandle : node.getAssignments().values()) {
                    columns.add(createColumn(metadata.getColumnMetadata(session, tableHandle, columnHandle)));
                }

                List<ColumnHandle> desiredColumns = node.getAssignments().values().stream().collect(toImmutableList());

                Optional<TableStatistics> statistics = Optional.empty();
                Constraint<ColumnHandle> constraint = new Constraint<>(node.getCurrentConstraint());
                statistics = Optional.ofNullable(metadata.getTableStatistics(session, tableHandle, desiredColumns, constraint));

                if (statistics.isPresent()) {
                    log.info(node.toString() + " statistic is present ->" + statistics.get());
                    tableStatisticsBuilder.add(statistics.get());
                }
                else {
                    log.info("statistics unavailable for table-> " + node.toString());
                    numberOfSourceMissingData++;
                }

                return null;
            }
            @Override
            public Void visitPlan(PlanNode node, Void context)
            {
                for (PlanNode source : node.getSources()) {
                    log.info("iterating over source node->" + source.toString());
                    source.accept(this, context);
                }
                return null;
            }
        }
    }
}
