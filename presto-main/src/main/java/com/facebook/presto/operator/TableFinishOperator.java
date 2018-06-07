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
package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.StatisticAggregationsDescriptor;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TableFinishOperator
        implements Operator
{
    public static final List<Type> TYPES = ImmutableList.of(BIGINT);

    public static class TableFinishOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final TableFinisher tableFinisher;
        private final OperatorFactory statisticsAggregation;
        private final StatisticAggregationsDescriptor<Integer> descriptor;
        private boolean closed;

        public TableFinishOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                TableFinisher tableFinisher,
                OperatorFactory statisticsAggregation,
                StatisticAggregationsDescriptor<Integer> descriptor)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.tableFinisher = requireNonNull(tableFinisher, "tableCommitter is null");
            this.statisticsAggregation = requireNonNull(statisticsAggregation, "statisticsAggregation is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableFinishOperator.class.getSimpleName());
            return new TableFinishOperator(context, tableFinisher, statisticsAggregation.createOperator(driverContext), descriptor);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TableFinishOperatorFactory(operatorId, planNodeId, tableFinisher, statisticsAggregation, descriptor);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final TableFinisher tableFinisher;
    private final Operator statisticsAggregation;
    private final StatisticAggregationsDescriptor<Integer> descriptor;

    private State state = State.RUNNING;
    private long rowCount;
    private boolean closed;
    private Optional<ConnectorOutputMetadata> outputMetadata = Optional.empty();
    private final ImmutableList.Builder<Slice> fragmentBuilder = ImmutableList.builder();

    public TableFinishOperator(
            OperatorContext operatorContext,
            TableFinisher tableFinisher,
            Operator statisticsAggregation,
            StatisticAggregationsDescriptor<Integer> descriptor)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.tableFinisher = requireNonNull(tableFinisher, "tableCommitter is null");
        this.statisticsAggregation = requireNonNull(statisticsAggregation, "statisticsAggregation is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");

        operatorContext.setInfoSupplier(() -> new TableFinishInfo(outputMetadata));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (state == State.RUNNING) {
            state = State.FINISHING;
            statisticsAggregation.finish();
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return statisticsAggregation.isBlocked();
    }

    @Override
    public boolean needsInput()
    {
        return state == State.RUNNING && statisticsAggregation.needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(state == State.RUNNING, "Operator is %s", state);

        if (isStatisticsPage(page)) {
            statisticsAggregation.addInput(page);
        }
        else {
            processFragmentPage(page);
        }
    }

    private boolean isStatisticsPage(Page page)
    {
        return page.getPositionCount() > 0 && page.getBlock(0).isNull(0);
    }

    private void processFragmentPage(Page page)
    {
        Block rowCountBlock = page.getBlock(0);
        Block fragmentBlock = page.getBlock(1);
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (!rowCountBlock.isNull(position)) {
                rowCount += BIGINT.getLong(rowCountBlock, position);
            }
            if (!fragmentBlock.isNull(position)) {
                fragmentBuilder.add(VARBINARY.getSlice(fragmentBlock, position));
            }
        }
    }

    @Override
    public Page getOutput()
    {
        if (!isBlocked().isDone()) {
            return null;
        }

        if (state != State.FINISHING) {
            return null;
        }
        state = State.FINISHED;

        List<ComputedStatistics> statistics = getComputedStatistics();
        System.out.println(statistics);
        outputMetadata = tableFinisher.finishTable(fragmentBuilder.build());

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder page = new PageBuilder(1, TYPES);
        page.declarePosition();
        BIGINT.writeLong(page.getBlockBuilder(0), rowCount);
        return page.build();
    }

    private List<ComputedStatistics> getComputedStatistics()
    {
        ImmutableList.Builder<ComputedStatistics> statistics = ImmutableList.builder();
        while (!statisticsAggregation.isFinished()) {
            Page page = statisticsAggregation.getOutput();
            if (page == null) {
                continue;
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                statistics.add(getComputedStatistics(page, position));
            }
        }
        return statistics.build();
    }

    private ComputedStatistics getComputedStatistics(Page page, int position)
    {
        ImmutableList.Builder<String> groupingColumns = ImmutableList.builder();
        ImmutableList.Builder<Block> groupingValues = ImmutableList.builder();
        descriptor.getGrouping().forEach((channel, column) -> {
            groupingColumns.add(column);
            groupingValues.add(page.getBlock(channel).getSingleValueBlock(position));
        });

        ComputedStatistics.Builder statistics = ComputedStatistics.builder(groupingColumns.build(), groupingValues.build());

        descriptor.getTableStatistics().forEach((channel, type) -> statistics.addTableStatistic(type, page.getBlock(channel).getSingleValueBlock(position)));

        descriptor.getColumnStatistics().forEach((channel, metadata) -> statistics.addColumnStatistic(metadata, page.getBlock(channel).getSingleValueBlock(position)));

        return statistics.build();
    }

    @Override
    public void close()
            throws Exception
    {
        if (!closed) {
            closed = true;
            statisticsAggregation.close();
        }
    }

    public interface TableFinisher
    {
        Optional<ConnectorOutputMetadata> finishTable(Collection<Slice> fragments);
    }
}
