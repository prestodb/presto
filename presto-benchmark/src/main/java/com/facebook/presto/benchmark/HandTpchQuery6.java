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
package com.facebook.presto.benchmark;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.AggregationOperator.AggregationOperatorFactory;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.project.InputChannels;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.project.PageProjectionWithOutputs;
import com.facebook.presto.operator.project.SelectedPositions;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.util.DateTimeUtils;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.BYTE;

public class HandTpchQuery6
        extends AbstractSimpleOperatorBenchmark
{
    private final InternalAggregationFunction doubleSum;

    public HandTpchQuery6(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hand_tpch_query_6", 10, 100);
        FunctionAndTypeManager functionAndTypeManager = localQueryRunner.getMetadata().getFunctionAndTypeManager();
        doubleSum = functionAndTypeManager.getAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction("sum", fromTypes(DOUBLE)));
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        // select sum(extendedprice * discount) as revenue
        // from lineitem
        // where shipdate >= '1994-01-01'
        //    and shipdate < '1995-01-01'
        //    and discount >= 0.05
        //    and discount <= 0.07
        //    and quantity < 24;
        OperatorFactory tableScanOperator = createTableScanOperator(0, new PlanNodeId("test"), "lineitem", "extendedprice", "discount", "shipdate", "quantity");

        List<Supplier<PageProjectionWithOutputs>> projection = new PageFunctionCompiler(localQueryRunner.getMetadata(), 0)
                .compileProjections(
                        session.getSqlFunctionProperties(),
                        session.getSessionFunctions(),
                        ImmutableList.of(field(0, BIGINT)),
                        false,
                        Optional.empty());

        FilterAndProjectOperator.FilterAndProjectOperatorFactory tpchQuery6Operator = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                1,
                new PlanNodeId("test"),
                () -> new PageProcessor(Optional.of(new TpchQuery6Filter()), projection.stream().map(Supplier::get).collect(toImmutableList())),
                ImmutableList.of(DOUBLE),
                new DataSize(0, BYTE),
                0);

        AggregationOperatorFactory aggregationOperator = new AggregationOperatorFactory(
                2,
                new PlanNodeId("test"),
                Step.SINGLE,
                ImmutableList.of(
                        doubleSum.bind(ImmutableList.of(0), Optional.empty())),
                false);

        return ImmutableList.of(tableScanOperator, tpchQuery6Operator, aggregationOperator);
    }

    public static class TpchQuery6Filter
            implements PageFilter
    {
        private static final int MIN_SHIP_DATE = DateTimeUtils.parseDate("1994-01-01");
        private static final int MAX_SHIP_DATE = DateTimeUtils.parseDate("1995-01-01");
        private static final InputChannels INPUT_CHANNELS = new InputChannels(1, 2, 3);

        private boolean[] selectedPositions = new boolean[0];

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public InputChannels getInputChannels()
        {
            return INPUT_CHANNELS;
        }

        @Override
        public SelectedPositions filter(SqlFunctionProperties properties, Page page)
        {
            if (selectedPositions.length < page.getPositionCount()) {
                selectedPositions = new boolean[page.getPositionCount()];
            }

            for (int position = 0; position < page.getPositionCount(); position++) {
                selectedPositions[position] = filter(page, position);
            }

            return PageFilter.positionsArrayToSelectedPositions(selectedPositions, page.getPositionCount());
        }

        private static boolean filter(Page page, int position)
        {
            Block discountBlock = page.getBlock(0);
            Block shipDateBlock = page.getBlock(1);
            Block quantityBlock = page.getBlock(2);
            return !shipDateBlock.isNull(position) && DATE.getLong(shipDateBlock, position) >= MIN_SHIP_DATE &&
                    !shipDateBlock.isNull(position) && DATE.getLong(shipDateBlock, position) < MAX_SHIP_DATE &&
                    !discountBlock.isNull(position) && DOUBLE.getDouble(discountBlock, position) >= 0.05 &&
                    !discountBlock.isNull(position) && DOUBLE.getDouble(discountBlock, position) <= 0.07 &&
                    !quantityBlock.isNull(position) && BIGINT.getLong(quantityBlock, position) < 24;
        }
    }

    public static void main(String[] args)
    {
        new HandTpchQuery6(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
