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
package io.prestosql.benchmark;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.AggregationOperator.AggregationOperatorFactory;
import io.prestosql.operator.FilterAndProjectOperator;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.project.InputChannels;
import io.prestosql.operator.project.PageFilter;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.operator.project.PageProjection;
import io.prestosql.operator.project.SelectedPositions;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.planner.plan.AggregationNode.Step;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.util.DateTimeUtils;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.prestosql.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.relational.Expressions.field;

public class HandTpchQuery6
        extends AbstractSimpleOperatorBenchmark
{
    private final InternalAggregationFunction doubleSum;

    public HandTpchQuery6(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hand_tpch_query_6", 10, 100);

        doubleSum = localQueryRunner.getMetadata().getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("sum", AGGREGATE, DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));
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

        Supplier<PageProjection> projection = new PageFunctionCompiler(localQueryRunner.getMetadata(), 0).compileProjection(field(0, BIGINT), Optional.empty());

        FilterAndProjectOperator.FilterAndProjectOperatorFactory tpchQuery6Operator = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                1,
                new PlanNodeId("test"),
                () -> new PageProcessor(Optional.of(new TpchQuery6Filter()), ImmutableList.of(projection.get())),
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
        public SelectedPositions filter(ConnectorSession session, Page page)
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
