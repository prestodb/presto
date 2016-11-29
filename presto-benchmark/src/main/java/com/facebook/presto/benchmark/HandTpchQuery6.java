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

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.AggregationOperator.AggregationOperatorFactory;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.util.DateTimeUtils;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

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

        FilterAndProjectOperator.FilterAndProjectOperatorFactory tpchQuery6Operator = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(1, new PlanNodeId("test"), () -> new TpchQuery6Processor(), ImmutableList.of(DOUBLE));

        AggregationOperatorFactory aggregationOperator = new AggregationOperatorFactory(
                2,
                new PlanNodeId("test"),
                Step.SINGLE,
                ImmutableList.of(
                        doubleSum.bind(ImmutableList.of(0), Optional.empty())
                ));

        return ImmutableList.of(tableScanOperator, tpchQuery6Operator, aggregationOperator);
    }

    public static class TpchQuery6Processor
            implements PageProcessor
    {
        private static final int MIN_SHIP_DATE = DateTimeUtils.parseDate("1994-01-01");
        private static final int MAX_SHIP_DATE = DateTimeUtils.parseDate("1995-01-01");

        @Override
        public int process(ConnectorSession session, Page page, int start, int end, PageBuilder pageBuilder)
        {
            Block discountBlock = page.getBlock(1);
            int position = start;
            for (; position < end; position++) {
                // where shipdate >= '1994-01-01'
                //    and shipdate < '1995-01-01'
                //    and discount >= 0.05
                //    and discount <= 0.07
                //    and quantity < 24;
                if (filter(position, discountBlock, page.getBlock(2), page.getBlock(3))) {
                    project(position, pageBuilder, page.getBlock(0), discountBlock);
                }
            }
            return position;
        }

        @Override
        public Page processColumnar(ConnectorSession session, Page page, List<? extends Type> types)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page processColumnarDictionary(ConnectorSession session, Page page, List<? extends Type> types)
        {
            throw new UnsupportedOperationException();
        }

        private static void project(int position, PageBuilder pageBuilder, Block extendedPriceBlock, Block discountBlock)
        {
            if (discountBlock.isNull(position) || extendedPriceBlock.isNull(position)) {
                pageBuilder.getBlockBuilder(0).appendNull();
            }
            else {
                DOUBLE.writeDouble(pageBuilder.getBlockBuilder(0), DOUBLE.getDouble(extendedPriceBlock, position) * DOUBLE.getDouble(discountBlock, position));
            }
        }

        private static boolean filter(int position, Block discountBlock, Block shipDateBlock, Block quantityBlock)
        {
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
