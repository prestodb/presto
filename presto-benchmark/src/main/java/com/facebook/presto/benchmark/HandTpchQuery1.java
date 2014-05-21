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

import com.facebook.presto.benchmark.HandTpchQuery1.TpchQuery1Operator.TpchQuery1OperatorFactory;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.AverageAggregations.DOUBLE_AVERAGE;
import static com.facebook.presto.operator.aggregation.AverageAggregations.LONG_AVERAGE;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.LONG_SUM;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class HandTpchQuery1
        extends AbstractSimpleOperatorBenchmark
{
    public HandTpchQuery1(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hand_tpch_query_1", 1, 5);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        // select
        //     returnflag,
        //     linestatus,
        //     sum(quantity) as sum_qty,
        //     sum(extendedprice) as sum_base_price,
        //     sum(extendedprice * (1 - discount)) as sum_disc_price,
        //     sum(extendedprice * (1 - discount) * (1 + tax)) as sum_charge,
        //     avg(quantity) as avg_qty,
        //     avg(extendedprice) as avg_price,
        //     avg(discount) as avg_disc,
        //     count(*) as count_order
        // from
        //     lineitem
        // where
        //     shipdate <= '1998-09-02'
        // group by
        //     returnflag,
        //     linestatus
        // order by
        //     returnflag,
        //     linestatus

        OperatorFactory tableScanOperator = createTableScanOperator(
                0,
                "lineitem",
                "returnflag",
                "linestatus",
                "quantity",
                "extendedprice",
                "discount",
                "tax",
                "shipdate");

        TpchQuery1OperatorFactory tpchQuery1Operator = new TpchQuery1OperatorFactory(1);
        HashAggregationOperatorFactory aggregationOperator = new HashAggregationOperatorFactory(
                2,
                ImmutableList.of(tpchQuery1Operator.getTypes().get(0), tpchQuery1Operator.getTypes().get(1)),
                Ints.asList(0, 1),
                Step.SINGLE,
                ImmutableList.of(
                        aggregation(LONG_SUM, ImmutableList.of(new Input(2)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(DOUBLE_SUM, ImmutableList.of(new Input(3)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(DOUBLE_SUM, ImmutableList.of(new Input(4)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(LONG_AVERAGE, ImmutableList.of(new Input(2)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(DOUBLE_AVERAGE, ImmutableList.of(new Input(5)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(DOUBLE_AVERAGE, ImmutableList.of(new Input(6)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(COUNT, ImmutableList.of(new Input(2)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0)
                        ),
                10_000);

        return ImmutableList.of(tableScanOperator, tpchQuery1Operator, aggregationOperator);
    }

    public static class TpchQuery1Operator
            implements com.facebook.presto.operator.Operator
    {
        private static final ImmutableList<Type> TYPES = ImmutableList.of(
                VARCHAR,
                VARCHAR,
                BIGINT,
                DOUBLE,
                DOUBLE,
                DOUBLE,
                DOUBLE);

        public static class TpchQuery1OperatorFactory
                implements OperatorFactory
        {
            private final int operatorId;

            public TpchQuery1OperatorFactory(int operatorId)
            {
                this.operatorId = operatorId;
            }

            @Override
            public List<Type> getTypes()
            {
                return TYPES;
            }

            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, TpchQuery1Operator.class.getSimpleName());
                return new TpchQuery1Operator(operatorContext);
            }

            @Override
            public void close()
            {
            }
        }

        private final OperatorContext operatorContext;
        private final PageBuilder pageBuilder;
        private boolean finishing;

        public TpchQuery1Operator(OperatorContext operatorContext)
        {
            this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
            this.pageBuilder = new PageBuilder(TYPES);
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public List<Type> getTypes()
        {
            return TYPES;
        }

        @Override
        public void finish()
        {
            finishing = true;
        }

        @Override
        public boolean isFinished()
        {
            return finishing && pageBuilder.isEmpty();
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            return NOT_BLOCKED;
        }

        @Override
        public boolean needsInput()
        {
            return !pageBuilder.isFull();
        }

        @Override
        public void addInput(Page page)
        {
            checkNotNull(page, "page is null");
            checkState(!pageBuilder.isFull(), "Output buffer is full");
            checkState(!finishing, "Operator is finished");

            filterAndProjectRowOriented(pageBuilder,
                    page.getBlock(0),
                    page.getBlock(1),
                    page.getBlock(2),
                    page.getBlock(3),
                    page.getBlock(4),
                    page.getBlock(5),
                    page.getBlock(6));
        }

        @Override
        public Page getOutput()
        {
            // only return a page if the page buffer isFull or we are finishing and the page buffer has data
            if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty())) {
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return page;
            }
            return null;
        }

        private static final Slice MAX_SHIP_DATE = Slices.copiedBuffer("1998-09-02", UTF_8);

        private void filterAndProjectRowOriented(PageBuilder pageBuilder,
                Block returnFlagBlock,
                Block lineStatusBlock,
                Block quantityBlock,
                Block extendedPriceBlock,
                Block discountBlock,
                Block taxBlock,
                Block shipDateBlock)
        {
            int rows = returnFlagBlock.getPositionCount();

            BlockCursor returnFlagCursor = returnFlagBlock.cursor();
            BlockCursor lineStatusCursor = lineStatusBlock.cursor();
            BlockCursor quantityCursor = quantityBlock.cursor();
            BlockCursor extendedPriceCursor = extendedPriceBlock.cursor();
            BlockCursor discountCursor = discountBlock.cursor();
            BlockCursor taxCursor = taxBlock.cursor();
            BlockCursor shipDateCursor = shipDateBlock.cursor();

            for (int position = 0; position < rows; position++) {
                checkState(returnFlagCursor.advanceNextPosition());
                checkState(lineStatusCursor.advanceNextPosition());
                checkState(quantityCursor.advanceNextPosition());
                checkState(extendedPriceCursor.advanceNextPosition());
                checkState(discountCursor.advanceNextPosition());
                checkState(taxCursor.advanceNextPosition());
                checkState(shipDateCursor.advanceNextPosition());

                if (shipDateCursor.isNull()) {
                    continue;
                }

                Slice shipDate = shipDateCursor.getSlice();

                // where
                //     shipdate <= '1998-09-02'
                if (shipDate.compareTo(MAX_SHIP_DATE) <= 0) {
                    //     returnflag,
                    //     linestatus
                    //     quantity
                    //     extendedprice
                    //     extendedprice * (1 - discount)
                    //     extendedprice * (1 - discount) * (1 + tax)
                    //     discount

                    if (returnFlagCursor.isNull()) {
                        pageBuilder.getBlockBuilder(0).appendNull();
                    }
                    else {
                        pageBuilder.getBlockBuilder(0).appendSlice(returnFlagCursor.getSlice());
                    }
                    if (lineStatusCursor.isNull()) {
                        pageBuilder.getBlockBuilder(1).appendNull();
                    }
                    else {
                        pageBuilder.getBlockBuilder(1).appendSlice(lineStatusCursor.getSlice());
                    }

                    long quantity = quantityCursor.getLong();
                    double extendedPrice = extendedPriceCursor.getDouble();
                    double discount = discountCursor.getDouble();
                    double tax = taxCursor.getDouble();

                    boolean quantityIsNull = quantityCursor.isNull();
                    boolean extendedPriceIsNull = extendedPriceCursor.isNull();
                    boolean discountIsNull = discountCursor.isNull();
                    boolean taxIsNull = taxCursor.isNull();

                    if (quantityIsNull) {
                        pageBuilder.getBlockBuilder(2).appendNull();
                    }
                    else {
                        pageBuilder.getBlockBuilder(2).appendLong(quantity);
                    }

                    if (extendedPriceIsNull) {
                        pageBuilder.getBlockBuilder(3).appendNull();
                    }
                    else {
                        pageBuilder.getBlockBuilder(3).appendDouble(extendedPrice);
                    }

                    if (extendedPriceIsNull || discountIsNull) {
                        pageBuilder.getBlockBuilder(4).appendNull();
                    }
                    else {
                        pageBuilder.getBlockBuilder(4).appendDouble(extendedPrice * (1 - discount));
                    }

                    if (extendedPriceIsNull || discountIsNull || taxIsNull) {
                        pageBuilder.getBlockBuilder(5).appendNull();
                    }
                    else {
                        pageBuilder.getBlockBuilder(5).appendDouble(extendedPrice * (1 - discount) * (1 + tax));
                    }

                    if (discountIsNull) {
                        pageBuilder.getBlockBuilder(6).appendNull();
                    }
                    else {
                        pageBuilder.getBlockBuilder(6).appendDouble(discount);
                    }
                }
            }

            checkState(!returnFlagCursor.advanceNextPosition());
            checkState(!lineStatusCursor.advanceNextPosition());
            checkState(!quantityCursor.advanceNextPosition());
            checkState(!extendedPriceCursor.advanceNextPosition());
            checkState(!discountCursor.advanceNextPosition());
            checkState(!taxCursor.advanceNextPosition());
            checkState(!shipDateCursor.advanceNextPosition());
        }
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new HandTpchQuery1(createLocalQueryRunner(executor)).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
