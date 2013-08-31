package com.facebook.presto.benchmark;

import com.facebook.presto.benchmark.NewHandTpchQuery6.TpchQuery6Operator.TpchQuery6OperatorFactory;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.noperator.DriverContext;
import com.facebook.presto.noperator.DriverOperator;
import com.facebook.presto.noperator.NewAggregationOperator.NewAggregationOperatorFactory;
import com.facebook.presto.noperator.NewAlignmentOperator.NewAlignmentOperatorFactory;
import com.facebook.presto.noperator.NewOperator;
import com.facebook.presto.noperator.NewOperatorFactory;
import com.facebook.presto.noperator.OperatorContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class NewHandTpchQuery6
        extends AbstractOperatorBenchmark
{
    public NewHandTpchQuery6(ExecutorService executor, TpchBlocksProvider tpchBlocksProvider)
    {
        super(executor, tpchBlocksProvider, "hand_tpch_query_6", 10, 100);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        // select sum(extendedprice * discount) as revenue
        // from lineitem
        // where shipdate >= '1994-01-01'
        //    and shipdate < '1995-01-01'
        //    and discount >= 0.05
        //    and discount <= 0.07
        //    and quantity < 24;

        BlockIterable extendedPrice = getBlockIterable("lineitem", "extendedprice", BlocksFileEncoding.RAW);
        BlockIterable discount = getBlockIterable("lineitem", "discount", BlocksFileEncoding.RAW);
        BlockIterable shipDate = getBlockIterable("lineitem", "shipdate", BlocksFileEncoding.RAW);
        BlockIterable quantity = getBlockIterable("lineitem", "quantity", BlocksFileEncoding.RAW);

        NewAlignmentOperatorFactory alignmentOperator = new NewAlignmentOperatorFactory(0, extendedPrice, discount, shipDate, quantity);

        TpchQuery6OperatorFactory tpchQuery6Operator = new TpchQuery6OperatorFactory(1);

        NewAggregationOperatorFactory aggregationOperator = new NewAggregationOperatorFactory(
                2,
                Step.SINGLE,
                ImmutableList.of(
                        aggregation(DOUBLE_SUM, new Input(0, 0))
                ));

        return new DriverOperator(alignmentOperator, tpchQuery6Operator, aggregationOperator);
    }

    public static class TpchQuery6Operator
            extends com.facebook.presto.noperator.NewAbstractFilterAndProjectOperator
    {
        public static class TpchQuery6OperatorFactory
                implements NewOperatorFactory
        {
            private final int operatorId;

            public TpchQuery6OperatorFactory(int operatorId)
            {
                this.operatorId = operatorId;
            }

            @Override
            public List<TupleInfo> getTupleInfos()
            {
                return ImmutableList.of(TupleInfo.SINGLE_DOUBLE);
            }

            @Override
            public NewOperator createOperator(DriverContext driverContext)
            {
                OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, TpchQuery6Operator.class.getSimpleName());
                return new TpchQuery6Operator(operatorContext);
            }

            @Override
            public void close()
            {
            }
        }

        private static final Slice MIN_SHIP_DATE = Slices.copiedBuffer("1994-01-01", UTF_8);
        private static final Slice MAX_SHIP_DATE = Slices.copiedBuffer("1995-01-01", UTF_8);

        public TpchQuery6Operator(OperatorContext operatorContext)
        {
            super(operatorContext, ImmutableList.of(TupleInfo.SINGLE_DOUBLE));
        }

        @Override
        protected void filterAndProjectRowOriented(Block[] blocks, PageBuilder pageBuilder)
        {
            filterAndProjectRowOriented(pageBuilder, blocks[0], blocks[1], blocks[2], blocks[3]);
        }

        private void filterAndProjectRowOriented(PageBuilder pageBuilder, Block extendedPriceBlock, Block discountBlock, Block shipDateBlock, Block quantityBlock)
        {
            int rows = extendedPriceBlock.getPositionCount();

            BlockCursor extendedPriceCursor = extendedPriceBlock.cursor();
            BlockCursor discountCursor = discountBlock.cursor();
            BlockCursor shipDateCursor = shipDateBlock.cursor();
            BlockCursor quantityCursor = quantityBlock.cursor();

            for (int position = 0; position < rows; position++) {
                checkState(extendedPriceCursor.advanceNextPosition());
                checkState(discountCursor.advanceNextPosition());
                checkState(shipDateCursor.advanceNextPosition());
                checkState(quantityCursor.advanceNextPosition());

                // where shipdate >= '1994-01-01'
                //    and shipdate < '1995-01-01'
                //    and discount >= 0.05
                //    and discount <= 0.07
                //    and quantity < 24;
                if (filter(discountCursor, shipDateCursor, quantityCursor)) {
                    project(pageBuilder, extendedPriceCursor, discountCursor);
                }
            }

            checkState(!extendedPriceCursor.advanceNextPosition());
            checkState(!discountCursor.advanceNextPosition());
            checkState(!shipDateCursor.advanceNextPosition());
            checkState(!quantityCursor.advanceNextPosition());
        }

        private void project(PageBuilder pageBuilder, BlockCursor extendedPriceCursor, BlockCursor discountCursor)
        {
            if (discountCursor.isNull(0) || extendedPriceCursor.isNull(0)) {
                pageBuilder.getBlockBuilder(0).appendNull();
            }
            else {
                pageBuilder.getBlockBuilder(0).append(extendedPriceCursor.getDouble(0) * discountCursor.getDouble(0));
            }
        }

        private boolean filter(BlockCursor discountCursor, BlockCursor shipDateCursor, BlockCursor quantityCursor)
        {
            return !shipDateCursor.isNull(0) && shipDateCursor.getSlice(0).compareTo(MIN_SHIP_DATE) >= 0 &&
                    !shipDateCursor.isNull(0) && shipDateCursor.getSlice(0).compareTo(MAX_SHIP_DATE) < 0 &&
                    !discountCursor.isNull(0) && discountCursor.getDouble(0) >= 0.05 &&
                    !discountCursor.isNull(0) && discountCursor.getDouble(0) <= 0.07 &&
                    !quantityCursor.isNull(0) && quantityCursor.getDouble(0) < 24;
        }
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new NewHandTpchQuery6(executor, DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
