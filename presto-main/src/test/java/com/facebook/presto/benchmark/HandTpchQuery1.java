package com.facebook.presto.benchmark;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.AbstractPageIterator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.List;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.DoubleAverageAggregation.DOUBLE_AVERAGE;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;

public class HandTpchQuery1
        extends AbstractOperatorBenchmark
{
    public HandTpchQuery1(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "hand_tpch_query_1", 1, 5);
    }

    @Override
    protected Operator createBenchmarkedOperator()
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

        BlockIterable returnFlag = getBlockIterable("lineitem", "returnflag", BlocksFileEncoding.RAW);
        BlockIterable lineStatus = getBlockIterable("lineitem", "linestatus", BlocksFileEncoding.RAW);
        BlockIterable quantity = getBlockIterable("lineitem", "quantity", BlocksFileEncoding.RAW);
        BlockIterable extendedPrice = getBlockIterable("lineitem", "extendedprice", BlocksFileEncoding.RAW);
        BlockIterable discount = getBlockIterable("lineitem", "discount", BlocksFileEncoding.RAW);
        BlockIterable tax = getBlockIterable("lineitem", "tax", BlocksFileEncoding.RAW);
        BlockIterable shipDate = getBlockIterable("lineitem", "shipdate", BlocksFileEncoding.RAW);

        AlignmentOperator alignmentOperator = new AlignmentOperator(returnFlag,
                lineStatus,
                quantity,
                extendedPrice,
                discount,
                tax,
                shipDate);

        TpchQuery1Operator tpchQuery1Operator = new TpchQuery1Operator(alignmentOperator);
        return new HashAggregationOperator(tpchQuery1Operator,
                0,
                Step.SINGLE,
                ImmutableList.of(
                        aggregation(DOUBLE_SUM, new Input(1, 0)),
                        aggregation(DOUBLE_SUM, new Input(2, 0)),
                        aggregation(DOUBLE_SUM, new Input(3, 0)),
                        aggregation(DOUBLE_AVERAGE, new Input(1, 0)),
                        aggregation(DOUBLE_AVERAGE, new Input(4, 0)),
                        aggregation(DOUBLE_AVERAGE, new Input(5, 0)),
                        aggregation(COUNT, new Input(1, 0))
                ),
                10_000,
                new TaskMemoryManager(new DataSize(256, Unit.MEGABYTE))
        );
    }

    public static class TpchQuery1Operator
            implements Operator
    {
        private final Operator source;
        private final List<TupleInfo> tupleInfos;

        public TpchQuery1Operator(Operator source)
        {
            this.source = source;
            this.tupleInfos = ImmutableList.of(new TupleInfo(Type.VARIABLE_BINARY, Type.VARIABLE_BINARY),
                    TupleInfo.SINGLE_DOUBLE,
                    TupleInfo.SINGLE_DOUBLE,
                    TupleInfo.SINGLE_DOUBLE,
                    TupleInfo.SINGLE_DOUBLE,
                    TupleInfo.SINGLE_DOUBLE);
        }

        @Override
        public int getChannelCount()
        {
            return 1;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public PageIterator iterator(OperatorStats operatorStats)
        {
            return new TpchQuery1Iterator(source.iterator(operatorStats));
        }

        private static class TpchQuery1Iterator
                extends AbstractPageIterator
        {
            private final PageIterator pageIterator;
            private final PageBuilder pageBuilder;

            public TpchQuery1Iterator(PageIterator pageIterator)
            {
                super(ImmutableList.of(new TupleInfo(Type.VARIABLE_BINARY, Type.VARIABLE_BINARY),
                        TupleInfo.SINGLE_DOUBLE,
                        TupleInfo.SINGLE_DOUBLE,
                        TupleInfo.SINGLE_DOUBLE,
                        TupleInfo.SINGLE_DOUBLE,
                        TupleInfo.SINGLE_DOUBLE));
                this.pageIterator = pageIterator;
                this.pageBuilder = new PageBuilder(getTupleInfos());
            }

            protected Page computeNext()
            {
                pageBuilder.reset();
                while (!pageBuilder.isFull() && pageIterator.hasNext()) {
                    Page page = pageIterator.next();
                    filterAndProjectRowOriented(pageBuilder,
                            page.getBlock(0),
                            page.getBlock(1),
                            page.getBlock(2),
                            page.getBlock(3),
                            page.getBlock(4),
                            page.getBlock(5),
                            page.getBlock(6));
                }

                if (pageBuilder.isEmpty()) {
                    return endOfData();
                }

                Page page = pageBuilder.build();
                return page;
            }

            @Override
            protected void doClose()
            {
                pageIterator.close();
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

                    if (shipDateCursor.isNull(0)) {
                        continue;
                    }

                    Slice shipDate = shipDateCursor.getSlice(0);

                    // where
                    //     shipdate <= '1998-09-02'
                    if (shipDate.compareTo(MAX_SHIP_DATE) <= 0) {
                        //     returnflag, linestatus
                        //     quantity
                        //     extendedprice
                        //     extendedprice * (1 - discount)
                        //     extendedprice * (1 - discount) * (1 + tax)
                        //     discount

                        if (returnFlagCursor.isNull(0)) {
                            pageBuilder.getBlockBuilder(0).appendNull();
                        } else {
                            pageBuilder.getBlockBuilder(0).append(returnFlagCursor.getSlice(0));
                        }
                        if (lineStatusCursor.isNull(0)) {
                            pageBuilder.getBlockBuilder(0).appendNull();
                        } else {
                            pageBuilder.getBlockBuilder(0).append(lineStatusCursor.getSlice(0));
                        }

                        double quantity = quantityCursor.getDouble(0);
                        double extendedPrice = extendedPriceCursor.getDouble(0);
                        double discount = discountCursor.getDouble(0);
                        double tax = taxCursor.getDouble(0);

                        boolean quantityIsNull = quantityCursor.isNull(0);
                        boolean extendedPriceIsNull = extendedPriceCursor.isNull(0);
                        boolean discountIsNull = discountCursor.isNull(0);
                        boolean taxIsNull = taxCursor.isNull(0);

                        if (quantityIsNull) {
                            pageBuilder.getBlockBuilder(1).appendNull();
                        }
                        else {
                            pageBuilder.getBlockBuilder(1).append(quantity);
                        }

                        if (extendedPriceIsNull) {
                            pageBuilder.getBlockBuilder(2).appendNull();
                        }
                        else {
                            pageBuilder.getBlockBuilder(2).append(extendedPrice);
                        }

                        if (extendedPriceIsNull || discountIsNull) {
                            pageBuilder.getBlockBuilder(3).appendNull();
                        } else {
                            pageBuilder.getBlockBuilder(3).append(extendedPrice * (1 - discount));
                        }

                        if (extendedPriceIsNull || discountIsNull || taxIsNull) {
                            pageBuilder.getBlockBuilder(4).appendNull();
                        }
                        else {
                            pageBuilder.getBlockBuilder(4).append(extendedPrice * (1 - discount) * (1 + tax));
                        }

                        if (discountIsNull) {
                            pageBuilder.getBlockBuilder(5).appendNull();
                        }
                        else {
                            pageBuilder.getBlockBuilder(5).append(discount);
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
    }

    public static void main(String[] args)
    {
        new HandTpchQuery1(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
