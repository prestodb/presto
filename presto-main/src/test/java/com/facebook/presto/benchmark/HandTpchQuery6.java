package com.facebook.presto.benchmark;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AbstractPageIterator;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.AlignmentOperator;
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
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;

public class HandTpchQuery6
        extends AbstractOperatorBenchmark
{
    public HandTpchQuery6(TpchBlocksProvider tpchBlocksProvider)
    {
        super(tpchBlocksProvider, "hand_tpch_query_6", 10, 100);
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

        AlignmentOperator alignmentOperator = new AlignmentOperator(extendedPrice, discount, shipDate, quantity);
        TpchQuery1Operator tpchQuery1Operator = new TpchQuery1Operator(alignmentOperator);
        return new AggregationOperator(tpchQuery1Operator, Step.SINGLE, ImmutableList.of(aggregation(DOUBLE_SUM, new Input(0, 0))));
    }

    public static class TpchQuery1Operator
            implements Operator
    {
        private final Operator source;
        private final List<TupleInfo> tupleInfos;

        public TpchQuery1Operator(Operator source)
        {
            this.source = source;
            this.tupleInfos = ImmutableList.of(TupleInfo.SINGLE_DOUBLE);
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
                super(ImmutableList.of(TupleInfo.SINGLE_DOUBLE));
                this.pageIterator = pageIterator;
                this.pageBuilder = new PageBuilder(getTupleInfos());
            }

            protected Page computeNext()
            {
                pageBuilder.reset();
                while (!pageBuilder.isFull() && pageIterator.hasNext()) {
                    Page page = pageIterator.next();
                    filterAndProjectRowOriented(pageBuilder, page.getBlock(0), page.getBlock(1), page.getBlock(2), page.getBlock(3));
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

            private static final Slice MIN_SHIP_DATE = Slices.copiedBuffer("1994-01-01", UTF_8);
            private static final Slice MAX_SHIP_DATE = Slices.copiedBuffer("1995-01-01", UTF_8);

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

                    if (extendedPriceCursor.isNull(0) ||
                            discountCursor.isNull(0) ||
                            shipDateCursor.isNull(0) ||
                            quantityCursor.isNull(0)) {
                        continue;
                    }

                    double extendedPrice = extendedPriceCursor.getDouble(0);
                    double discount = discountCursor.getDouble(0);
                    Slice shipDate = shipDateCursor.getSlice(0);
                    double quantity = quantityCursor.getDouble(0);

                    // where shipdate >= '1994-01-01'
                    //    and shipdate < '1995-01-01'
                    //    and discount >= 0.05
                    //    and discount <= 0.07
                    //    and quantity < 24;
                    if (shipDate.compareTo(MIN_SHIP_DATE) >= 0 && shipDate.compareTo(MAX_SHIP_DATE) < 0 && discount >= 0.05 && discount <= 0.07 && quantity < 24) {
                        pageBuilder.getBlockBuilder(0).append(extendedPrice * discount);
                    }
                }

                checkState(!extendedPriceCursor.advanceNextPosition());
                checkState(!discountCursor.advanceNextPosition());
                checkState(!shipDateCursor.advanceNextPosition());
                checkState(!quantityCursor.advanceNextPosition());
            }
        }
    }

    public static void main(String[] args)
    {
        new HandTpchQuery6(DEFAULT_TPCH_BLOCKS_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
