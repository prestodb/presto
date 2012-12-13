package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.LimitOperator;
import com.facebook.presto.operator.NewInMemoryOrderByOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;

import static com.facebook.presto.block.BlockIterables.concat;
import static java.util.Collections.nCopies;

public class NewInMemoryOrderByBenchmark
    extends AbstractOperatorBenchmark
{
    private final int rows;

    public NewInMemoryOrderByBenchmark()
    {
        super("duel_in_memory_orderby_1.5M", 5, 10);
        rows = 1_000_000;
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider blocksProvider)
    {
        BlockIterable totalPrice = getBlockIterable(blocksProvider, "orders", "totalprice", BlocksFileEncoding.RAW);
        BlockIterable clerk = getBlockIterable(blocksProvider, "orders", "clerk", BlocksFileEncoding.RAW);
        AlignmentOperator alignmentOperator = new AlignmentOperator(concat(nCopies(100, totalPrice)), concat(nCopies(100, clerk)));

        LimitOperator limitOperator = new LimitOperator(alignmentOperator, rows);
        NewInMemoryOrderByOperator orderByOperator = new NewInMemoryOrderByOperator(limitOperator, 0, new int[]{1}, rows);
        return orderByOperator;
    }

    @Override
    protected long execute(TpchBlocksProvider blocksProvider)
    {
        Operator operator = createBenchmarkedOperator(blocksProvider);

        long outputRows = 0;
        PageIterator iterator = operator.iterator(new OperatorStats());
        while (iterator.hasNext()) {
            Page page = iterator.next();
            BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                outputRows++;
            }
        }
        return outputRows;
    }

    public static void main(String[] args)
    {
        new NewInMemoryOrderByBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
