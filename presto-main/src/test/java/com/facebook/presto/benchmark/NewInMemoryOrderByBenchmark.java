package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.LimitOperator;
import com.facebook.presto.operator.NewInMemoryOrderByOperator;
import com.facebook.presto.operator.Operator;
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
        super("duel_in_memory_orderby_1.5M", 5, 5);
        rows = 1_000_000;
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider blocksProvider)
    {
        BlockIterable totalPrice = getBlockIterable(blocksProvider, "orders", "totalprice", BlocksFileEncoding.RAW);
        BlockIterable clerk = getBlockIterable(blocksProvider, "orders", "clerk", BlocksFileEncoding.RAW);
        AlignmentOperator alignmentOperator = new AlignmentOperator(concat(nCopies(100, totalPrice)), concat(nCopies(100, clerk)));

        LimitOperator limitOperator = new LimitOperator(alignmentOperator, rows);
        NewInMemoryOrderByOperator orderByOperator = new NewInMemoryOrderByOperator(limitOperator, 0, 1, rows);
        return orderByOperator;
    }

    public static void main(String[] args)
    {
        new NewInMemoryOrderByBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
