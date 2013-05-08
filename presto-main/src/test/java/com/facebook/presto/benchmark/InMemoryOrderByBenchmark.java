package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.InMemoryOrderByOperator;
import com.facebook.presto.operator.LimitOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import io.airlift.units.DataSize;

import static com.facebook.presto.block.BlockIterables.concat;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Collections.nCopies;

public class InMemoryOrderByBenchmark
    extends AbstractOperatorBenchmark
{
    private static final int ROWS = 1_500_000;

    public InMemoryOrderByBenchmark()
    {
        super("in_memory_orderby_1.5M", 5, 10);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        BlockIterable totalPrice = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);
        BlockIterable clerk = getBlockIterable("orders", "clerk", BlocksFileEncoding.RAW);
        AlignmentOperator alignmentOperator = new AlignmentOperator(concat(nCopies(100, totalPrice)), concat(nCopies(100, clerk)));

        LimitOperator limitOperator = new LimitOperator(alignmentOperator, ROWS);
        InMemoryOrderByOperator orderByOperator = new InMemoryOrderByOperator(limitOperator, 0, new int[]{1}, ROWS, new DataSize(256, MEGABYTE));
        return orderByOperator;
    }

    public static void main(String[] args)
    {
        new InMemoryOrderByBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
