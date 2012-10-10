package com.facebook.presto.benchmark;

import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.operator.ApplyPredicateOperator;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tpch.TpchTupleStreamProvider;

import java.util.List;

public class DicRleGroupByBenchmark
        extends AbstractTupleStreamBenchmark
{
    public DicRleGroupByBenchmark()
    {
        super("groupby_dic_rle", 10, 50);
    }

    @Override
    protected TupleStream createBenchmarkedTupleStream(TpchTupleStreamProvider inputStreamProvider)
    {
        return new GroupByOperator(
                inputStreamProvider.getTupleStream(TpchSchema.Orders.ORDERSTATUS, TupleStreamSerdes.Encoding.DICTIONARY_RLE)
        );
    }

    public static void main(String[] args)
    {
        new DicRleGroupByBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
