package com.facebook.presto.benchmark;

import com.facebook.presto.block.TupleStream;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.tpch.TpchSchema;

import java.util.List;

import static com.facebook.presto.block.StatsCollectingTupleStreamSerde.Encoding;

public class DicRleGroupByBenchmark
        extends AbstractTupleStreamBenchmark
{
    public DicRleGroupByBenchmark()
    {
        super("groupby_dic_rle", 10, 50);
    }


    @Override
    protected void setUp()
    {
        loadColumnFile(TpchSchema.Orders.ORDERSTATUS, Encoding.DICTIONARY_RLE);
    }

    @Override
    protected TupleStream createBenchmarkedTupleStream(List<? extends TupleStream> inputTupleStreams)
    {
        return new GroupByOperator(inputTupleStreams.get(0));
    }

    public static void main(String[] args)
    {
        new DicRleGroupByBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
