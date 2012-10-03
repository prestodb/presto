package com.facebook.presto.benchmark;

import com.facebook.presto.aggregation.CountAggregation;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.tpch.TpchSchema;

import java.util.List;

public class CountAggregationBenchmark
    extends AbstractTupleStreamBenchmark
{
    public CountAggregationBenchmark()
    {
        super("count_agg", 10, 100);
    }

    @Override
    protected void setUp()
    {
        loadColumnFile(TpchSchema.Orders.ORDERKEY, TupleStreamSerde.Encoding.RAW);
    }

    @Override
    protected TupleStream createBenchmarkedTupleStream(List<? extends TupleStream> inputTupleStreams)
    {
        return new AggregationOperator(inputTupleStreams.get(0), CountAggregation.PROVIDER);
    }

    public static void main(String[] args)
    {
        new CountAggregationBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
