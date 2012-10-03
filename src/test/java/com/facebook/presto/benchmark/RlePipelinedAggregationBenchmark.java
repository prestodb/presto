package com.facebook.presto.benchmark;

import com.facebook.presto.aggregation.DoubleSumAggregation;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.operator.PipelinedAggregationOperator;
import com.facebook.presto.tpch.TpchSchema;

import java.util.List;

public class RlePipelinedAggregationBenchmark
    extends AbstractTupleStreamBenchmark
{
    public RlePipelinedAggregationBenchmark()
    {
        super("pipelined_agg_rle", 5, 20);
    }

    @Override
    protected void setUp()
    {
        loadColumnFile(TpchSchema.Orders.ORDERSTATUS, TupleStreamSerde.Encoding.RLE);
        loadColumnFile(TpchSchema.Orders.TOTALPRICE, TupleStreamSerde.Encoding.RAW);
    }

    @Override
    protected TupleStream createBenchmarkedTupleStream(List<? extends TupleStream> inputTupleStreams)
    {
        return new PipelinedAggregationOperator(
                new GroupByOperator(inputTupleStreams.get(0)),
                inputTupleStreams.get(1),
                DoubleSumAggregation.PROVIDER
        );
    }

    public static void main(String[] args)
    {
        new RlePipelinedAggregationBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
