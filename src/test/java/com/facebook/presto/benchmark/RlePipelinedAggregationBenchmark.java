package com.facebook.presto.benchmark;

import com.facebook.presto.aggregation.DoubleSumAggregation;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.PipelinedAggregationOperator;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tpch.TpchTupleStreamProvider;

import java.util.List;

public class RlePipelinedAggregationBenchmark
    extends AbstractTupleStreamBenchmark
{
    public RlePipelinedAggregationBenchmark()
    {
        super("pipelined_agg_rle", 5, 20);
    }
    
    @Override
    protected TupleStream createBenchmarkedTupleStream(TpchTupleStreamProvider inputStreamProvider)
    {
        return new PipelinedAggregationOperator(
                new GroupByOperator(
                        inputStreamProvider.getTupleStream(TpchSchema.Orders.ORDERSTATUS, TupleStreamSerdes.Encoding.RLE)
                ),
                inputStreamProvider.getTupleStream(TpchSchema.Orders.TOTALPRICE, TupleStreamSerdes.Encoding.RAW),
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
