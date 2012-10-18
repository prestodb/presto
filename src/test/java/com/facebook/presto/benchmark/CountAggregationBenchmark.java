package com.facebook.presto.benchmark;

import com.facebook.presto.aggregation.CountAggregation;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tpch.TpchTupleStreamProvider;

import java.util.List;

public class CountAggregationBenchmark
    extends AbstractTupleStreamBenchmark
{
    public CountAggregationBenchmark()
    {
        super("count_agg", 10, 100);
    }

    @Override
    protected TupleStream createBenchmarkedTupleStream(TpchTupleStreamProvider inputStreamProvider)
    {
        return new AggregationOperator(
                inputStreamProvider.getTupleStream(TpchSchema.Orders.ORDERKEY, TupleStreamSerdes.Encoding.RAW),
                CountAggregation.PROVIDER
        );
    }

    public static void main(String[] args)
    {
        new CountAggregationBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
