package com.facebook.presto.benchmark;

import com.facebook.presto.aggregation.SumAggregation;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.operation.SubtractionOperation;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.UncompressedBinaryOperator;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tpch.TpchTupleStreamProvider;

import java.util.List;

public class BinaryOperatorBenchmark
    extends AbstractTupleStreamBenchmark
{
    public BinaryOperatorBenchmark()
    {
        super("binary_operator", 1, 4);
    }

    @Override
    protected TupleStream createBenchmarkedTupleStream(TpchTupleStreamProvider inputStreamProvider)
    {
        return new HashAggregationOperator(
                new GroupByOperator(inputStreamProvider.getTupleStream(TpchSchema.LineItem.LINESTATUS, TupleStreamSerdes.Encoding.RAW)),
                new UncompressedBinaryOperator(
                        inputStreamProvider.getTupleStream(TpchSchema.LineItem.ORDERKEY, TupleStreamSerdes.Encoding.RAW),
                        inputStreamProvider.getTupleStream(TpchSchema.LineItem.PARTKEY, TupleStreamSerdes.Encoding.RAW),
                        new SubtractionOperation()
                ),
                SumAggregation.PROVIDER
        );
    }

    public static void main(String[] args)
    {
        new BinaryOperatorBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
