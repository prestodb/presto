package com.facebook.presto.benchmark;

import com.facebook.presto.aggregation.SumAggregation;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.operation.SubtractionOperation;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.UncompressedBinaryOperator;
import com.facebook.presto.tpch.TpchSchema;

import java.util.List;

public class BinaryOperatorBenchmark
    extends AbstractTupleStreamBenchmark
{
    public BinaryOperatorBenchmark()
    {
        super("binary_operator", 5, 20);
    }

    @Override
    protected void setUp()
    {
        loadColumnFile(TpchSchema.LineItem.ORDERKEY, TupleStreamSerde.Encoding.RAW);
        loadColumnFile(TpchSchema.LineItem.PARTKEY, TupleStreamSerde.Encoding.RAW);
        loadColumnFile(TpchSchema.LineItem.LINESTATUS, TupleStreamSerde.Encoding.RAW);
    }

    @Override
    protected TupleStream createBenchmarkedTupleStream(List<? extends TupleStream> inputTupleStreams)
    {
        return new HashAggregationOperator(
                new GroupByOperator(inputTupleStreams.get(2)),
                new UncompressedBinaryOperator(inputTupleStreams.get(0), inputTupleStreams.get(1), new SubtractionOperation()),
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
