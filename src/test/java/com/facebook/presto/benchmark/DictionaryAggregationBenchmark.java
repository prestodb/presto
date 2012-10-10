package com.facebook.presto.benchmark;

import com.facebook.presto.aggregation.DoubleSumAggregation;
import com.facebook.presto.block.*;
import com.facebook.presto.block.dictionary.DictionaryEncodedTupleStream;
import com.facebook.presto.operator.DictionaryAggregationOperator;
import com.facebook.presto.tpch.TpchSchema;

import java.util.List;

public class DictionaryAggregationBenchmark
    extends AbstractTupleStreamBenchmark
{
    public DictionaryAggregationBenchmark()
    {
        super("dictionary_agg", 10, 100);
    }

    @Override
    protected void setUp()
    {
        loadColumnFile(TpchSchema.Orders.ORDERSTATUS, TupleStreamSerdes.Encoding.DICTIONARY_RLE);
        loadColumnFile(TpchSchema.Orders.TOTALPRICE, TupleStreamSerdes.Encoding.RAW);
    }

    @Override
    protected TupleStream createBenchmarkedTupleStream(List<? extends TupleStream> inputTupleStreams)
    {
        return new DictionaryAggregationOperator(
                // Terrible hack until we can figure out how to propagate the DictionaryEncodedTupleStream more cleanly!
                (DictionaryEncodedTupleStream) ((StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStream) inputTupleStreams.get(0)).getUnderlyingTupleStream(),
                inputTupleStreams.get(1),
                DoubleSumAggregation.PROVIDER
        );
    }

    public static void main(String[] args)
    {
        new DictionaryAggregationBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
