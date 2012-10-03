package com.facebook.presto.benchmark;

import com.facebook.presto.aggregation.DoubleSumAggregation;
import com.facebook.presto.aggregation.SumAggregation;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.StatsCollectingTupleStreamSerde;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.dictionary.DictionaryEncodedTupleStream;
import com.facebook.presto.block.dictionary.DictionarySerde;
import com.facebook.presto.block.rle.RunLengthEncodedSerde;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.operator.DictionaryAggregationOperator;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tpch.TpchDataProvider;
import com.facebook.presto.tpch.TpchSchema;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
        loadColumnFile(TpchSchema.Orders.ORDERSTATUS, TupleStreamSerde.Encoding.DICTIONARY_RLE);
        loadColumnFile(TpchSchema.Orders.TOTALPRICE, TupleStreamSerde.Encoding.RAW);
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
