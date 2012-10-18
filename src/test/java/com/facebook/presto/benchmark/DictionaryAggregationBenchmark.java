package com.facebook.presto.benchmark;

import com.facebook.presto.aggregation.DoubleSumAggregation;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.block.dictionary.DictionaryEncodedTupleStream;
import com.facebook.presto.operator.DictionaryAggregationOperator;
import com.facebook.presto.tpch.TpchSchema;
import com.facebook.presto.tpch.TpchTupleStreamProvider;

public class DictionaryAggregationBenchmark
    extends AbstractTupleStreamBenchmark
{
    public DictionaryAggregationBenchmark()
    {
        super("dictionary_agg", 10, 50);
    }
    
    @Override
    protected TupleStream createBenchmarkedTupleStream(TpchTupleStreamProvider inputStreamProvider)
    {
        return new DictionaryAggregationOperator(
                (DictionaryEncodedTupleStream) inputStreamProvider.getTupleStream(TpchSchema.Orders.ORDERSTATUS, TupleStreamSerdes.Encoding.DICTIONARY_RLE),
                inputStreamProvider.getTupleStream(TpchSchema.Orders.TOTALPRICE, TupleStreamSerdes.Encoding.RAW),
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
