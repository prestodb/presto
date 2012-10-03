package com.facebook.presto.benchmark;

import com.facebook.presto.aggregation.SumAggregation;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.dictionary.DictionaryEncodedTupleStream;
import com.facebook.presto.block.dictionary.DictionarySerde;
import com.facebook.presto.block.rle.RunLengthEncodedSerde;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.operator.DictionaryAggregationOperator;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class BenchmarkDictionaryAggregation
{
    public static void main(String[] args)
            throws IOException, InterruptedException
    {
//        // dictionary-raw not-sorted
//        File groupByFile = new File("data/dic-raw/column5.string_dic-raw.data");
//        DictionarySerde groupBySerde = new DictionarySerde(UncompressedSerde.INSTANCE);
//        File aggregateFile = new File("data/dic-raw/column3.fmillis_raw.data");
//        TupleStreamSerde aggregateSerde = UncompressedSerde.INSTANCE;

        // dictionary-rle
        File groupByFile = new File("data/dic-rle/column6.string_dic-rle.data");
        DictionarySerde groupBySerde = new DictionarySerde(new RunLengthEncodedSerde());
        File aggregateFile = new File("data/dic-rle/column3.fmillis_raw.data");
        TupleStreamSerde aggregateSerde = UncompressedSerde.INSTANCE;

        Slice groupBySlice = Slices.mapFileReadOnly(groupByFile);
        Slice aggregateSlice = Slices.mapFileReadOnly(aggregateFile);

        for (int i = 0; i < 100000; ++i) {
            DictionaryEncodedTupleStream groupBySource = groupBySerde.deserialize(groupBySlice);
            TupleStream aggregateSource = aggregateSerde.deserialize(aggregateSlice);

            DictionaryAggregationOperator aggregation = new DictionaryAggregationOperator(groupBySource, aggregateSource, SumAggregation.PROVIDER);

            Result result = doIt(aggregation);
            long count = result.count;
            Duration duration = result.duration;

            DataSize fileSize = new DataSize(groupByFile.length() + aggregateFile.length(), DataSize.Unit.BYTE);

            System.out.println(String.format("%s, %s, %.2f/s, %2.2f MB/s", duration, count, count / duration.toMillis() * 1000, fileSize.getValue(DataSize.Unit.MEGABYTE) / duration.convertTo(TimeUnit.SECONDS)));
        }
        Thread.sleep(1000);
    }

    public static Result doIt(TupleStream source)
    {
        long start = System.nanoTime();

        Cursor cursor = source.cursor();

        int count = 0;
        long sum = 0;

        while (Cursors.advanceNextValueNoYield(cursor)) {
            ++count;
//            System.out.printf("%s\t%s\n", cursor.getSlice(0).toString(Charsets.UTF_8), cursor.getLong(1));
        }

        Duration duration = Duration.nanosSince(start);

        return new Result(count, sum, duration);
    }

    public static class Result
    {
        private final int count;
        private final long sum;
        private final Duration duration;

        public Result(int count, long sum, Duration duration)
        {
            this.count = count;
            this.sum = sum;
            this.duration = duration;
        }
    }

}
