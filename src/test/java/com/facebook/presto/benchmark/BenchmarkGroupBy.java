package com.facebook.presto.benchmark;

import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.dictionary.DictionarySerde;
import com.facebook.presto.block.rle.RunLengthEncodedSerde;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class BenchmarkGroupBy
{
    public static void main(String[] args)
            throws IOException, InterruptedException
    {
//        // raw not-sorted
//        File groupByFile = new File("data/raw/column5.string_raw.data");
//        TupleStreamSerde groupBySerde = UncompressedSerde.INSTANCE;

//        // dictionary-raw not-sorted
//        File groupByFile = new File("data/dic-raw/column5.string_dic-raw.data");
//        TupleStreamSerde groupBySerde = new DictionarySerde(UncompressedSerde.INSTANCE);

//        // raw sorted
//        File groupByFile = new File("data/sorted/column5.string_raw.data");
//        TupleStreamSerde groupBySerde = UncompressedSerde.INSTANCE;

//        // rle
//        File groupByFile = new File("data/rle/column5.string_rle.data");
//        TupleStreamSerde groupBySerde = new RunLengthEncodedSerde();

        // dictionary-rle
        File groupByFile = new File("data/dic-rle/column5.string_dic-rle.data");
        TupleStreamSerde groupBySerde = new DictionarySerde(new RunLengthEncodedSerde());

        Slice pageTypeColumnSlice = Slices.mapFileReadOnly(groupByFile);
        for (int i = 0; i < 100000; ++i) {
            TupleStream pageTypeColumn = groupBySerde.deserialize(pageTypeColumnSlice);
            GroupByOperator groupBy = new GroupByOperator(pageTypeColumn) ;

            Result result = doIt(groupBy);
            long count = result.count;
            Duration duration = result.duration;

            DataSize fileSize = new DataSize(groupByFile.length(), DataSize.Unit.BYTE);

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

        while (cursor.advanceNextValue()) {
            ++count;
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
