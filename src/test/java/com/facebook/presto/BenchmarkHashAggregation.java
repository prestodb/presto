package com.facebook.presto;

import com.facebook.presto.aggregations.SumAggregation;
import com.facebook.presto.operators.GroupByBlockStream;
import com.facebook.presto.operators.HashAggregationBlockStream;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class BenchmarkHashAggregation
{
    public static void main(String[] args)
            throws IOException, InterruptedException
    {
//        File groupByFile = new File("data/column5/column0.data");  // sorted
        File groupByFile = new File("data/columns/column5.data");  // not-sorted
        File aggregateFile = new File("data/columns/column3.data");

        Slice groupBySlice = Slices.mapFileReadOnly(groupByFile);
        Slice aggregateSlice = Slices.mapFileReadOnly(aggregateFile);

        for (int i = 0; i < 100000; ++i) {
            BlockStream<UncompressedValueBlock> groupBySource = UncompressedBlockSerde.readAsStream(groupBySlice);
            BlockStream<UncompressedValueBlock> aggregateSource = UncompressedBlockSerde.readAsStream(aggregateSlice);

            GroupByBlockStream groupBy = new GroupByBlockStream(groupBySource);
            HashAggregationBlockStream aggregation = new HashAggregationBlockStream(groupBy, aggregateSource, SumAggregation.PROVIDER);

            Result result = doIt(aggregation);
            long count = result.count;
            Duration duration = result.duration;

            DataSize fileSize = new DataSize(groupByFile.length(), DataSize.Unit.BYTE);

            System.out.println(String.format("%s, %s, %.2f/s, %2.2f MB/s", duration, count, count / duration.toMillis() * 1000, fileSize.getValue(DataSize.Unit.MEGABYTE) / duration.convertTo(TimeUnit.SECONDS)));
        }
        Thread.sleep(1000);
    }

    public static Result doIt(BlockStream<? extends ValueBlock> source)
    {
        Cursor cursor = source.cursor();

        int count = 0;
        long sum = 0;
        long start = System.nanoTime();

        while (cursor.hasNextValue()) {
            cursor.advanceNextValue();
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
