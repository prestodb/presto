package com.facebook.presto.benchmark;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class BenchmarkLong
{
    public static void main(String[] args)
            throws IOException, InterruptedException
    {
        File file = new File("data/columns/column3.data");

        Slice pageTypeColumnSlice = Slices.mapFileReadOnly(file);

//        TupleInfo info = new TupleInfo(TupleInfo.Type.FIXED_INT_64);

        for (int i = 0; i < 1000000; ++i) {
            TupleStream pageTypeColumn = UncompressedSerde.readAsStream(pageTypeColumnSlice);

//            int count = pageTypeColumnSlice.length() / 8;
//            int current = 0;

//            while (current < count) {
//                sum += pageTypeColumnSlice.getLong(current * 8);
//                ++current;
//            }

            Result result = doIt(pageTypeColumn, pageTypeColumn.getTupleInfo());
            long count = result.count;
            long sum = result.sum;
            Duration duration = result.duration;

            DataSize fileSize = new DataSize(file.length(), DataSize.Unit.BYTE);

            if (i % 10 == 0) {
                System.out.println(String.format("%s, %s, %.2f/s, %2.2f MB/s, %s", duration, count, count / duration.toMillis() * 1000, fileSize.getValue(DataSize.Unit.MEGABYTE) / duration.convertTo(TimeUnit.SECONDS), sum));
            }
        }
        Thread.sleep(1000);
    }

    public static Result doIt(TupleStream pageTypeColumn, TupleInfo info)
    {
        long start = System.nanoTime();
        Cursor groupBy = pageTypeColumn.cursor();

        int count = 0;
        long sum = 0;

        while (Cursors.advanceNextValueNoYield(groupBy)) {
            ++count;
            sum += groupBy.getLong(0);
        }

        Duration duration = Duration.nanosSince(start);

        return new Result(count, sum, duration);



//        int count = 0;
//        long sum = 0;
//        long start = System.nanoTime();
//        for (UncompressedValueBlock block : pageTypeColumn) {
//            Slice slice = block.getSlice();
//            int entries = slice.length() / 8;
//            int current = 0;
//            while (current < entries) {
////                                    sum += slice.getLong(current * 8);
//                sum += info.getLong(slice, current * 8, 0);
//                ++current;
//            }
//
//            count += entries;
//        }
//        Duration duration = Duration.nanosSince(start);
//
//        return new Result(count, sum, duration);
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
