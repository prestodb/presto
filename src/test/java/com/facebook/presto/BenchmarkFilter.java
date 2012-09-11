package com.facebook.presto;

import com.facebook.presto.block.cursor.BlockCursor;
import com.facebook.presto.operators.DataScan2;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Predicate;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class BenchmarkFilter
{
    public static void main(String[] args)
            throws IOException, InterruptedException
    {
        File file = new File("data/columns//column4.data");  // long
        Predicate<BlockCursor> predicate = new LongFilter(1_343_900_000_000L);

//        File file = new File("data/columns/column5.data");  // string
//        Predicate<BlockCursor> predicate = new StringFilter(9);

        Slice pageTypeColumnSlice = Slices.mapFileReadOnly(file);
        for (int i = 0; i < 100000; ++i) {
            BlockStream<? extends ValueBlock> pageTypeColumn = UncompressedBlockSerde.readAsStream(pageTypeColumnSlice);
            DataScan2 filtered = new DataScan2(pageTypeColumn, predicate) ;

            Result result = doIt(filtered);
            long count = result.count;
            Duration duration = result.duration;

            DataSize fileSize = new DataSize(file.length(), DataSize.Unit.BYTE);

            System.out.println(String.format("%s, %s, %.2f/s, %2.2f MB/s", duration, count, count / duration.toMillis() * 1000, fileSize.getValue(DataSize.Unit.MEGABYTE) / duration.convertTo(TimeUnit.SECONDS)));
        }
        Thread.sleep(1000);
    }

    public static Result doIt(BlockStream<? extends ValueBlock> source)
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

    public static class StringFilter implements Predicate<BlockCursor> {

        private final long minLength;

        public StringFilter(long minLength)
        {
            this.minLength = minLength;
        }

        @Override
        public boolean apply(@Nullable BlockCursor input)
        {
            return input.getSlice(0).length() >= minLength;
        }
    }

    public static class LongFilter implements Predicate<BlockCursor> {

        private final long minValue;

        public LongFilter(long minValue)
        {
            this.minValue = minValue;
        }

        @Override
        public boolean apply(@Nullable BlockCursor input)
        {
            return input.getLong(0) >= minValue;
        }
    }
}
