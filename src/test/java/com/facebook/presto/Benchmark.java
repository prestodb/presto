package com.facebook.presto;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Benchmark
{
    public static void main(String[] args)
            throws IOException, InterruptedException
    {
        File file = new File("data/columns/column5.data");

        Slice pageTypeColumnSlice = Slices.mapFileReadOnly(file);

        for (int i = 0; i < 100000; ++i) {
            BlockStream<UncompressedValueBlock> pageTypeColumn = UncompressedBlockSerde.readAsStream(pageTypeColumnSlice);

            Result result = doIt(pageTypeColumn);
            long count = result.count;
            Duration duration = result.duration;

            DataSize fileSize = new DataSize(file.length(), DataSize.Unit.BYTE);

            System.out.println(String.format("%s, %s, %.2f/s, %2.2f MB/s", duration, count, count / duration.toMillis() * 1000, fileSize.getValue(DataSize.Unit.MEGABYTE) / duration.convertTo(TimeUnit.SECONDS)));
        }
        Thread.sleep(1000);
    }

    public static Result doIt(BlockStream<UncompressedValueBlock> pageTypeColumn)
    {
        Cursor groupBy = pageTypeColumn.cursor();

        int count = 0;
        long sum = 0;
        long start = System.nanoTime();

        while (groupBy.hasNextValue()) {
            groupBy.advanceNextValue();
            ++count;
            sum += groupBy.getSlice(0).getByte(0);
//            sum += groupBy.getLong(0);
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
