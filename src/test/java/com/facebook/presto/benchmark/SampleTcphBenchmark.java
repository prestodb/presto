package com.facebook.presto.benchmark;

import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tpch.TpchDataProvider;
import com.facebook.presto.tpch.TpchSchema;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SampleTcphBenchmark
    extends AbstractBenchmark
{
    private final TpchDataProvider tpchDataProvider;
    private File totalPriceColumnFile;
    private Slice totalPriceSlice;

    public SampleTcphBenchmark(TpchDataProvider tpchDataProvider)
    {
        super("sample", 2, 10);
        Preconditions.checkNotNull(tpchDataProvider, "tpchDataProvider is null");
        this.tpchDataProvider = tpchDataProvider;
    }

    @Override
    protected String getDefaultResult()
    {
        return "megabytes_per_second";
    }

    @Override
    protected void setUp()
    {
        try {
            totalPriceColumnFile = tpchDataProvider.getColumnFile(TpchSchema.Orders.TOTALPRICE, TupleStreamSerde.Encoding.RAW);
            totalPriceSlice = Slices.mapFileReadOnly(totalPriceColumnFile);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected Map<String, Long> runOnce()
    {
        long start = System.nanoTime();

        TupleStream tupleStream = TupleStreamSerde.Encoding.RAW.createSerde()
                .createDeserializer()
                .deserialize(totalPriceSlice);
        Cursor cursor = tupleStream.cursor(new QuerySession());
        long count = 0;
        double sum = 0;
        while (Cursors.advanceNextValueNoYield(cursor)) {
            count++;
            sum += cursor.getDouble(0);
        }

        Duration duration = Duration.nanosSince(start);

        double elapsedSeconds = duration.convertTo(TimeUnit.SECONDS);
        DataSize fileSize = new DataSize(totalPriceColumnFile.length(), DataSize.Unit.BYTE);


        return ImmutableMap.<String, Long>builder()
                .put("elapsed_seconds", (long) elapsedSeconds)
                .put("lines", count)
                .put("sum", (long) sum)
                .put("lines_per_second", (long) (count / elapsedSeconds))
                .put("megabytes_per_second", (long) (fileSize.getValue(DataSize.Unit.MEGABYTE) / elapsedSeconds))
                .build();
    }

    public static void main(String[] args)
    {
        new SampleTcphBenchmark(BenchmarkSuite.TPCH_DATA_PROVIDER).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
