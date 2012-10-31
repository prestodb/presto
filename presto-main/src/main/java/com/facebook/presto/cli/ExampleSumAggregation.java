package com.facebook.presto.cli;

import com.facebook.presto.Main;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.operator.ConsolePrinter;
import com.facebook.presto.server.HttpQueryProvider;
import com.facebook.presto.server.QueryDriversTupleStream;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.block.Cursors.advanceNextPositionNoYield;

@Command(name = "sum", description = "Run an example sum aggregation")
public class ExampleSumAggregation
        implements Runnable
{
    @Arguments(required = true)
    public URI server;

    public void run()
    {
        Main.initializeLogging(false);

        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            long start = System.nanoTime();

            ApacheHttpClient httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(1, TimeUnit.MINUTES))
                    .setReadTimeout(new Duration(1, TimeUnit.MINUTES)));
            AsyncHttpClient asyncHttpClient = new AsyncHttpClient(httpClient, executor);
            QueryDriversTupleStream tupleStream = new QueryDriversTupleStream(new TupleInfo(TupleInfo.Type.VARIABLE_BINARY, TupleInfo.Type.FIXED_INT_64), 10,
                    new HttpQueryProvider("sum", asyncHttpClient, server)
            );
            // TODO: this currently leaks query resources (need to delete)

            //                TuplePrinter tuplePrinter = new RecordTuplePrinter();
            ConsolePrinter.TuplePrinter tuplePrinter = new ConsolePrinter.DelimitedTuplePrinter();

            int count = 0;
            long grandTotal = 0;
            Cursor cursor = tupleStream.cursor(new QuerySession());
            while (advanceNextPositionNoYield(cursor)) {
                count++;
                Tuple tuple = cursor.getTuple();
                grandTotal += tuple.getLong(1);
                tuplePrinter.print(tuple);
            }
            Duration duration = Duration.nanosSince(start);

            System.out.printf("%d rows in %4.2f ms %d grandTotal\n", count, duration.toMillis(), grandTotal);
        }
        finally {
            executor.shutdownNow();
        }
    }
}
