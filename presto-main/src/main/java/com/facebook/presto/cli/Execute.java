package com.facebook.presto.cli;

import com.facebook.presto.Main;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.operator.ConsolePrinter;
import com.facebook.presto.server.HttpQueryProvider;
import com.facebook.presto.server.QueryDriversTupleStream;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.block.Cursors.advanceNextPositionNoYield;

@Command(name = "execute", description = "Execute a query")
public class Execute
        implements Runnable
{
    private static final Logger log = Logger.get(Execute.class);

    @Option(name = "-s", title = "server", required = true)
    public URI server;

    @Option(name = "-q", title = "query", required = true)
    public String query;

    public void run()
    {
        Main.initializeLogging(false);

        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            ApacheHttpClient httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(1, TimeUnit.MINUTES))
                    .setReadTimeout(new Duration(30, TimeUnit.MINUTES)));
            AsyncHttpClient asyncHttpClient = new AsyncHttpClient(httpClient, executor);
            QueryDriversTupleStream tupleStream = new QueryDriversTupleStream(new TupleInfo(TupleInfo.Type.FIXED_INT_64), 10,
                    new HttpQueryProvider(query, asyncHttpClient, server)
            );
            // TODO: this currently leaks query resources (need to delete)

            ConsolePrinter.TuplePrinter tuplePrinter = new ConsolePrinter.DelimitedTuplePrinter();

            Cursor cursor = tupleStream.cursor(new QuerySession());
            while (advanceNextPositionNoYield(cursor)) {
                Tuple tuple = cursor.getTuple();
                tuplePrinter.print(tuple);
            }
            log.info("Query complete.");
        }
        finally {
            executor.shutdownNow();
        }
    }
}
