package com.facebook.presto.cli;

import com.facebook.presto.Main;
import com.facebook.presto.server.HttpQueryProvider;
import com.facebook.presto.server.QueryDriversOperator;
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
            long start = System.nanoTime();

            ApacheHttpClient httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(1, TimeUnit.MINUTES))
                    .setReadTimeout(new Duration(30, TimeUnit.MINUTES)));
            AsyncHttpClient asyncHttpClient = new AsyncHttpClient(httpClient, executor);
            QueryDriversOperator operator = new QueryDriversOperator(10,
                    new HttpQueryProvider(query, asyncHttpClient, server, 1)
            );
            // TODO: this currently leaks query resources (need to delete)
            Utils.printResults(start, operator);
        }
        finally {
            executor.shutdownNow();
        }
    }
}
