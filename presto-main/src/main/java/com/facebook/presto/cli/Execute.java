package com.facebook.presto.cli;

import com.facebook.presto.Main;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OutputProcessor;
import com.facebook.presto.server.HttpQueryClient;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.TaskInfo;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.OutputProcessor.OutputStats;
import static io.airlift.json.JsonCodec.jsonCodec;

@Command(name = "execute", description = "Execute a query")
public class Execute
        implements Runnable
{
    @Option(name = "-s", title = "server", required = true)
    public URI server;

    @Option(name = "-q", title = "query", required = true)
    public String query;

    public void run()
    {
        Main.initializeLogging(false);

        ExecutorService executor = Executors.newCachedThreadPool();
        HttpQueryClient queryClient = null;
        try {
            ApacheHttpClient httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(1, TimeUnit.DAYS))
                    .setReadTimeout(new Duration(10, TimeUnit.DAYS)));

            queryClient = new HttpQueryClient(query,
                    server,
                    httpClient,
                    executor,
                    jsonCodec(QueryInfo.class),
                    jsonCodec(TaskInfo.class));

            StatusPrinter statusPrinter = new StatusPrinter(queryClient);
            statusPrinter.printInitialStatusUpdates(System.err);

            Operator operator = queryClient.getResultsOperator();
            List<String> fieldNames = queryClient.getQueryInfo().getFieldNames();

            OutputProcessor processor = new OutputProcessor(operator, new AlignedTuplePrinter(fieldNames));
            OutputStats stats = processor.process();

            System.out.println(statusPrinter.getFinalInfo(stats));
        }
        finally {
            if (queryClient != null) {
                queryClient.destroy();
            }
            executor.shutdownNow();
        }
    }
}
