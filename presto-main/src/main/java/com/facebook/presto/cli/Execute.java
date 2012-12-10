package com.facebook.presto.cli;

import com.facebook.presto.Main;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OutputProcessor;
import com.facebook.presto.server.HttpQueryClient;
import com.google.common.base.Charsets;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.Duration;
import org.fusesource.jansi.AnsiConsole;

import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.OutputProcessor.OutputHandler;
import static com.facebook.presto.operator.OutputProcessor.OutputStats;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.fusesource.jansi.AnsiConsole.out;

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
        AnsiConsole.systemInstall();
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

            out().print("\n");

            StatusPrinter statusPrinter = new StatusPrinter(queryClient, out());
            statusPrinter.printInitialStatusUpdates();

            Operator operator = queryClient.getResultsOperator();
            List<String> fieldNames = queryClient.getQueryInfo().getFieldNames();

            OutputStats stats = pageOutput(operator, fieldNames);

            // print final info after the user exits from the pager
            statusPrinter.printFinalInfo(stats);
        }
        finally {
            if (queryClient != null) {
                queryClient.destroy();
            }
            executor.shutdownNow();
        }
    }

    private static OutputStats pageOutput(Operator operator, List<String> fieldNames)
    {
        try (Pager pager = Pager.create(Pager.LESS)) {
            OutputStreamWriter writer = new OutputStreamWriter(pager, Charsets.UTF_8);
            OutputHandler outputHandler = new AlignedTuplePrinter(fieldNames, writer);
            OutputProcessor processor = new OutputProcessor(operator, outputHandler);
            return processor.process();
        }
    }
}
