package com.facebook.presto.cli;

import com.facebook.presto.Main;
import com.facebook.presto.execution.FailureInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OutputProcessor;
import com.facebook.presto.server.HttpQueryClient;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.Duration;
import org.antlr.runtime.RecognitionException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.fusesource.jansi.AnsiConsole;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.OutputProcessor.OutputHandler;
import static com.facebook.presto.operator.OutputProcessor.OutputStats;
import static org.fusesource.jansi.AnsiConsole.out;

@Command(name = "execute", description = "Execute a query")
public class Execute
        implements Runnable
{
    @Option(name = "-s", title = "server", required = true)
    public URI server;

    @Option(name = "-q", title = "query", required = true)
    public String query;

    @Option(name = "--debug", title = "debug")
    public boolean debug;

    private static final JsonCodecFactory codecFactory;

    static {
        // todo use Guice
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        ImmutableMap.Builder<Class<?>, JsonDeserializer<?>> deserializers = ImmutableMap.builder();
        deserializers.put(Expression.class, new JsonDeserializer<Expression>()
        {
            @Override
            public Expression deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                    throws IOException
            {
                try {
                    return SqlParser.createExpression(jsonParser.readValueAs(String.class));
                }
                catch (RecognitionException e) {
                    throw Throwables.propagate(e);
                }
            }
        });


        deserializers.put(FunctionCall.class, new JsonDeserializer<FunctionCall>()
        {
            @Override
            public FunctionCall deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                    throws IOException
            {
                try {
                    return (FunctionCall) SqlParser.createExpression(jsonParser.readValueAs(String.class));
                }
                catch (RecognitionException e) {
                    throw Throwables.propagate(e);
                }
            }
        });

        objectMapperProvider.setJsonDeserializers(deserializers.build());
        codecFactory = new JsonCodecFactory(objectMapperProvider);
    }

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
                    codecFactory.jsonCodec(QueryInfo.class),
                    codecFactory.jsonCodec(TaskInfo.class));

            out().print("\n");

            StatusPrinter statusPrinter = new StatusPrinter(queryClient, out());
            statusPrinter.printInitialStatusUpdates();

            QueryInfo queryInfo = queryClient.getQueryInfo();
            if (queryInfo.getState().isDone()) {
                if (queryInfo.getState() == QueryState.CANCELED) {
                    AnsiConsole.out().printf("Query %s was canceled\n", queryInfo.getQueryId());
                }
                else if (queryInfo.getState() == QueryState.FAILED) {
                    renderFailure(queryInfo, AnsiConsole.out());
                }
                else {
                    AnsiConsole.out().printf("Query %s finished with no output\n", queryInfo.getQueryId());
                }
            } else {
                Operator operator = queryClient.getResultsOperator();
                List<String> fieldNames = queryInfo.getFieldNames();

                OutputStats stats = pageOutput(operator, fieldNames);

                // print final info after the user exits from the pager
                statusPrinter.printFinalInfo(stats);
            }
        }
        finally {
            if (!debug && queryClient != null) {
                queryClient.destroy();
            }
            executor.shutdownNow();
        }
    }

    public void renderFailure(QueryInfo queryInfo, PrintStream out)
    {
        if (!debug) {
            Set<String> failureMessages = ImmutableSet.copyOf(getFailureMessages(queryInfo));
            if (failureMessages.isEmpty()) {
                out.printf("Query %s failed for an unknown reason\n", queryInfo.getQueryId());
            }
            else if (failureMessages.size() == 1) {
                out.printf("Query %s failed: %s\n", queryInfo.getQueryId(), Iterables.getOnlyElement(failureMessages));
            }
            else {
                out.printf("Query %s failed:\n", queryInfo.getQueryId());
                for (String failureMessage : failureMessages) {
                    out.println("    " + failureMessage);
                }
            }
        } else {
            out.printf("Query %s failed:\n", queryInfo.getQueryId());
            renderStacks(queryInfo, out);
        }
    }

    private void renderStacks(QueryInfo queryInfo, PrintStream out)
    {
        for (FailureInfo failureInfo : queryInfo.getFailures()) {
            failureInfo.toException().printStackTrace(out);
        }
        if (queryInfo.getOutputStage() != null) {
            renderStacks(queryInfo.getOutputStage(), out);
        }
    }

    private void renderStacks(StageInfo stageInfo, PrintStream out)
    {
        if (!stageInfo.getFailures().isEmpty()) {
            out.printf("Stage %s failed:\n", stageInfo.getStageId());
            for (FailureInfo failureInfo : stageInfo.getFailures()) {
                failureInfo.toException().printStackTrace(out);
            }
        }
        for (TaskInfo taskInfo : stageInfo.getTasks()) {
            renderStacks(taskInfo, out);
        }
        for (StageInfo subStageInfo : stageInfo.getSubStages()) {
            renderStacks(subStageInfo, out);
        }
    }

    private void renderStacks(TaskInfo taskInfo, PrintStream out)
    {
        if (!taskInfo.getFailures().isEmpty()) {
            out.printf("Task %s failed:\n", taskInfo.getTaskId());
            for (FailureInfo failureInfo : taskInfo.getFailures()) {
                failureInfo.toException().printStackTrace(out);
            }
        }
    }

    public static List<String> getFailureMessages(QueryInfo queryInfo)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (FailureInfo failureInfo : queryInfo.getFailures()) {
            builder.add(failureInfo.getMessage());
        }
        if (queryInfo.getOutputStage() != null) {
            builder.addAll(getFailureMessages(queryInfo.getOutputStage()));
        }
        return builder.build();
    }

    public static List<String> getFailureMessages(StageInfo stageInfo)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (FailureInfo failureInfo : stageInfo.getFailures()) {
            builder.add(failureInfo.getMessage());
        }
        for (TaskInfo taskInfo : stageInfo.getTasks()) {
            builder.addAll(getFailureMessages(taskInfo));
        }
        for (StageInfo subStageInfo : stageInfo.getSubStages()) {
            builder.addAll(getFailureMessages(subStageInfo));
        }
        return builder.build();
    }

    private static List<String> getFailureMessages(TaskInfo taskInfo)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (FailureInfo failureInfo : taskInfo.getFailures()) {
            builder.add(failureInfo.getMessage());
        }
        return builder.build();
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
