package com.facebook.presto.cli;

import com.facebook.presto.Main;
import com.facebook.presto.server.HttpQueryProvider;
import com.facebook.presto.server.QueryDriversOperator;
import com.facebook.presto.server.QueryInfo;
import com.facebook.presto.server.QueryState.State;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.command.Command;
import io.airlift.command.Option;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.concat;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;

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
        HttpQueryProvider queryProvider = null;
        try {
            long start = System.nanoTime();

            ApacheHttpClient httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(1, TimeUnit.MINUTES))
                    .setReadTimeout(new Duration(30, TimeUnit.MINUTES)));

            queryProvider = new HttpQueryProvider(createStaticBodyGenerator(query, Charsets.UTF_8),
                    Optional.<String>absent(),
                    httpClient,
                    executor,
                    server);

            try {
                for (QueryInfo queryInfo = queryProvider.getQueryInfo(); queryInfo.getState() == State.PREPARING; queryInfo = queryProvider.getQueryInfo()) {
                    System.err.print("\r" + toInfoString(queryInfo, start));
                    System.err.flush();
                    Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
                }
            }
            finally {
                System.err.println("\r");
                System.err.flush();
            }

            QueryDriversOperator operator = new QueryDriversOperator(10, queryProvider);

            Utils.printResults(start, operator);
        }
        finally {
            if (queryProvider != null) {
                queryProvider.destroy();
            }
            executor.shutdownNow();
        }
    }

    private String toInfoString(QueryInfo queryInfo, long start)
    {
        int stages = queryInfo.getStages().size();
        int completeStages = queryInfo.getStages().size();

        State overallState = State.PREPARING;
        int tasks = 0;
        int preparingTasks = 0;
        int runningTasks = 0;
        int completeTasks = 0;
        int splits = 0;
        int completedSplits = 0;
        for (QueryInfo info : concat(ImmutableList.of(queryInfo), concat(queryInfo.getStages().values()))) {
            tasks++;
            splits += info.getSplits();
            completedSplits += info.getCompletedSplits();
            switch (info.getState()) {
                case PREPARING:
                    preparingTasks++;
                    break;
                case RUNNING:
                    runningTasks++;
                    if (overallState == State.PREPARING) {
                        overallState = State.RUNNING;
                    }
                    break;
                case FINISHED:
                    completeTasks++;
                    break;
                case CANCELED:
                    completeTasks++;
                    if (overallState != State.FAILED) {
                        overallState = State.CANCELED;
                    }
                    break;
                case FAILED:
                    completeTasks++;
                    overallState = State.FAILED;
                    break;
            }
        }
        if (completedSplits == splits && (overallState == State.PREPARING || overallState == State.RUNNING)) {
            overallState = State.FINISHED;
        }

        return String.format("%s QueryId %s: Stages [%d of %d]: Splits [%d of %d]: Tasks [%d total, %d preparing, %d running, %d complete)]: Elapsed %s",
                overallState,
                queryInfo.getQueryId(),
                completeStages,
                stages,
                completedSplits,
                splits,
                tasks,
                preparingTasks,
                runningTasks,
                completeTasks,
                Duration.nanosSince(start).toString(TimeUnit.SECONDS));
    }
}
