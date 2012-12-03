package com.facebook.presto.cli;

import com.facebook.presto.Main;
import com.facebook.presto.operator.ConsolePrinter;
import com.facebook.presto.operator.ConsolePrinter.DelimitedTuplePrinter;
import com.facebook.presto.operator.Page;
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
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
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
                    .setConnectTimeout(new Duration(1, TimeUnit.SECONDS))
                    .setReadTimeout(new Duration(1, TimeUnit.SECONDS)));

            queryProvider = new HttpQueryProvider(createStaticBodyGenerator(query, Charsets.UTF_8),
                    Optional.<String>absent(),
                    httpClient,
                    executor,
                    server);

            printInitialStatusUpdates(queryProvider, start);

            QueryDriversOperator operator = new QueryDriversOperator(10, queryProvider);

            ConsolePrinter consolePrinter = new ConsolePrinter(operator, new DelimitedTuplePrinter());

            long outputPositions = 0;
            long outputDataSize = 0;
            for (Page page : consolePrinter) {
                outputPositions += page.getPositionCount();
                outputDataSize += page.getDataSize().toBytes();
            }
            System.err.println(toFinalInfo(queryProvider.getQueryInfo(), start, outputPositions, outputDataSize));
        }
        finally {
            if (queryProvider != null) {
                queryProvider.destroy();
            }
            executor.shutdownNow();
        }
    }

    private void printInitialStatusUpdates(HttpQueryProvider queryProvider, long start)
    {
        try {
            while (true) {
                try {
                    QueryInfo queryInfo = queryProvider.getQueryInfo();
                    if (queryInfo == null) {
                        // todo maybe throw
                        return;
                    }
                    if (queryInfo.getState() != State.PREPARING) {
                        return;
                    }
                    System.err.print("\r" + toInfoString(queryInfo, start));
                    System.err.flush();
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                }
                catch (Exception ignored) {
                }
            }
        }
        finally {
            // clear line and flush
            System.err.printf("%-" + maxInfoString + "s\r", "");
            System.err.flush();
            // reset to beginning of line... this shouldn't be necessary, but intellij likes it
            System.err.print("\r");
        }
    }

    private int maxInfoString = 1;

    private String toInfoString(QueryInfo queryInfo, long start)
    {
        Duration elapsedTime = Duration.nanosSince(start);

        int stages = queryInfo.getStages().size();
        int completeStages = queryInfo.getStages().size();

        State overallState = State.PREPARING;
        int startedSplits = 0;
        int completedSplits = 0;
        int totalSplits = 0;
        long splitCpuTime = 0;
        long inputDataSize = 0;
        long inputPositions = 0;
        long completedDataSize = 0;
        long completedPositions = 0;
        for (QueryInfo info : concat(ImmutableList.of(queryInfo), concat(queryInfo.getStages().values()))) {
            totalSplits += info.getSplits();
            startedSplits += info.getStartedSplits();
            completedSplits += info.getCompletedSplits();
            splitCpuTime += info.getSplitCpuTime();
            inputDataSize += info.getInputDataSize();
            inputPositions += info.getInputPositionCount();
            completedDataSize += info.getCompletedDataSize();
            completedPositions += info.getCompletedPositionCount();
            switch (info.getState()) {
                case RUNNING:
                    if (overallState == State.PREPARING) {
                        overallState = State.RUNNING;
                    }
                    break;
                case FINISHED:
                case CANCELED:
                    if (overallState != State.FAILED) {
                        overallState = State.CANCELED;
                    }
                    break;
                case FAILED:
                    overallState = State.FAILED;
                    break;
            }
        }
        if (totalSplits > 0 && completedSplits == totalSplits && (overallState == State.PREPARING || overallState == State.RUNNING)) {
            overallState = State.FINISHED;
        }

        Duration cpuTime = new Duration(splitCpuTime, TimeUnit.MILLISECONDS);
        String infoString = String.format(
                "%s QueryId %s: Stages [%,d of %,d]: Splits [%,d total, %,d pending, %,d running, %,d finished]: Input [%,d rows %s]: CPU Time %s %s: Elapsed %s %s",
                overallState,
                queryInfo.getQueryId(),
                completeStages,
                stages,
                totalSplits,
                totalSplits - startedSplits,
                startedSplits - completedSplits,
                completedSplits,
                inputPositions,
                formatDataSize(inputDataSize),
                cpuTime.toString(TimeUnit.SECONDS),
                formatDataRate(completedDataSize, cpuTime),
                elapsedTime.toString(TimeUnit.SECONDS),
                formatDataRate(completedDataSize, elapsedTime));

        if (infoString.length() < maxInfoString) {
            infoString = String.format("%-" + maxInfoString + "s", infoString);
        }
        maxInfoString = infoString.length();

        return infoString;
    }

    private String toFinalInfo(QueryInfo queryInfo, long start, long outputPositions, long outputDataSize)
    {
        Duration elapsedTime = Duration.nanosSince(start);

        int totalSplits = 0;
        long splitCpuTime = 0;
        long inputDataSize = 0;
        long inputPositions = 0;
        for (QueryInfo info : concat(ImmutableList.of(queryInfo), concat(queryInfo.getStages().values()))) {
            totalSplits += info.getSplits();
            splitCpuTime += info.getSplitCpuTime();
            inputDataSize += info.getInputDataSize();
            inputPositions += info.getInputPositionCount();
        }
        Duration cpuTime = new Duration(splitCpuTime, TimeUnit.MILLISECONDS);

        return String.format("FINISHED QueryId %s: Splits %,d: In %,d rows %s: Out %,d rows %s: CPU Time %s %s: Elapsed %s %s",
                queryInfo.getQueryId(),
                totalSplits,
                inputPositions,
                formatDataSize(inputDataSize),
                outputPositions,
                formatDataSize(outputDataSize),
                cpuTime.toString(TimeUnit.SECONDS),
                formatDataRate(inputDataSize, cpuTime),
                elapsedTime.toString(TimeUnit.SECONDS),
                formatDataRate(inputDataSize, elapsedTime)
        );
    }

    private String formatDataSize(long inputDataSize)
    {
        DataSize dataSize = new DataSize(inputDataSize, Unit.BYTE).convertToMostSuccinctDataSize();
        String unitString;
        switch (dataSize.getUnit()) {
            case BYTE:
                unitString = "B";
                break;
            case KILOBYTE:
                unitString = "kB";
                break;
            case MEGABYTE:
                unitString = "MB";
                break;
            case GIGABYTE:
                unitString = "GB";
                break;
            case TERABYTE:
                unitString = "TB";
                break;
            case PETABYTE:
                unitString = "PB";
                break;
            default:
                throw new IllegalStateException("Unknown data unit: " + dataSize.getUnit());
        }
        return String.format("%.01f%s", dataSize.getValue(), unitString);
    }

    private String formatDataRate(long inputDataSize, Duration duration)
    {
        double rate = inputDataSize / duration.convertTo(TimeUnit.SECONDS);
        if (Double.isNaN(rate) || Double.isInfinite(rate)) {
            return "0Bps";
        }
        DataSize dataSize = new DataSize(rate, Unit.BYTE).convertToMostSuccinctDataSize();
        String unitString;
        switch (dataSize.getUnit()) {
            case BYTE:
                unitString = "Bps";
                break;
            case KILOBYTE:
                unitString = "kBps";
                break;
            case MEGABYTE:
                unitString = "MBps";
                break;
            case GIGABYTE:
                unitString = "GBps";
                break;
            case TERABYTE:
                unitString = "TBps";
                break;
            case PETABYTE:
                unitString = "PBps";
                break;
            default:
                throw new IllegalStateException("Unknown data unit: " + dataSize.getUnit());
        }
        return String.format("%.01f%s", dataSize.getValue(), unitString);
    }
}
