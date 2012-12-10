package com.facebook.presto.cli;

import com.facebook.presto.execution.ExecutionStats;
import com.facebook.presto.server.HttpQueryClient;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.TaskInfo;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.OutputProcessor.OutputStats;
import static com.google.common.collect.Iterables.concat;

public class StatusPrinter
{
    private final long start = System.nanoTime();
    private final HttpQueryClient queryClient;
    private int maxInfoString = 1;

    public StatusPrinter(HttpQueryClient queryClient)
    {
        this.queryClient = queryClient;
    }

    public void printInitialStatusUpdates(PrintStream out)
    {
        try {
            while (true) {
                try {
                    Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);

                    QueryInfo queryInfo = queryClient.getQueryInfo();
                    if (queryInfo == null) {
                        return;
                    }
                    // if query is no longer running, finish
                    if (queryInfo.getState().isDone()) {
                        return;
                    }

                    // check if there is there is pending output
                    if (queryInfo.getOutputStage() != null) {
                        List<TaskInfo> outStage = queryInfo.getStages().get(queryInfo.getOutputStage());
                        for (TaskInfo outputTask : outStage) {
                            if (outputTask.getStats().getBufferedPages() > 0) {
                                return;
                            }
                        }
                    }
                    out.print("\r" + toInfoString(queryInfo, start));
                    out.flush();
                    Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
                }
                catch (Exception ignored) {
                }
            }
        }
        finally {
            // clear line and flush
            out.printf("\r%-" + maxInfoString + "s\r", "");
            out.flush();
        }
    }

    public String getFinalInfo(OutputStats stats)
    {
        return toFinalInfo(queryClient.getQueryInfo(), start, stats.getRows(), stats.getBytes());
    }

    private String toInfoString(QueryInfo queryInfo, long start)
    {
        Duration elapsedTime = Duration.nanosSince(start);

        ExecutionStats executionStats = new ExecutionStats();
        for (TaskInfo info : concat(queryInfo.getStages().values())) {
            executionStats.add(info.getStats());
        }

        QueryState queryState = queryInfo.getState();
        Duration cpuTime = new Duration(executionStats.getSplitCpuTime(), TimeUnit.MILLISECONDS);
        int completedStaged = 0;
        int stages = 0;

        String infoString = String.format(
                "%s QueryId %s: Stages [%,d of %,d]: Splits [%,d total, %,d pending, %,d running, %,d finished]: Input [%,d rows %s]: CPU Time %s %s: Elapsed %s %s",
                queryState,
                queryInfo.getQueryId(),
                completedStaged,
                stages,
                executionStats.getSplits(),
                executionStats.getSplits() - executionStats.getStartedSplits(),
                executionStats.getStartedSplits() - executionStats.getCompletedSplits(),
                executionStats.getCompletedSplits(),
                executionStats.getInputPositionCount(),
                StatusPrinter.formatDataSize(executionStats.getInputDataSize()),
                cpuTime.toString(TimeUnit.SECONDS),
                StatusPrinter.formatDataRate(executionStats.getCompletedDataSize(), cpuTime),
                elapsedTime.toString(TimeUnit.SECONDS),
                StatusPrinter.formatDataRate(executionStats.getCompletedDataSize(), elapsedTime));

        if (infoString.length() < maxInfoString) {
            infoString = String.format("%-" + maxInfoString + "s", infoString);
        }
        maxInfoString = infoString.length();

        return infoString;
    }

    private String toFinalInfo(QueryInfo queryInfo, long start, long outputPositions, long outputDataSize)
    {
        Duration elapsedTime = Duration.nanosSince(start);

        ExecutionStats executionStats = new ExecutionStats();
        for (TaskInfo info : concat(queryInfo.getStages().values())) {
            executionStats.add(info.getStats());
        }

        Duration cpuTime = new Duration(executionStats.getSplitCpuTime(), TimeUnit.MILLISECONDS);

        return String.format("%s QueryId %s: Splits %,d: In %,d rows %s: Out %,d rows %s: CPU Time %s %s: Elapsed %s %s",
                queryInfo.getState(),
                queryInfo.getQueryId(),
                executionStats.getSplits(),
                executionStats.getInputPositionCount(),
                StatusPrinter.formatDataSize(executionStats.getInputDataSize()),
                outputPositions,
                StatusPrinter.formatDataSize(outputDataSize),
                cpuTime.toString(TimeUnit.SECONDS),
                StatusPrinter.formatDataRate(executionStats.getInputDataSize(), cpuTime),
                elapsedTime.toString(TimeUnit.SECONDS),
                StatusPrinter.formatDataRate(executionStats.getInputDataSize(), elapsedTime)
        );
    }

    private static String formatDataSize(long inputDataSize)
    {
        DataSize dataSize = new DataSize(inputDataSize, DataSize.Unit.BYTE).convertToMostSuccinctDataSize();
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

    private static String formatDataRate(long inputDataSize, Duration duration)
    {
        double rate = inputDataSize / duration.convertTo(TimeUnit.SECONDS);
        if (Double.isNaN(rate) || Double.isInfinite(rate)) {
            return "0Bps";
        }
        DataSize dataSize = new DataSize(rate, DataSize.Unit.BYTE).convertToMostSuccinctDataSize();
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
