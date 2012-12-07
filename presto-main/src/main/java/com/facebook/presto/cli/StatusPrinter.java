package com.facebook.presto.cli;

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
                            if (outputTask.getBufferedPages() > 0) {
                                return;
                            }
                        }
                    }
                    out.print("\r" + toInfoString(queryInfo, start));
                    out.flush();
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
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

        int stages = queryInfo.getStages().size();
        int completeStages = queryInfo.getStages().size();

        int startedSplits = 0;
        int completedSplits = 0;
        int totalSplits = 0;
        long splitCpuTime = 0;
        long inputDataSize = 0;
        long inputPositions = 0;
        long completedDataSize = 0;
        long completedPositions = 0;
        for (TaskInfo info : concat(queryInfo.getStages().values())) {
            totalSplits += info.getSplits();
            startedSplits += info.getStartedSplits();
            completedSplits += info.getCompletedSplits();
            splitCpuTime += info.getSplitCpuTime();
            inputDataSize += info.getInputDataSize();
            inputPositions += info.getInputPositionCount();
            completedDataSize += info.getCompletedDataSize();
            completedPositions += info.getCompletedPositionCount();
        }

        QueryState queryState = queryInfo.getState();
        Duration cpuTime = new Duration(splitCpuTime, TimeUnit.MILLISECONDS);
        String infoString = String.format(
                "%s QueryId %s: Stages [%,d of %,d]: Splits [%,d total, %,d pending, %,d running, %,d finished]: Input [%,d rows %s]: CPU Time %s %s: Elapsed %s %s",
                queryState,
                queryInfo.getQueryId(),
                completeStages,
                stages,
                totalSplits,
                totalSplits - startedSplits,
                startedSplits - completedSplits,
                completedSplits,
                inputPositions,
                StatusPrinter.formatDataSize(inputDataSize),
                cpuTime.toString(TimeUnit.SECONDS),
                StatusPrinter.formatDataRate(completedDataSize, cpuTime),
                elapsedTime.toString(TimeUnit.SECONDS),
                StatusPrinter.formatDataRate(completedDataSize, elapsedTime));

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
        for (TaskInfo info : concat(queryInfo.getStages().values())) {
            totalSplits += info.getSplits();
            splitCpuTime += info.getSplitCpuTime();
            inputDataSize += info.getInputDataSize();
            inputPositions += info.getInputPositionCount();
        }
        Duration cpuTime = new Duration(splitCpuTime, TimeUnit.MILLISECONDS);

        return String.format("%s QueryId %s: Splits %,d: In %,d rows %s: Out %,d rows %s: CPU Time %s %s: Elapsed %s %s",
                queryInfo.getState(),
                queryInfo.getQueryId(),
                totalSplits,
                inputPositions,
                StatusPrinter.formatDataSize(inputDataSize),
                outputPositions,
                StatusPrinter.formatDataSize(outputDataSize),
                cpuTime.toString(TimeUnit.SECONDS),
                StatusPrinter.formatDataRate(inputDataSize, cpuTime),
                elapsedTime.toString(TimeUnit.SECONDS),
                StatusPrinter.formatDataRate(inputDataSize, elapsedTime)
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
