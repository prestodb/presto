package com.facebook.presto.cli;

import com.facebook.presto.execution.ExecutionStats;
import com.facebook.presto.execution.PageBufferInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.server.HttpQueryClient;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.Ansi.Erase;

import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.OutputProcessor.OutputStats;
import static java.lang.Math.max;
import static org.fusesource.jansi.internal.CLibrary.STDOUT_FILENO;
import static org.fusesource.jansi.internal.CLibrary.isatty;

public class StatusPrinter
{
    private static final String HIDE_CURSOR = "\u001B[?25l";
    private static final String SHOW_CURSOR = "\u001B[?25h";

    private final long start = System.nanoTime();
    private final HttpQueryClient queryClient;
    private final PrintStream out;
    private int lines;

    public StatusPrinter(HttpQueryClient queryClient, PrintStream out)
    {
        this.queryClient = queryClient;
        this.out = out;
    }
/*
RUNNING QueryId 31: Stages [0 of 0]: Splits [648 total, 252 pending, 16 running, 380 finished]: Input [2,659,640 rows 67.3MB]: CPU Time 11.45s 4.2MBps: Elapsed 0.77s 62.7MBps
  RUNNING  31.0: Splits [0 total, -1 pending, 1 running, 0 finished]: Input [0 rows 0.0B]: Elapsed 0.77s 0.0Bps
    RUNNING  31.1: Splits [648 total, 253 pending, 15 running, 380 finished]: Input [2,659,640 rows 67.3MB]: Elapsed 0.77s 62.6MBps


Query 31: Splits [648 total, 252 pending, 16 running, 380 finished]: Input [2,659,640 rows 67.3MB]: CPU Time 11.45s 4.2MBps: Elapsed 0.77s 62.7MBps
  Stage  31.0: Splits [0 total, -1 pending, 1 running, 0 finished]: Input [0 rows 0.0B]: Elapsed 0.77s 0.0Bps
    Table  31.1: Splits [648 total, 253 pending, 15 running, 380 finished]: Input [2,659,640 rows 67.3MB]: Elapsed 0.77s 62.6MBps
                                                                               X

Query 32: RUNNING, 89 nodes
Splits: 648 total, 252 pending, 16 running, 380 finished
CPU wall: 11.45s 4.2MBps total, 9.45s 8.2MBps per node
CPU user: 11.45s 4.2MBps total, 9.45s 8.2MBps per node
Mem: 1949M shared, 7594M private

31.0 [S] i[2,659,640 67.3MB 62.7MBps] o[35 6.1KB 1KBps] tasks[252/16/380]
  31.1 [R] i[2,659,640 67.3MB 62.7MBps] o[35 6.1KB 1KBps] tasks[252/16/380]
    31.0 [F] i[2,659,640 67.3MB 62.7MBps] o[35 6.1KB 1KBps] tasks[252/16/380]
  31.1 [R] i[2,659,640 67.3MB 62.7MBps] o[35 6.1KB 1KBps] tasks[252/16/380]
    31.0 [F] i[2,659,640 67.3MB 62.7MBps] o[35 6.1KB 1KBps] tasks[252/16/380]

STAGE     S   IROWS      ISIZE   IRATE    OROWS   OSIZE  ORATE
31.0      Q   2,659,640  67.3MB  62.7MBps 35      6.1KB  1KBps 252/16/380]
  31.2    R   2,659,640  67.3MB  62.7MBps 35      6.1KB  1KBps 252/16/380]
    31.3  F   2,659,640  67.3MB  62.7MBps 35      6.1KB  1KBps 252/16/380]
  31.4    R   2,659,640  67.3MB  62.7MBps 35      6.1KB  1KBps 252/16/380]
    31.5  F   2,659,640  67.3MB  62.7MBps 35      6.1KB  1KBps 252/16/380]


Query 39: Splits 648: In 6,528,597 rows 171.0MB: Out 329 rows 10.7kB: CPU Time 49.88s 3.4MBps: Elapsed 3.26s 52.4MBps
Query 39: FINISHED i[2,659,640 67.3MB 62.7MBps] o[35 6.1KB 1KBps] tasks[252/16/380]

Query 32: FINISHED, 89 nodes, 648 splits
CPU wall: 11.45s 4.2MBps total, 9.45s 8.2MBps per node
CPU user: 11.45s 4.2MBps total, 9.45s 8.2MBps per node

 */

    public void printInitialStatusUpdates()
    {
        try {
            hideCursor();
            while (true) {
                try {
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

                    QueryInfo queryInfo = queryClient.getQueryInfo(false);

                    // if query is no longer running, finish
                    if (queryInfo == null || queryInfo.getState().isDone()) {
                        return;
                    }

                    // check if there is there is pending output
                    if (queryInfo.getOutputStage() != null) {
                        List<TaskInfo> outStage = queryInfo.getOutputStage().getTasks();
                        for (TaskInfo outputTask : outStage) {
                            for (PageBufferInfo outputBuffer : outputTask.getOutputBuffers()) {
                                if (outputBuffer.getBufferedPages() > 0) {
                                    return;
                                }
                            }
                        }
                    }

                    resetScreen();
                    printQueryInfo(queryInfo);
                }
                catch (Exception ignored) {
                }
            }
        }
        finally {
            resetScreen();
            showCursor();
        }
    }

    public void printFinalInfo(OutputStats stats)
    {
        Duration elapsedTime = Duration.nanosSince(start);

        QueryInfo queryInfo = queryClient.getQueryInfo(true);

        ExecutionStats executionStats = new ExecutionStats();
        StageInfo outputStage = queryInfo.getOutputStage();

        sumStats(outputStage, executionStats);
        int nodes = uniqueNodes(outputStage).size();

        // blank line
        out.println();

        // Query 32: FINISHED, 89 nodes, 648 splits
        String querySummary = String.format("Query %s: %s, %,d nodes, %,d splits",
                queryInfo.getQueryId(),
                queryInfo.getState(),
                nodes,
                executionStats.getSplits());
        out.println(querySummary);

        // CPU wall: 11.45s 4.2MBps total, 9.45s 8.2MBps per node
        Duration wallTime = new Duration(elapsedTime.toMillis() * nodes, TimeUnit.MILLISECONDS);
        Duration wallTimePerNode = elapsedTime;
        String cpuWallSummary = String.format("CPU wall: %s %s total, %s %s per node",
                wallTime.toString(TimeUnit.SECONDS),
                formatDataRate(executionStats.getCompletedDataSize(), wallTime),
                wallTimePerNode.toString(TimeUnit.SECONDS),
                formatDataRate(executionStats.getCompletedDataSize(), wallTimePerNode));
        out.println(cpuWallSummary);

        // CPU user: 11.45s 4.2MBps wall, 9.45s 8.2MBps user, 9.45s 8.2MBps wall/node
        Duration userTime = new Duration(executionStats.getSplitCpuTime(), TimeUnit.MILLISECONDS);
        Duration userTimePerNode = new Duration(userTime.toMillis() / nodes, TimeUnit.MILLISECONDS);
        String cpuUserSummary = String.format("CPU user: %s %s total, %s %s per node",
                userTime.toString(TimeUnit.SECONDS),
                formatDataRate(executionStats.getCompletedDataSize(), userTime),
                userTimePerNode.toString(TimeUnit.SECONDS),
                formatDataRate(executionStats.getCompletedDataSize(), userTimePerNode));
        out.println(cpuUserSummary);

        // blank line
        out.println();
    }

    private void printQueryInfo(QueryInfo queryInfo)
    {
        Duration elapsedTime = Duration.nanosSince(start);

        ExecutionStats executionStats = new ExecutionStats();
        StageInfo outputStage = queryInfo.getOutputStage();

        sumStats(outputStage, executionStats);
        int nodes = uniqueNodes(outputStage).size();

        if (REAL_TERMINAL) {
            // Query 143: RUNNING, 39 nodes
            String querySummary = String.format("Query %s: %s, %,d nodes", queryInfo.getQueryId(), queryInfo.getState(), nodes);
            reprintLine(querySummary);

            // Splits: 648 total, 252 pending, 16 running, 380 finished
            String splitsSummary = String.format("Splits: %,d total, %,d pending, %,d running, %,d finished",
                    executionStats.getSplits(),
                    max(0, executionStats.getSplits() - executionStats.getStartedSplits()),
                    max(0, executionStats.getStartedSplits() - executionStats.getCompletedSplits()),
                    executionStats.getCompletedSplits());
            reprintLine(splitsSummary);

            // CPU wall: 11.45s 4.2MBps total, 9.45s 8.2MBps per node
            Duration wallTime = new Duration(elapsedTime.toMillis() * nodes, TimeUnit.MILLISECONDS);
            Duration wallTimePerNode = elapsedTime;
            String cpuWallSummary = String.format("CPU wall: %s %s total, %s %s per node",
                    wallTime.toString(TimeUnit.SECONDS),
                    formatDataRate(executionStats.getCompletedDataSize(), wallTime),
                    wallTimePerNode.toString(TimeUnit.SECONDS),
                    formatDataRate(executionStats.getCompletedDataSize(), wallTimePerNode));
            reprintLine(cpuWallSummary);

            // CPU user: 11.45s 4.2MBps wall, 9.45s 8.2MBps user, 9.45s 8.2MBps wall/node
            Duration userTime = new Duration(executionStats.getSplitCpuTime(), TimeUnit.MILLISECONDS);
            Duration userTimePerNode = new Duration(userTime.toMillis() / nodes, TimeUnit.MILLISECONDS);
            String cpuUserSummary = String.format("CPU user: %s %s total, %s %s per node",
                    userTime.toString(TimeUnit.SECONDS),
                    formatDataRate(executionStats.getCompletedDataSize(), userTime),
                    userTimePerNode.toString(TimeUnit.SECONDS),
                    formatDataRate(executionStats.getCompletedDataSize(), userTimePerNode));
            reprintLine(cpuUserSummary);

            // todo Mem: 1949M shared, 7594M private

            // blank line
            reprintLine("");

            printStageTree(outputStage, "");
        }
        else {
            // Query 31 [S] i[2,659,640 67.3MB 62.7MBps] o[35 6.1KB 1KBps] splits[252/16/380]
            String querySummary = String.format("Query %s [%s] i[%,d %s %s] o[%,d %s %s] splits[%,d/%,d/%,d]",
                    queryInfo.getQueryId(),
                    queryInfo.getState().toString().charAt(0),

                    executionStats.getInputPositionCount(),
                    formatDataSize(executionStats.getInputDataSize()),
                    formatDataRate(executionStats.getCompletedDataSize(), elapsedTime),

                    executionStats.getOutputPositionCount(),
                    formatDataSize(executionStats.getOutputDataSize()),
                    formatDataRate(executionStats.getOutputDataSize(), elapsedTime),

                    max(0, executionStats.getSplits() - executionStats.getStartedSplits()),
                    max(0, executionStats.getStartedSplits() - executionStats.getCompletedSplits()),
                    executionStats.getCompletedSplits());
            reprintLine(querySummary);
        }
    }

    private void printStageTree(StageInfo stage, String indent)
    {
        Duration elapsedTime = Duration.nanosSince(start);

        ExecutionStats executionStats = new ExecutionStats();
        sumTaskStats(stage, executionStats);

        // 31.0 [S] i[2,659,640 67.3MB 62.7MBps] o[35 6.1KB 1KBps] tasks[252/16/380]
        String stageSummary = String.format("%s%s [%s] i[%,d %s %s] o[%,d %s %s] splits[%,d/%,d/%,d]",
                indent,
                stage.getStageId(),
                stage.getState().toString().charAt(0),

                executionStats.getInputPositionCount(),
                formatDataSize(executionStats.getInputDataSize()),
                formatDataRate(executionStats.getCompletedDataSize(), elapsedTime),

                executionStats.getOutputPositionCount(),
                formatDataSize(executionStats.getOutputDataSize()),
                formatDataRate(executionStats.getOutputDataSize(), elapsedTime),

                max(0, executionStats.getSplits() - executionStats.getStartedSplits()),
                max(0, executionStats.getStartedSplits() - executionStats.getCompletedSplits()),
                executionStats.getCompletedSplits());
        reprintLine(stageSummary);

        for (StageInfo subStage : stage.getSubStages()) {
            printStageTree(subStage, indent + "  ");
        }
    }

    public void sumStats(StageInfo stageInfo, ExecutionStats executionStats)
    {
        sumTaskStats(stageInfo, executionStats);
        for (StageInfo subStage : stageInfo.getSubStages()) {
            sumStats(subStage, executionStats);
        }
    }

    private void sumTaskStats(StageInfo stageInfo, ExecutionStats executionStats)
    {
        for (TaskInfo task : stageInfo.getTasks()) {
            executionStats.add(task.getStats());
        }
    }

    public Set<String> uniqueNodes(StageInfo stageInfo)
    {
        ImmutableSet.Builder<String> nodes = ImmutableSet.builder();
        for (TaskInfo task : stageInfo.getTasks()) {
            // todo add nodeId to TaskInfo
            URI uri = task.getSelf();
            nodes.add(uri.getHost() + ":" + uri.getPort());
        }

        for (StageInfo subStage : stageInfo.getSubStages()) {
            nodes.addAll(uniqueNodes(subStage));
        }
        return nodes.build();
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

    private void reprintLine(String line)
    {
        if (REAL_TERMINAL) {
            out.print(Ansi.ansi().eraseLine(Erase.ALL).a(line).a('\n').toString());
        } else {
            out.print('\r' + line);
        }
        out.flush();
        lines++;
    }

    private void resetScreen()
    {
        if (lines > 0) {
            if (REAL_TERMINAL) {
                out.print(Ansi.ansi().cursorUp(lines).eraseScreen(Erase.FORWARD).toString());
            } else {
                out.print('\r');
            }
            out.flush();
            lines = 0;
        }
    }

    private void hideCursor()
    {
        if (REAL_TERMINAL) {
            out.print(HIDE_CURSOR);
            out.flush();
        }
    }

    private void showCursor()
    {
        if (REAL_TERMINAL) {
            out.print(SHOW_CURSOR);
            out.flush();
        }
    }

    public static final boolean REAL_TERMINAL = isRealTerminal();

    private static boolean isRealTerminal()
    {

        // If the jansi.passthrough property is set, then don't interpret
        // any of the ansi sequences.
        if (Boolean.getBoolean("jansi.passthrough")) {
            return true;
        }

        // If the jansi.strip property is set, then we just strip the
        // the ansi escapes.
        if (Boolean.getBoolean("jansi.strip")) {
            return false;
        }

        String os = System.getProperty("os.name");
        if (os.startsWith("Windows")) {
            // We could support this, but we'd need a windows box
            return true;
        }

        // We must be on some unix variant..
        try {
            // check if standard out is a terminal
            int rc = isatty(STDOUT_FILENO);
            if (rc == 0) {
                return false;
            }

        }
        catch (NoClassDefFoundError | UnsatisfiedLinkError ignore) {
            // These errors happen if the JNI lib is not available for your platform.
        }
        return true;
    }

}
