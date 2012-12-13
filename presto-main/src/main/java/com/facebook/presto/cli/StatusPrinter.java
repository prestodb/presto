package com.facebook.presto.cli;

import com.facebook.presto.execution.ExecutionStats;
import com.facebook.presto.execution.PageBufferInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.server.HttpQueryClient;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.units.Duration;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.Ansi.Erase;

import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.operator.OutputProcessor.OutputStats;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.fusesource.jansi.internal.CLibrary.STDOUT_FILENO;
import static org.fusesource.jansi.internal.CLibrary.isatty;

public class StatusPrinter
{
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

Query 47: RUNNING, 3 nodes
Splits: 1,350 total  12,623,941 pending  12,623,941 running, 12,623,941 finished
CPU wall: 9.18s 20M total, 3.06s 60M per node
CPU user: 134.47s 1M total, 44.82s 4M per node
Mem: 1949M shared, 7594M private

STAGE  S    ROWS    RPS  BYTES    BPS   PEND    RUN   DONE
0......Q     26M  9077M  9993G  9077M  9077M  9077M  9077M
  2....R     17K   627M   673M   627M   627M   627M   627M
    3..C   9990    627M   673M   627M   627M   627M   627M
  4....R     26M   627M   673T   627M   627M   627M   627M
    5..F     29T   627M   673M   627M   627M   627M   627M

Query 32: FINISHED, 89 nodes, 648 splits
CPU wall: 11.45s 4.2MBps total, 9.45s 8.2MBps per node
CPU user: 11.45s 4.2MBps total, 9.45s 8.2MBps per node

 */

    public void printInitialStatusUpdates()
    {
        long lastPrint = System.nanoTime();
        try {
            while (true) {
                try {

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

                    if (Duration.nanosSince(lastPrint).convertTo(SECONDS) >= 0.5) {
                        repositionCursor();
                        printQueryInfo(queryInfo);
                        lastPrint = System.nanoTime();
                    }
                    Uninterruptibles.sleepUninterruptibly(100, MILLISECONDS);
                }
                catch (Exception ignored) {
                }
            }
        }
        finally {
            resetScreen();
        }
    }

    public void printFinalInfo(OutputStats stats)
    {
        Duration elapsedTime = Duration.nanosSince(start);

        QueryInfo queryInfo = queryClient.getQueryInfo(true);
        QueryStats queryStats = queryInfo.getQueryStats();

        StageInfo outputStage = queryInfo.getOutputStage();

        // only include input (leaf) stages
        ExecutionStats inputExecutionStats = new ExecutionStats();
        sumStats(outputStage, inputExecutionStats, true);

        // sum all stats (used for global stats like cpu time)
        ExecutionStats globalExecutionStats = new ExecutionStats();
        sumStats(outputStage, globalExecutionStats, false);

        int nodes = uniqueNodes(outputStage).size();

        // blank line
        out.println();

        // Query 32: FINISHED, 89 nodes, 648 splits
        String querySummary = String.format("Query %s: %s, %,d nodes, %,d splits",
                queryInfo.getQueryId(),
                queryInfo.getState(),
                nodes,
                queryStats.getSplits());
        out.println(querySummary);

        // CPU user: 11.45s 4.2MBps wall, 9.45s 8.2MBps user, 9.45s 8.2MBps wall/node
        Duration userTime = new Duration(globalExecutionStats.getSplitCpuTime(), MILLISECONDS);
        Duration userTimePerNode = new Duration(userTime.toMillis() / nodes, MILLISECONDS);
        long completedDataSizePerNode = inputExecutionStats.getCompletedDataSize() / nodes;
        String cpuUserSummary = String.format("CPU user: %s %s total, %s %s per node",
                userTime.toString(SECONDS),
                formatDataRate(inputExecutionStats.getCompletedDataSize(), userTime, true),
                userTimePerNode.toString(SECONDS),
                formatDataRate(completedDataSizePerNode, userTimePerNode, true));
        out.println(cpuUserSummary);

        // CPU wall: 11.45s 4.2MBps total, 9.45s 8.2MBps per node
        Duration wallTime = elapsedTime;
        Duration wallTimePerNode = new Duration(wallTime.toMillis() / nodes, MILLISECONDS);
        String cpuWallSummary = String.format("CPU wall: %s %s total, %s %s per node",
                wallTime.toString(SECONDS),
                formatDataRate(inputExecutionStats.getCompletedDataSize(), wallTime, true),
                wallTimePerNode.toString(SECONDS),
                formatDataRate(completedDataSizePerNode, wallTime, true));
        out.println(cpuWallSummary);

        // blank line
        out.println();
    }

    private void printQueryInfo(QueryInfo queryInfo)
    {
        Duration elapsedTime = Duration.nanosSince(start);

        StageInfo outputStage = queryInfo.getOutputStage();

        // only include input (leaf) stages
        ExecutionStats inputExecutionStats = new ExecutionStats();
        sumStats(outputStage, inputExecutionStats, true);

        ExecutionStats globalExecutionStats = new ExecutionStats();
        sumStats(outputStage, globalExecutionStats, false);

        int nodes = uniqueNodes(outputStage).size();
        long completedDataSizePerNode = inputExecutionStats.getCompletedDataSize() / nodes;

        if (REAL_TERMINAL) {
            // Query 143: RUNNING, 39 nodes, 84.3s elapsed
            Duration wallTime = elapsedTime;
            String querySummary = String.format("Query %s: %s, %,d nodes, %.1fs elapsed",
                    queryInfo.getQueryId(),
                    queryInfo.getState(),
                    nodes,
                    wallTime.convertTo(SECONDS));
            reprintLine(querySummary);

            // Splits: 648 total, 252 pending, 16 running, 380 finished
            String splitsSummary = String.format("Splits: %,4d total, %,4d pending, %,4d running, %,4d done",
                    globalExecutionStats.getSplits(),
                    max(0, globalExecutionStats.getSplits() - globalExecutionStats.getStartedSplits()),
                    max(0, globalExecutionStats.getSplits() - globalExecutionStats.getCompletedSplits()),
                    globalExecutionStats.getCompletedSplits());
            reprintLine(splitsSummary);

            // CPU user: 11.45s 4.2MBps wall, 9.45s 8.2MBps user, 9.45s 8.2MBps wall/node
            Duration userTime = new Duration(globalExecutionStats.getSplitCpuTime(), MILLISECONDS);
            Duration userTimePerNode = new Duration(userTime.toMillis() / nodes, MILLISECONDS);
            String cpuUserSummary = String.format("CPU user: %5.1fs %7s total, %5.1fs %7s per node",
                    userTime.convertTo(SECONDS),
                    formatDataRate(inputExecutionStats.getCompletedDataSize(), userTime, true),
                    userTimePerNode.convertTo(SECONDS),
                    formatDataRate(completedDataSizePerNode, userTime, true));
            reprintLine(cpuUserSummary);

            // CPU wall: 11.45s 4.2MBps total, 9.45s 8.2MBps per node
            Duration wallTimePerNode = new Duration(wallTime.toMillis() / nodes, MILLISECONDS);
            String cpuWallSummary = String.format("CPU wall: %5.1fs %7s total, %5.1fs %7s per node",
                    wallTime.convertTo(SECONDS),
                    formatDataRate(inputExecutionStats.getCompletedDataSize(), wallTime, true),
                    wallTimePerNode.convertTo(SECONDS),
                    formatDataRate(completedDataSizePerNode, wallTime, true));
            reprintLine(cpuWallSummary);

            // todo Mem: 1949M shared, 7594M private

            // blank line
            reprintLine("");

            // STAGE  S    ROWS    RPS  BYTES    BPS   PEND    RUN   DONE
            String stagesHeader = String.format("%10s%1s  %5s  %5s  %5s  %5s  %5s  %5s %5s",
                    "STAGE",
                    "S",
                    "ROWS",
                    "ROW/S",
                    "BYTES",
                    "B/S",
                    "PEND",
                    "RUN",
                    "DONE");
            reprintLine(stagesHeader);

            printStageTree(outputStage, "");
        }
        else {
            // Query 31 [S] i[2,659,640 67.3MB 62.7MBps] o[35 6.1KB 1KBps] splits[252/16/380]
            String querySummary = String.format("Query %s [%s] i[%,d %s %s] o[%,d %s %s] splits[%,d/%,d/%,d]",
                    queryInfo.getQueryId(),
                    queryInfo.getState().toString().charAt(0),

                    globalExecutionStats.getInputPositionCount(),
                    formatDataSize(globalExecutionStats.getInputDataSize()),
                    formatDataRate(globalExecutionStats.getCompletedDataSize(), elapsedTime, false),

                    globalExecutionStats.getOutputPositionCount(),
                    formatDataSize(globalExecutionStats.getOutputDataSize()),
                    formatDataRate(globalExecutionStats.getOutputDataSize(), elapsedTime, false),

                    max(0, globalExecutionStats.getSplits() - globalExecutionStats.getStartedSplits()),
                    max(0, globalExecutionStats.getSplits() - globalExecutionStats.getCompletedSplits()),
                    globalExecutionStats.getCompletedSplits());
            reprintLine(querySummary);
        }
    }

    private void printStageTree(StageInfo stage, String indent)
    {
        Duration elapsedTime = Duration.nanosSince(start);

        ExecutionStats executionStats = new ExecutionStats();
        sumTaskStats(stage, executionStats);

        // STAGE  S    ROWS  ROW/S  BYTES    B/S   PEND    RUN   DONE
        // 0......Q     26M  9077M  9993G  9077M  9077M  9077M  9077M
        //   2....R     17K   627M   673M   627M   627M   627M   627M
        //     3..C   9990    627M   673M   627M   627M   627M   627M
        //   4....R     26M   627M   673T   627M   627M   627M   627M
        //     5..F     29T   627M   673M   627M   627M   627M   627M

        // todo this is a total hack
        String id = stage.getStageId().substring(stage.getQueryId().length() + 1);
        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(indent).append(id);
        while (nameBuilder.length() < 10) {
            nameBuilder.append('.');
        }

        String stageSummary = String.format("%10s%1s  %5s  %5s  %5s  %5s  %5s  %5s %5s",
                nameBuilder.toString(),
                stage.getState().toString().charAt(0),

                formatCount(executionStats.getInputPositionCount()),
                formatCountRate(executionStats.getInputPositionCount(), elapsedTime, false),

                formatDataSize(executionStats.getInputDataSize()),
                formatDataRate(executionStats.getCompletedDataSize(), elapsedTime, false),

                max(0, executionStats.getSplits() - executionStats.getStartedSplits()),
                max(0, executionStats.getSplits() - executionStats.getCompletedSplits()),
                executionStats.getCompletedSplits());
        reprintLine(stageSummary);

        for (StageInfo subStage : stage.getSubStages()) {
            printStageTree(subStage, indent + "  ");
        }
    }

    public void sumStats(StageInfo stageInfo, ExecutionStats executionStats, boolean sumLeafOnly)
    {
        if (!sumLeafOnly || stageInfo.getSubStages().isEmpty()) {
            sumTaskStats(stageInfo, executionStats);
        }
        for (StageInfo subStage : stageInfo.getSubStages()) {
            sumStats(subStage, executionStats, sumLeafOnly);
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

    private static String formatCount(double count)
    {
        String unit = " ";
        if (count > 1000) {
            count /= 1000;
            unit = "K";
        }
        if (count > 1000) {
            count /= 1000;
            unit = "M";
        }
        if (count > 1000) {
            count /= 1000;
            unit = "B";
        }
        if (count > 1000) {
            count /= 1000;
            unit = "T";
        }
        if (count > 1000) {
            count /= 1000;
            unit = "Q";
        }
        return String.format("%d%s", (long) count, unit);
    }

    private static String formatCountRate(double count, Duration duration, boolean longForm)
    {
        double rate = count / duration.convertTo(SECONDS);
        if (Double.isNaN(rate) || Double.isInfinite(rate)) {
            rate = 0;
        }

        String rateString = formatCount(rate);
        if (longForm) {
            if (rateString.endsWith(" ")) {
                rateString = rateString.substring(0, rateString.length() - 1);
            }
            rateString += "ps";
        }
        return rateString;
    }

    private static String formatDataSize(double dataSize)
    {
        String unit = "B";
        if (dataSize > 1000) {
            dataSize /= 1024;
            unit = "K";
        }
        if (dataSize > 1000) {
            dataSize /= 1024;
            unit = "M";
        }
        if (dataSize > 1000) {
            dataSize /= 1024;
            unit = "G";
        }
        if (dataSize > 1000) {
            dataSize /= 1024;
            unit = "T";
        }
        if (dataSize > 1000) {
            dataSize /= 1024;
            unit = "P";
        }
        return String.format("%d%s", (long) dataSize, unit);
    }

    private static String formatDataRate(double dataSize, Duration duration, boolean longForm)
    {
        double rate = dataSize / duration.convertTo(SECONDS);
        if (Double.isNaN(rate) || Double.isInfinite(rate)) {
            rate = 0;
        }

        String rateString = formatDataSize(rate);
        if (longForm) {
            if (!rateString.endsWith("B")) {
                rateString += "B";
            }
            rateString += "ps";
        }
        return rateString;
    }

    private void reprintLine(String line)
    {
        if (REAL_TERMINAL) {
            out.print(Ansi.ansi().eraseLine(Erase.ALL).a(line).a('\n').toString());
        }
        else {
            out.print('\r' + line);
        }
        out.flush();
        lines++;
    }

    private void repositionCursor()
    {
        if (lines > 0) {
            if (REAL_TERMINAL) {
                out.print(Ansi.ansi().cursorUp(lines).toString());
            }
            else {
                out.print('\r');
            }
            out.flush();
            lines = 0;
        }
    }

    private void resetScreen()
    {
        if (lines > 0) {
            if (REAL_TERMINAL) {
                out.print(Ansi.ansi().cursorUp(lines).eraseScreen(Erase.FORWARD).toString());
            }
            else {
                out.print('\r');
            }
            out.flush();
            lines = 0;
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
