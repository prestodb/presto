package com.facebook.presto.cli;

import com.facebook.presto.execution.ExecutionStats;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageState;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.PrintStream;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.cli.FormatUtils.formatCount;
import static com.facebook.presto.cli.FormatUtils.formatCountRate;
import static com.facebook.presto.cli.FormatUtils.formatDataRate;
import static com.facebook.presto.cli.FormatUtils.formatDataSize;
import static com.facebook.presto.cli.FormatUtils.formatProgressBar;
import static com.facebook.presto.cli.FormatUtils.formatTime;
import static com.facebook.presto.cli.FormatUtils.pluralize;
import static com.facebook.presto.execution.StageInfo.globalExecutionStats;
import static com.facebook.presto.execution.StageInfo.stageOnlyExecutionStats;
import static com.facebook.presto.execution.StageInfo.stageStateGetter;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StatusPrinter
{
    private static final Logger log = Logger.get(StatusPrinter.class);

    private final long start = System.nanoTime();
    private final HttpQueryClient queryClient;
    private final PrintStream out;
    private final ConsolePrinter console;

    public StatusPrinter(HttpQueryClient queryClient, PrintStream out)
    {
        this.queryClient = queryClient;
        this.out = out;
        this.console = new ConsolePrinter(out);
    }

/*

Query 16, RUNNING, 1 node, 855 splits
http://my.server:8080/v1/query/16?pretty
Splits:   646 queued, 34 running, 175 done
CPU Time: 33.7s total,  191K rows/s, 16.6MB/s, 22% active
Per Node: 2.5 parallelism,  473K rows/s, 41.1MB/s
Parallelism: 2.5
0:13 [6.45M rows,  560MB] [ 473K rows/s, 41.1MB/s] [=========>>           ] 20%

     STAGES   ROWS  ROWS/s  BYTES  BYTES/s   PEND    RUN   DONE
0.........R  13.8M    336K  1.99G    49.5M      0      1    706
  1.......R   666K   41.5K  82.1M    5.12M    563     65     79
    2.....R  4.58M    234K   620M    31.6M    406     65    236

 */

    public void printInitialStatusUpdates()
    {
        long lastPrint = System.nanoTime();
        try {
            while (true) {
                try {
                    QueryInfo queryInfo = queryClient.getQueryInfo(false);

                    // if query is no longer running, finish
                    if ((queryInfo == null) || queryInfo.getState().isDone()) {
                        return;
                    }

                    // check if there is there is pending output
                    if (queryInfo.resultsPending()) {
                        return;
                    }

                    if (Duration.nanosSince(lastPrint).convertTo(SECONDS) >= 0.5) {
                        console.repositionCursor();
                        printQueryInfo(queryInfo);
                        lastPrint = System.nanoTime();
                    }
                    Uninterruptibles.sleepUninterruptibly(100, MILLISECONDS);
                }
                catch (Exception e) {
                    log.debug(e, "error printing status");
                }
            }
        }
        finally {
            console.resetScreen();
        }
    }

    public void printFinalInfo()
    {
        Duration wallTime = Duration.nanosSince(start);

        QueryInfo queryInfo = queryClient.getQueryInfo(true);

        StageInfo outputStage = queryInfo.getOutputStage();

        // sum all stats (used for global stats like cpu time)
        ExecutionStats globalExecutionStats = globalExecutionStats(outputStage);

        int nodes = uniqueNodes(outputStage).size();

        if (nodes == 0 || globalExecutionStats.getSplits() == 0) {
            return;
        }

        // blank line
        out.println();

        // Query 12, FINISHED, 1 node
        String querySummary = String.format("Query %s, %s, %,d %s",
                queryInfo.getQueryId(),
                queryInfo.getState(),
                nodes,
                pluralize("node", nodes));
        out.println(querySummary);

        if (queryClient.isDebug()) {
            out.println(queryClient.getQueryLocation() + "?pretty");
        }

        // Splits: 1000 total, 842 done (84.20%)
        String splitsSummary = String.format("Splits: %,d total, %,d done (%.2f%%)",
                globalExecutionStats.getSplits(),
                globalExecutionStats.getCompletedSplits(),
                Math.min(100, globalExecutionStats.getCompletedSplits() * 100.0 / globalExecutionStats.getSplits()));
        out.println(splitsSummary);

        if (queryClient.isDebug()) {
            // CPU Time: 565.2s total,   26K rows/s, 3.85MB/s
            Duration cpuTime = globalExecutionStats.getSplitCpuTime();
            String cpuTimeSummary = String.format("CPU Time: %.1fs total, %5s rows/s, %8s, %d%% active",
                    cpuTime.convertTo(SECONDS),
                    formatCountRate(globalExecutionStats.getCompletedPositionCount(), cpuTime, false),
                    formatDataRate(globalExecutionStats.getCompletedDataSize(), cpuTime, true),
                    (int) (globalExecutionStats.getSplitCpuTime().toMillis() * 100.0 / (globalExecutionStats.getSplitWallTime().toMillis() + 1))); // Add 1 to avoid divide by zero
            out.println(cpuTimeSummary);

            double parallelism = cpuTime.toMillis() / wallTime.toMillis();

            // Per Node: 3.5 parallelism, 83.3K rows/s, 0.7 MB/s
            String perNodeSummary = String.format("Per Node: %.1f parallelism, %5s rows/s, %8s",
                    parallelism / nodes,
                    formatCountRate(globalExecutionStats.getCompletedPositionCount() / nodes, wallTime, false),
                    formatDataRate(new DataSize(globalExecutionStats.getCompletedDataSize().toBytes() / nodes, BYTE), wallTime, true));
            reprintLine(perNodeSummary);

            out.println(String.format("Parallelism: %.1f", parallelism));
        }

        // 0:32 [2.12GB, 15M rows] [67MB/s, 463K rows/s]
        String statsLine = String.format("%s [%s rows, %s] [%s rows/s, %s]",
                formatTime(wallTime),
                formatCount(globalExecutionStats.getCompletedPositionCount()),
                formatDataSize(globalExecutionStats.getCompletedDataSize(), true),
                formatCountRate(globalExecutionStats.getCompletedPositionCount(), wallTime, false),
                formatDataRate(globalExecutionStats.getCompletedDataSize(), wallTime, true));

        out.println(statsLine);

        // blank line
        out.println();
    }

    private void printQueryInfo(QueryInfo queryInfo)
    {
        Duration wallTime = Duration.nanosSince(start);

        StageInfo outputStage = queryInfo.getOutputStage();

        ExecutionStats globalExecutionStats = globalExecutionStats(outputStage);

        int nodes = uniqueNodes(outputStage).size();

        if (nodes == 0 || globalExecutionStats.getSplits() == 0) {
            return ;
        }

        // cap progress at 99%, otherwise it looks weird when the query is still running and it says 100%
        int progressPercentage = min(99, (int) ((globalExecutionStats.getCompletedSplits() * 100.0) / globalExecutionStats.getSplits()));

        if (console.isRealTerminal()) {
            // blank line
            reprintLine("");

            int terminalWidth = console.getWidth();

            if (terminalWidth < 75) {
                reprintLine("WARNING: Terminal");
                reprintLine("must be at least");
                reprintLine("80 characters wide");
                reprintLine("");
                reprintLine(queryInfo.getState().toString());
                reprintLine(String.format("%s %d%%", formatTime(wallTime), progressPercentage));
                return;
            }

            // Query 10, RUNNING, 1 node, 778 splits
            String querySummary = String.format("Query %s, %s, %,d %s, %,d splits",
                    queryInfo.getQueryId(),
                    queryInfo.getState(),
                    nodes,
                    pluralize("node", nodes),
                    globalExecutionStats.getSplits());
            reprintLine(querySummary);

            if (queryClient.isDebug()) {
                reprintLine(queryClient.getQueryLocation() + "?pretty");
            }

            if (queryInfo.getState() == QueryState.PLANNING) {
                return;
            }

            if (queryClient.isDebug()) {
                // Splits:   620 queued, 34 running, 124 done
                String splitsSummary = String.format("Splits:   %,d queued, %,d running, %,d done",
                        max(0, globalExecutionStats.getSplits() - globalExecutionStats.getStartedSplits()),
                        max(0, globalExecutionStats.getStartedSplits() - globalExecutionStats.getCompletedSplits()),
                        globalExecutionStats.getCompletedSplits());
                reprintLine(splitsSummary);

                // CPU Time: 56.5s total, 36.4K rows/s, 4.44MB/s, 60% active
                Duration cpuTime = globalExecutionStats.getSplitCpuTime();
                String cpuTimeSummary = String.format("CPU Time: %.1fs total, %5s rows/s, %8s, %d%% active",
                        cpuTime.convertTo(SECONDS),
                        formatCountRate(globalExecutionStats.getCompletedPositionCount(), cpuTime, false),
                        formatDataRate(globalExecutionStats.getCompletedDataSize(), cpuTime, true),
                        (int) (globalExecutionStats.getSplitCpuTime().toMillis() * 100.0 / (globalExecutionStats.getSplitWallTime().toMillis() + 1))); // Add 1 to avoid divide by zero
                reprintLine(cpuTimeSummary);

                double parallelism = cpuTime.toMillis() / wallTime.toMillis();

                // Per Node: 3.5 parallelism, 83.3K rows/s, 0.7 MB/s
                String perNodeSummary = String.format("Per Node: %.1f parallelism, %5s rows/s, %8s",
                        parallelism / nodes,
                        formatCountRate(globalExecutionStats.getCompletedPositionCount() / nodes, wallTime, false),
                        formatDataRate(new DataSize(globalExecutionStats.getCompletedDataSize().toBytes() / nodes, BYTE), wallTime, true));
                reprintLine(perNodeSummary);

                reprintLine(String.format("Parallelism: %.1f", parallelism));
            }

            assert terminalWidth >= 75;
            int progressWidth = (min(terminalWidth, 100) - 75) + 17; // progress bar is 17-42 characters wide


            boolean noMoreSplits = IterableTransformer.on(StageInfo.getAllStages(queryInfo.getOutputStage()))
                    .transform(stageStateGetter())
                    .all(isStageRunningOrDone());

            if (noMoreSplits) {
                String progressBar = formatProgressBar(progressWidth,
                        globalExecutionStats.getCompletedSplits(),
                        max(0, globalExecutionStats.getStartedSplits() - globalExecutionStats.getCompletedSplits()),
                        globalExecutionStats.getSplits());

                // 0:17 [ 103MB,  802K rows] [5.74MB/s, 44.9K rows/s] [=====>>                                   ] 10%
                String progressLine = String.format("%s [%5s rows, %6s] [%5s rows/s, %8s] [%s] %d%%",
                        formatTime(wallTime),
                        formatCount(globalExecutionStats.getCompletedPositionCount()),
                        formatDataSize(globalExecutionStats.getCompletedDataSize(), true),
                        formatCountRate(globalExecutionStats.getCompletedPositionCount(), wallTime, false),
                        formatDataRate(globalExecutionStats.getCompletedDataSize(), wallTime, true),
                        progressBar,
                        progressPercentage);

                reprintLine(progressLine);
            }
            else {
                String progressBar = formatProgressBar(progressWidth, (int) (Duration.nanosSince(start).convertTo(TimeUnit.SECONDS)));

                // 0:17 [ 103MB,  802K rows] [5.74MB/s, 44.9K rows/s] [    <=>                                  ]
                String progressLine = String.format("%s [%5s rows, %6s] [%5s rows/s, %8s] [%s]",
                        formatTime(wallTime),
                        formatCount(globalExecutionStats.getCompletedPositionCount()),
                        formatDataSize(globalExecutionStats.getCompletedDataSize(), true),
                        formatCountRate(globalExecutionStats.getCompletedPositionCount(), wallTime, false),
                        formatDataRate(globalExecutionStats.getCompletedDataSize(), wallTime, true),
                        progressBar);

                reprintLine(progressLine);
            }

            // todo Mem: 1949M shared, 7594M private

            // blank line
            reprintLine("");

            // STAGE  S    ROWS    RPS  BYTES    BPS   QUEUED    RUN   DONE
            String stagesHeader = String.format("%10s%1s  %5s  %6s  %5s  %7s  %6s  %5s  %5s",
                    "STAGE",
                    "S",
                    "ROWS",
                    "ROWS/s",
                    "BYTES",
                    "BYTES/s",
                    "QUEUED",
                    "RUN",
                    "DONE");
            reprintLine(stagesHeader);

            printStageTree(outputStage, "");
        }
        else {
            // Query 31 [S] i[2.7M 67.3MB 62.7MBps] o[35 6.1KB 1KBps] splits[252/16/380]
            String querySummary = String.format("Query %s [%s] i[%s %s %s] o[%s %s %s] splits[%,d/%,d/%,d]",
                    queryInfo.getQueryId(),
                    queryInfo.getState().toString().charAt(0),

                    formatCount(globalExecutionStats.getCompletedPositionCount()),
                    formatDataSize(globalExecutionStats.getCompletedDataSize(), false),
                    formatDataRate(globalExecutionStats.getCompletedDataSize(), wallTime, false),

                    formatCount(globalExecutionStats.getOutputPositionCount()),
                    formatDataSize(globalExecutionStats.getOutputDataSize(), false),
                    formatDataRate(globalExecutionStats.getOutputDataSize(), wallTime, false),

                    max(0, globalExecutionStats.getSplits() - globalExecutionStats.getStartedSplits()),
                    max(0, globalExecutionStats.getStartedSplits() - globalExecutionStats.getCompletedSplits()),
                    globalExecutionStats.getCompletedSplits());
            reprintLine(querySummary);
        }
    }

    private void printStageTree(StageInfo stage, String indent)
    {
        Duration elapsedTime = Duration.nanosSince(start);

        ExecutionStats executionStats = stageOnlyExecutionStats(stage);

        // STAGE  S    ROWS  ROWS/s  BYTES  BYTES/s  QUEUED    RUN   DONE
        // 0......Q     26M   9077M  9993G    9077M   9077M  9077M  9077M
        //   2....R     17K    627M   673M     627M    627M   627M   627M
        //     3..C     999    627M   673M     627M    627M   627M   627M
        //   4....R     26M    627M   673T     627M    627M   627M   627M
        //     5..F     29T    627M   673M     627M    627M   627M   627M

        // todo this is a total hack
        String id = stage.getStageId().substring(stage.getQueryId().length() + 1);
        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(indent).append(id);
        while (nameBuilder.length() < 10) {
            nameBuilder.append('.');
        }

        StageState state = stage.getState();
        String bytesPerSecond;
        String rowsPerSecond;
        if (state.isDone()) {
            bytesPerSecond = formatDataRate(new DataSize(0, BYTE), new Duration(0, SECONDS), false);
            rowsPerSecond = formatCountRate(0, new Duration(0, SECONDS), false);
        }
        else {
            bytesPerSecond = formatDataRate(executionStats.getCompletedDataSize(), elapsedTime, false);
            rowsPerSecond = formatCountRate(executionStats.getCompletedPositionCount(), elapsedTime, false);
        }

        String stageSummary = String.format("%10s%1s  %5s  %6s  %5s  %7s  %6s  %5s  %5s",
                nameBuilder.toString(),
                state.toString().charAt(0),

                formatCount(executionStats.getCompletedPositionCount()),
                rowsPerSecond,

                formatDataSize(executionStats.getCompletedDataSize(), false),
                bytesPerSecond,

                executionStats.getQueuedSplits(),
                executionStats.getRunningSplits(),
                executionStats.getCompletedSplits());
        reprintLine(stageSummary);

        for (StageInfo subStage : stage.getSubStages()) {
            printStageTree(subStage, indent + "  ");
        }
    }

    private void reprintLine(String line)
    {
        console.reprintLine(line);
    }

    private static Set<String> uniqueNodes(StageInfo stageInfo)
    {
        if (stageInfo == null) {
            return ImmutableSet.of();
        }
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

    private static Predicate<StageState> isStageRunningOrDone()
    {
        return new Predicate<StageState>() {
            @Override
            public boolean apply(StageState stageState)
            {
                return stageState == StageState.RUNNING || stageState.isDone();
            }
        };
    }

}
