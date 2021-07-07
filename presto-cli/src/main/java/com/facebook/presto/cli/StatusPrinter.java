/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cli;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.client.QueryStatusInfo;
import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.client.StatementStats;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.PrintStream;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.cli.FormatUtils.formatCount;
import static com.facebook.presto.cli.FormatUtils.formatCountRate;
import static com.facebook.presto.cli.FormatUtils.formatDataRate;
import static com.facebook.presto.cli.FormatUtils.formatDataSize;
import static com.facebook.presto.cli.FormatUtils.formatProgressBar;
import static com.facebook.presto.cli.FormatUtils.formatTime;
import static com.facebook.presto.cli.FormatUtils.pluralize;
import static com.facebook.presto.cli.KeyReader.readKey;
import static com.google.common.base.Verify.verify;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.Character.toUpperCase;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.IntStream.range;

public class StatusPrinter
{
    private static final Logger log = Logger.get(StatusPrinter.class);

    private static final int CTRL_C = 3;
    private static final int CTRL_P = 16;

    private final long start = System.nanoTime();
    private final StatementClient client;
    private final PrintStream out;
    private final ConsolePrinter console;

    private boolean debug;

    public StatusPrinter(StatementClient client, PrintStream out, boolean debug)
    {
        this.client = client;
        this.out = out;
        this.console = new ConsolePrinter(out);
        this.debug = debug;
    }

/*

Query 16, RUNNING, 1 node, 855 splits
http://my.server:8080/v1/query/16?pretty
Splits:   646 queued, 34 running, 175 done
CPU Time: 33.7s total,  191K rows/s, 16.6MB/s, 22% active
Per Node: 2.5 parallelism,  473K rows/s, 41.1MB/s
Parallelism: 2.5
Peak Memory: 1.97GB
Spilled: 20GB
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
            WarningsPrinter warningsPrinter = new ConsoleWarningsPrinter(console);
            while (client.isRunning()) {
                try {
                    // exit status loop if there is pending output
                    if (client.currentData().getData() != null) {
                        return;
                    }

                    // check if time to update screen
                    boolean update = nanosSince(lastPrint).getValue(SECONDS) >= 0.5;

                    // check for keyboard input
                    int key = readKey();
                    if (key == CTRL_P) {
                        client.cancelLeafStage();
                    }
                    else if (key == CTRL_C) {
                        updateScreen(warningsPrinter);
                        update = false;
                        client.close();
                    }
                    else if (toUpperCase(key) == 'D') {
                        debug = !debug;
                        console.resetScreen();
                        update = true;
                    }

                    // update screen
                    if (update) {
                        updateScreen(warningsPrinter);
                        lastPrint = System.nanoTime();
                    }

                    // fetch next results (server will wait for a while if no data)
                    client.advance();
                }
                catch (RuntimeException e) {
                    log.debug(e, "error printing status");
                    if (debug) {
                        e.printStackTrace(out);
                    }
                }
            }
        }
        finally {
            console.resetScreen();
        }
    }

    private void updateScreen(WarningsPrinter warningsPrinter)
    {
        console.repositionCursor();
        printQueryInfo(client.currentStatusInfo(), warningsPrinter);
    }

    public void printFinalInfo()
    {
        Duration wallTime = nanosSince(start);

        QueryStatusInfo results = client.finalStatusInfo();
        StatementStats stats = results.getStats();

        int nodes = stats.getNodes();
        if ((nodes == 0) || (stats.getTotalSplits() == 0)) {
            return;
        }

        // blank line
        out.println();

        // Query 12, FINISHED, 1 node
        String querySummary = format("Query %s, %s, %,d %s",
                results.getId(),
                stats.getState(),
                nodes,
                pluralize("node", nodes));
        out.println(querySummary);

        if (debug) {
            out.println(results.getInfoUri().toString());
        }

        // Splits: 1000 total, 842 done (84.20%)
        String splitsSummary = format("Splits: %,d total, %,d done (%.2f%%)",
                stats.getTotalSplits(),
                stats.getCompletedSplits(),
                stats.getProgressPercentage().orElse(0.0));
        out.println(splitsSummary);

        if (debug) {
            // CPU Time: 565.2s total,   26K rows/s, 3.85MB/s
            Duration cpuTime = millis(stats.getCpuTimeMillis());
            String cpuTimeSummary = format("CPU Time: %.1fs total, %5s rows/s, %8s, %d%% active",
                    cpuTime.getValue(SECONDS),
                    formatCountRate(stats.getProcessedRows(), cpuTime, false),
                    formatDataRate(bytes(stats.getProcessedBytes()), cpuTime, true),
                    (int) percentage(stats.getCpuTimeMillis(), stats.getWallTimeMillis()));
            out.println(cpuTimeSummary);

            double parallelism = cpuTime.getValue(MILLISECONDS) / wallTime.getValue(MILLISECONDS);

            // Per Node: 3.5 parallelism, 83.3K rows/s, 0.7 MB/s
            String perNodeSummary = format("Per Node: %.1f parallelism, %5s rows/s, %8s",
                    parallelism / nodes,
                    formatCountRate((double) stats.getProcessedRows() / nodes, wallTime, false),
                    formatDataRate(bytes(stats.getProcessedBytes() / nodes), wallTime, true));
            reprintLine(perNodeSummary);

            // Parallelism: 5.3
            out.println(format("Parallelism: %.1f", parallelism));

            // Peak User Memory: 1.97GB
            reprintLine("Peak User Memory: " + formatDataSize(bytes(stats.getPeakMemoryBytes()), true));
            // Peak Total Memory: 1.98GB
            reprintLine("Peak Total Memory: " + formatDataSize(bytes(stats.getPeakTotalMemoryBytes()), true));
            // Peak Task Total Memory: 1.99GB
            reprintLine("Peak Task Total Memory: " + formatDataSize(bytes(stats.getPeakTaskTotalMemoryBytes()), true));

            // Spilled Data: 20GB
            if (stats.getSpilledBytes() > 0) {
                reprintLine("Spilled: " + formatDataSize(bytes(stats.getSpilledBytes()), true));
            }

            // bytesFromCache: sum=2K count=2 min=1K max=1K
            if (stats.getRuntimeStats() != null) {
                stats.getRuntimeStats().getMetrics().values().forEach(
                        metric -> reprintLine(
                                metric.getName() + ": sum=" + formatCount(metric.getSum()) + " count=" + formatCount(metric.getCount()) + " min=" + formatCount(metric.getMin()) + " max=" + formatCount(metric.getMax())));
            }
        }

        // 0:32 [2.12GB, 15M rows] [67MB/s, 463K rows/s]
        String statsLine = format("%s [%s rows, %s] [%s rows/s, %s]",
                formatTime(wallTime),
                formatCount(stats.getProcessedRows()),
                formatDataSize(bytes(stats.getProcessedBytes()), true),
                formatCountRate(stats.getProcessedRows(), wallTime, false),
                formatDataRate(bytes(stats.getProcessedBytes()), wallTime, true));

        out.println(statsLine);

        // blank line
        out.println();
    }

    private void printQueryInfo(QueryStatusInfo results, WarningsPrinter warningsPrinter)
    {
        StatementStats stats = results.getStats();
        Duration wallTime = nanosSince(start);

        // cap progress at 99%, otherwise it looks weird when the query is still running and it says 100%
        int progressPercentage = (int) min(99, stats.getProgressPercentage().orElse(0.0));

        if (console.isRealTerminal()) {
            // blank line
            reprintLine("");

            int terminalWidth = console.getWidth();

            if (terminalWidth < 75) {
                reprintLine("WARNING: Terminal");
                reprintLine("must be at least");
                reprintLine("80 characters wide");
                reprintLine("");
                reprintLine(stats.getState());
                reprintLine(format("%s %d%%", formatTime(wallTime), progressPercentage));
                return;
            }

            int nodes = stats.getNodes();

            // Query 10, RUNNING, 1 node, 778 splits
            String querySummary = format("Query %s, %s, %,d %s, %,d splits",
                    results.getId(),
                    stats.getState(),
                    nodes,
                    pluralize("node", nodes),
                    stats.getTotalSplits());
            reprintLine(querySummary);

            String url = results.getInfoUri().toString();
            if (debug && (url.length() < terminalWidth)) {
                reprintLine(url);
            }

            if ((nodes == 0) || (stats.getTotalSplits() == 0)) {
                return;
            }

            if (debug) {
                // Splits:   620 queued, 34 running, 124 done
                String splitsSummary = format("Splits:   %,d queued, %,d running, %,d done",
                        stats.getQueuedSplits(),
                        stats.getRunningSplits(),
                        stats.getCompletedSplits());
                reprintLine(splitsSummary);

                // CPU Time: 56.5s total, 36.4K rows/s, 4.44MB/s, 60% active
                Duration cpuTime = millis(stats.getCpuTimeMillis());
                String cpuTimeSummary = format("CPU Time: %.1fs total, %5s rows/s, %8s, %d%% active",
                        cpuTime.getValue(SECONDS),
                        formatCountRate(stats.getProcessedRows(), cpuTime, false),
                        formatDataRate(bytes(stats.getProcessedBytes()), cpuTime, true),
                        (int) percentage(stats.getCpuTimeMillis(), stats.getWallTimeMillis()));
                reprintLine(cpuTimeSummary);

                double parallelism = cpuTime.getValue(MILLISECONDS) / wallTime.getValue(MILLISECONDS);

                // Per Node: 3.5 parallelism, 83.3K rows/s, 0.7 MB/s
                String perNodeSummary = format("Per Node: %.1f parallelism, %5s rows/s, %8s",
                        parallelism / nodes,
                        formatCountRate((double) stats.getProcessedRows() / nodes, wallTime, false),
                        formatDataRate(bytes(stats.getProcessedBytes() / nodes), wallTime, true));
                reprintLine(perNodeSummary);

                // Parallelism: 5.3
                reprintLine(format("Parallelism: %.1f", parallelism));

                // Peak Memory: 1.97GB
                reprintLine("Peak Memory: " + formatDataSize(bytes(stats.getPeakMemoryBytes()), true));
                // Peak Total Memory: 1.98GB
                reprintLine("Peak Total Memory: " + formatDataSize(bytes(stats.getPeakTotalMemoryBytes()), true));
                // Peak Task Total Memory: 1.99GB
                reprintLine("Peak Task Total Memory: " + formatDataSize(bytes(stats.getPeakTaskTotalMemoryBytes()), true));

                // Spilled Data: 20GB
                if (stats.getSpilledBytes() > 0) {
                    reprintLine("Spilled: " + formatDataSize(bytes(stats.getSpilledBytes()), true));
                }

                if (stats.getRuntimeStats() != null) {
                    stats.getRuntimeStats().getMetrics().values().forEach(
                            metric -> reprintLine(
                                    metric.getName() + ": sum=" + metric.getSum() + " count=" + metric.getCount() + " min=" + metric.getMin() + " max=" + metric.getMax()));
                }
            }

            verify(terminalWidth >= 75); // otherwise handled above
            int progressWidth = (min(terminalWidth, 100) - 75) + 17; // progress bar is 17-42 characters wide
            String formattedWallTime = formatTime(wallTime);
            if (formattedWallTime.length() > 5) {
                // fix overflowed progress bar for queries running over 100 minutes
                progressWidth -= formattedWallTime.length() - 5;
            }

            if (stats.isScheduled()) {
                String progressBar = formatProgressBar(progressWidth,
                        stats.getCompletedSplits(),
                        max(0, stats.getRunningSplits()),
                        stats.getTotalSplits());

                // 0:17 [ 103MB,  802K rows] [5.74MB/s, 44.9K rows/s] [=====>>                                   ] 10%
                String progressLine = format("%s [%5s rows, %6s] [%5s rows/s, %8s] [%s] %d%%",
                        formattedWallTime,
                        formatCount(stats.getProcessedRows()),
                        formatDataSize(bytes(stats.getProcessedBytes()), true),
                        formatCountRate(stats.getProcessedRows(), wallTime, false),
                        formatDataRate(bytes(stats.getProcessedBytes()), wallTime, true),
                        progressBar,
                        progressPercentage);

                reprintLine(progressLine);
            }
            else {
                String progressBar = formatProgressBar(progressWidth, Ints.saturatedCast(nanosSince(start).roundTo(SECONDS)));

                // 0:17 [ 103MB,  802K rows] [5.74MB/s, 44.9K rows/s] [    <=>                                  ]
                String progressLine = format("%s [%5s rows, %6s] [%5s rows/s, %8s] [%s]",
                        formattedWallTime,
                        formatCount(stats.getProcessedRows()),
                        formatDataSize(bytes(stats.getProcessedBytes()), true),
                        formatCountRate(stats.getProcessedRows(), wallTime, false),
                        formatDataRate(bytes(stats.getProcessedBytes()), wallTime, true),
                        progressBar);

                reprintLine(progressLine);
            }

            // blank line
            reprintLine("");

            // STAGE  S    ROWS    RPS  BYTES    BPS   QUEUED    RUN   DONE
            String stagesHeader = format("%10s%1s  %5s  %6s  %5s  %7s  %6s  %5s  %5s",
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

            printStageTree(stats.getRootStage(), "", new AtomicInteger());
        }
        else {
            // Query 31 [S] i[2.7M 67.3MB 62.7MBps] o[35 6.1KB 1KBps] splits[252/16/380]
            String querySummary = format("Query %s [%s] i[%s %s %s] o[%s %s %s] splits[%,d/%,d/%,d]",
                    results.getId(),
                    stats.getState(),

                    formatCount(stats.getProcessedRows()),
                    formatDataSize(bytes(stats.getProcessedBytes()), false),
                    formatDataRate(bytes(stats.getProcessedBytes()), wallTime, false),

                    formatCount(stats.getProcessedRows()),
                    formatDataSize(bytes(stats.getProcessedBytes()), false),
                    formatDataRate(bytes(stats.getProcessedBytes()), wallTime, false),

                    stats.getQueuedSplits(),
                    stats.getRunningSplits(),
                    stats.getCompletedSplits());
            reprintLine(querySummary);
        }
        warningsPrinter.print(results.getWarnings(), false, false);
    }

    private void printStageTree(StageStats stage, String indent, AtomicInteger stageNumberCounter)
    {
        Duration elapsedTime = nanosSince(start);

        // STAGE  S    ROWS  ROWS/s  BYTES  BYTES/s  QUEUED    RUN   DONE
        // 0......Q     26M   9077M  9993G    9077M   9077M  9077M  9077M
        //   2....R     17K    627M   673M     627M    627M   627M   627M
        //     3..C     999    627M   673M     627M    627M   627M   627M
        //   4....R     26M    627M   673T     627M    627M   627M   627M
        //     5..F     29T    627M   673M     627M    627M   627M   627M

        String id = String.valueOf(stageNumberCounter.getAndIncrement());
        String name = indent + id;
        name += Strings.repeat(".", max(0, 10 - name.length()));

        String bytesPerSecond;
        String rowsPerSecond;
        if (stage.isDone()) {
            bytesPerSecond = formatDataRate(new DataSize(0, BYTE), new Duration(0, SECONDS), false);
            rowsPerSecond = formatCountRate(0, new Duration(0, SECONDS), false);
        }
        else {
            bytesPerSecond = formatDataRate(bytes(stage.getProcessedBytes()), elapsedTime, false);
            rowsPerSecond = formatCountRate(stage.getProcessedRows(), elapsedTime, false);
        }

        String stageSummary = format("%10s%1s  %5s  %6s  %5s  %7s  %6s  %5s  %5s",
                name,
                stageStateCharacter(stage.getState()),

                formatCount(stage.getProcessedRows()),
                rowsPerSecond,

                formatDataSize(bytes(stage.getProcessedBytes()), false),
                bytesPerSecond,

                stage.getQueuedSplits(),
                stage.getRunningSplits(),
                stage.getCompletedSplits());
        reprintLine(stageSummary);

        for (StageStats subStage : stage.getSubStages()) {
            printStageTree(subStage, indent + "  ", stageNumberCounter);
        }
    }

    private void reprintLine(String line)
    {
        console.reprintLine(line);
    }

    private static char stageStateCharacter(String state)
    {
        return "FAILED".equals(state) ? 'X' : state.charAt(0);
    }

    private static Duration millis(long millis)
    {
        return new Duration(millis, MILLISECONDS);
    }

    private static DataSize bytes(long bytes)
    {
        return new DataSize(bytes, BYTE);
    }

    private static double percentage(double count, double total)
    {
        if (total == 0) {
            return 0;
        }
        return min(100, (count * 100.0) / total);
    }

    private static class ConsoleWarningsPrinter
            extends AbstractWarningsPrinter
    {
        private static final int DISPLAYED_WARNINGS = 5;
        private final ConsolePrinter console;

        ConsoleWarningsPrinter(ConsolePrinter console)
        {
            super(OptionalInt.of(DISPLAYED_WARNINGS));
            this.console = requireNonNull(console, "console is null");
        }

        @Override
        protected void print(List<String> warnings)
        {
            console.reprintLine("");
            warnings.stream()
                    .forEach(console::reprintLine);

            // Remove warnings from previous screen
            range(0, DISPLAYED_WARNINGS - warnings.size())
                    .forEach(line -> console.reprintLine(""));
        }

        @Override
        protected void printSeparator()
        {
            console.reprintLine("");
        }
    }
}
