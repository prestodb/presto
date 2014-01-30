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

import com.facebook.presto.cli.ClientOptions.OutputFormat;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.ErrorLocation;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.client.StatementStats;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import static com.google.common.io.ByteStreams.nullOutputStream;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.fusesource.jansi.Ansi;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import static java.util.concurrent.TimeUnit.SECONDS;

import static java.lang.Math.min;

import static com.facebook.presto.cli.ConsolePrinter.REAL_TERMINAL;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class Query
        implements Closeable
{
    private static final Logger log = Logger.get(Query.class);

    private static final Signal SIGINT = new Signal("INT");

    private final AtomicBoolean ignoreUserInterrupt = new AtomicBoolean();
    private final StatementClient client;

    public enum InteractionLevel {
        NONE,
        PROGRESS,
        ALL
    };

    public Query(StatementClient client)
    {
        this.client = checkNotNull(client, "client is null");
    }

    public int renderOutput(PrintStream out, OutputFormat outputFormat, InteractionLevel il)
    {
        int ret = 0;
        SignalHandler oldHandler = Signal.handle(SIGINT, new SignalHandler()
        {
            @Override
            public void handle(Signal signal)
            {
                if (ignoreUserInterrupt.get() || client.isClosed()) {
                    return;
                }
                try {
                    if (!client.cancelLeafStage()) {
                        client.close();
                    }
                }
                catch (RuntimeException e) {
                    log.debug(e, "error canceling leaf stage");
                    client.close();
                }
            }
        });
        try {
            ret = renderQueryOutput(out, outputFormat, il);
        }
        finally {
            Signal.handle(SIGINT, oldHandler);
        }

        return ret;
    }

    private int renderQueryOutput(PrintStream out, OutputFormat outputFormat, InteractionLevel il)
    {
        StatusPrinter statusPrinter = null;
        @SuppressWarnings("resource")
        PrintStream errorChannel = (il == InteractionLevel.ALL) ? out : System.err;

        if (il == InteractionLevel.ALL) {
            statusPrinter = new StatusPrinter(client, out);
            statusPrinter.printInitialStatusUpdates();
        }
        else {
            waitForData(il);
        }

        if ((!client.isFailed()) && (!client.isGone()) && (!client.isClosed())) {
            QueryResults results = client.isValid() ? client.current() : client.finalResults();
            if (results.getColumns() == null) {
                errorChannel.printf("Query %s has no columns\n", results.getId());
                return -1;
            }

            try {
                renderResults(out, outputFormat, (il == InteractionLevel.ALL), results);
            }
            catch (QueryAbortedException e) {
                System.out.println("(query aborted by user)");
                client.close();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        if (statusPrinter != null) {
            statusPrinter.printFinalInfo();
        }

        if (client.isClosed()) {
            errorChannel.println("Query aborted by user");
            return -1;
        }
        else if (client.isGone()) {
            errorChannel.println("Query is gone (server restarted?)");
            return -1;
        }
        else if (client.isFailed()) {
            renderFailure(client.finalResults(), errorChannel);
            return -1;
        }

        return 0;
    }

    private void waitForData(InteractionLevel il)
    {
        long lastPrint = 0;
        int progressPercentage;

        PrintStream err = System.err;
        switch(il) {
        case PROGRESS:
            // retain the stderr stream
            break;
        case NONE:
            // set stderr as null stream
            System.setErr(new PrintStream(nullOutputStream()));
            break;
        default:
            assert false;
            break;
        }

        System.err.println("Started Query: " + client.current().getId());
        while (client.isValid() && (client.current().getData() == null)) {
            QueryResults results = client.current();
            StatementStats stats = results.getStats();
            // Copy of what printQueryInfo() does.
            if (Duration.nanosSince(lastPrint).getValue(SECONDS) >= 0.5) {
                progressPercentage = (int) min(99, percentage(stats.getCompletedSplits(), stats.getTotalSplits()));
                System.err.println(String.format("Query: %s Progress: %d%%", results.getId(), progressPercentage));
                lastPrint = System.nanoTime();
            }
            client.advance();
        }

        if (client.isValid()) {
            System.err.println(String.format("Query: %s Progress: 100%%", client.current().getId()));
        }

        System.setErr(err);
    }

    private void renderResults(PrintStream out, OutputFormat format, boolean interactive, QueryResults results)
            throws IOException
    {
        List<String> fieldNames = Lists.transform(results.getColumns(), Column.nameGetter());
        if (interactive) {
            pageOutput(format, fieldNames);
        }
        else {
            sendOutput(out, format, fieldNames);
        }
    }

    private void pageOutput(OutputFormat format, List<String> fieldNames)
            throws IOException
    {
        // ignore the user pressing ctrl-C while in the pager
        ignoreUserInterrupt.set(true);

        try (Writer writer = createWriter(Pager.create());
                OutputHandler handler = createOutputHandler(format, writer, fieldNames)) {
            handler.processRows(client);
        }
    }

    private void sendOutput(PrintStream out, OutputFormat format, List<String> fieldNames)
            throws IOException
    {
        try (OutputHandler handler = createOutputHandler(format, createWriter(out), fieldNames)) {
            handler.processRows(client);
        }
    }

    private static OutputHandler createOutputHandler(OutputFormat format, Writer writer, List<String> fieldNames)
    {
        return new OutputHandler(createOutputPrinter(format, writer, fieldNames));
    }

    private static OutputPrinter createOutputPrinter(OutputFormat format, Writer writer, List<String> fieldNames)
    {
        switch (format) {
            case ALIGNED:
                return new AlignedTuplePrinter(fieldNames, writer);
            case VERTICAL:
                return new VerticalTuplePrinter(fieldNames, writer);
            case CSV:
                return new CsvPrinter(fieldNames, writer, false);
            case CSV_HEADER:
                return new CsvPrinter(fieldNames, writer, true);
            case TSV:
                return new TsvPrinter(fieldNames, writer, false);
            case TSV_HEADER:
                return new TsvPrinter(fieldNames, writer, true);
        }
        throw new RuntimeException(format + " not supported");
    }

    private static Writer createWriter(OutputStream out)
    {
        return new OutputStreamWriter(out, Charsets.UTF_8);
    }

    @Override
    public void close()
    {
        client.close();
    }

    public void renderFailure(QueryResults results, PrintStream out)
    {
        out.printf("Query %s failed: %s%n", results.getId(), results.getError().getMessage());
        if (client.isDebug()) {
            renderStack(results, out);
        }
        renderErrorLocation(client.getQuery(), results, out);
    }

    private static void renderErrorLocation(String query, QueryResults results, PrintStream out)
    {
        if (results.getError().getErrorLocation() != null) {
            renderErrorLocation(query, results.getError().getErrorLocation(), out);
        }
    }

    private static void renderErrorLocation(String query, ErrorLocation location, PrintStream out)
    {
        List<String> lines = ImmutableList.copyOf(Splitter.on('\n').split(query).iterator());

        String errorLine = lines.get(location.getLineNumber() - 1);
        String good = errorLine.substring(0, location.getColumnNumber() - 1);
        String bad = errorLine.substring(location.getColumnNumber() - 1);

        if ((location.getLineNumber() == lines.size()) && bad.trim().isEmpty()) {
            bad = " <EOF>";
        }

        if (REAL_TERMINAL) {
            Ansi ansi = Ansi.ansi();

            ansi.fg(Ansi.Color.CYAN);
            for (int i = 1; i < location.getLineNumber(); i++) {
                ansi.a(lines.get(i - 1)).newline();
            }
            ansi.a(good);

            ansi.fg(Ansi.Color.RED);
            ansi.a(bad).newline();
            for (int i = location.getLineNumber(); i < lines.size(); i++) {
                ansi.a(lines.get(i)).newline();
            }

            ansi.reset();
            out.println(ansi);
        }
        else {
            String prefix = format("LINE %s: ", location.getLineNumber());
            String padding = Strings.repeat(" ", prefix.length() + (location.getColumnNumber() - 1));
            out.println(prefix + errorLine);
            out.println(padding + "^");
        }
    }

    private static void renderStack(QueryResults results, PrintStream out)
    {
        if (results.getError().getFailureInfo() != null) {
            results.getError().getFailureInfo().toException().printStackTrace(out);
        }
    }

    private static double percentage(double count, double total)
    {
        if (total == 0) {
            return 0;
        }
        return min(100, (count * 100.0) / total);
    }
}
