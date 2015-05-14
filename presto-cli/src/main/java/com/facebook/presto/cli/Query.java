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
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.cli.ConsolePrinter.REAL_TERMINAL;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Query
        implements Closeable
{
    private static final Logger log = Logger.get(Query.class);

    private static final Signal SIGINT = new Signal("INT");

    private final AtomicBoolean ignoreUserInterrupt = new AtomicBoolean();
    private final StatementClient client;

    public Query(StatementClient client)
    {
        this.client = checkNotNull(client, "client is null");
    }

    public Map<String, String> getSetSessionProperties()
    {
        return client.getSetSessionProperties();
    }

    public Set<String> getResetSessionProperties()
    {
        return client.getResetSessionProperties();
    }

    public void renderOutput(PrintStream out, OutputFormat outputFormat, boolean interactive)
    {
        Thread clientThread = Thread.currentThread();
        SignalHandler oldHandler = Signal.handle(SIGINT, new SignalHandler()
        {
            @Override
            public void handle(Signal signal)
            {
                if (ignoreUserInterrupt.get() || client.isClosed()) {
                    return;
                }
                try {
                    if (client.cancelLeafStage(new Duration(1, SECONDS))) {
                        return;
                    }
                }
                catch (RuntimeException e) {
                    log.debug(e, "error canceling leaf stage");
                }
                client.close();
                clientThread.interrupt();
            }
        });
        try {
            renderQueryOutput(out, outputFormat, interactive);
        }
        finally {
            Signal.handle(SIGINT, oldHandler);
            Thread.interrupted(); // clear interrupt status
        }
    }

    private void renderQueryOutput(PrintStream out, OutputFormat outputFormat, boolean interactive)
    {
        StatusPrinter statusPrinter = null;
        @SuppressWarnings("resource")
        PrintStream errorChannel = interactive ? out : System.err;

        if (interactive) {
            statusPrinter = new StatusPrinter(client, out);
            statusPrinter.printInitialStatusUpdates();
        }
        else {
            waitForData();
        }

        if ((!client.isFailed()) && (!client.isGone()) && (!client.isClosed())) {
            QueryResults results = client.isValid() ? client.current() : client.finalResults();
            if (results.getUpdateType() != null) {
                renderUpdate(out, results);
            }
            else if (results.getColumns() == null) {
                errorChannel.printf("Query %s has no columns\n", results.getId());
                return;
            }
            else {
                renderResults(out, outputFormat, interactive, results.getColumns());
            }
        }

        if (statusPrinter != null) {
            statusPrinter.printFinalInfo();
        }

        if (client.isClosed()) {
            errorChannel.println("Query aborted by user");
        }
        else if (client.isGone()) {
            errorChannel.println("Query is gone (server restarted?)");
        }
        else if (client.isFailed()) {
            renderFailure(client.finalResults(), errorChannel);
        }
    }

    private void waitForData()
    {
        while (client.isValid() && (client.current().getData() == null)) {
            client.advance();
        }
    }

    private void renderUpdate(PrintStream out, QueryResults results)
    {
        String status = results.getUpdateType();
        if (results.getUpdateCount() != null) {
            long count = results.getUpdateCount();
            status += format(": %s row%s", count, (count != 1) ? "s" : "");
        }
        out.println(status);
        discardResults();
    }

    private void discardResults()
    {
        try (OutputHandler handler = new OutputHandler(new NullPrinter())) {
            handler.processRows(client);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void renderResults(PrintStream out, OutputFormat outputFormat, boolean interactive, List<Column> columns)
    {
        try {
            doRenderResults(out, outputFormat, interactive, columns);
        }
        catch (QueryAbortedException e) {
            System.out.println("(query aborted by user)");
            client.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void doRenderResults(PrintStream out, OutputFormat format, boolean interactive, List<Column> columns)
            throws IOException
    {
        List<String> fieldNames = Lists.transform(columns, Column::getName);
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
                return new AlignedTablePrinter(fieldNames, writer);
            case VERTICAL:
                return new VerticalRecordPrinter(fieldNames, writer);
            case CSV:
                return new CsvPrinter(fieldNames, writer, false);
            case CSV_HEADER:
                return new CsvPrinter(fieldNames, writer, true);
            case TSV:
                return new TsvPrinter(fieldNames, writer, false);
            case TSV_HEADER:
                return new TsvPrinter(fieldNames, writer, true);
            case NULL:
                return new NullPrinter();
        }
        throw new RuntimeException(format + " not supported");
    }

    private static Writer createWriter(OutputStream out)
    {
        return new OutputStreamWriter(out, UTF_8);
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
        out.println();
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
            out.print(ansi);
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
}
