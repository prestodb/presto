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
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryStatusInfo;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.security.SelectedRole;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.fusesource.jansi.Ansi;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.cli.ConsolePrinter.REAL_TERMINAL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class Query
        implements Closeable
{
    private static final Signal SIGINT = new Signal("INT");

    private final AtomicBoolean ignoreUserInterrupt = new AtomicBoolean();
    private final StatementClient client;
    private final boolean debug;

    public Query(StatementClient client, boolean debug)
    {
        this.client = requireNonNull(client, "client is null");
        this.debug = debug;
    }

    public Optional<String> getSetCatalog()
    {
        return client.getSetCatalog();
    }

    public Optional<String> getSetSchema()
    {
        return client.getSetSchema();
    }

    public Map<String, String> getSetSessionProperties()
    {
        return client.getSetSessionProperties();
    }

    public Set<String> getResetSessionProperties()
    {
        return client.getResetSessionProperties();
    }

    public Map<String, SelectedRole> getSetRoles()
    {
        return client.getSetRoles();
    }

    public Map<String, String> getAddedPreparedStatements()
    {
        return client.getAddedPreparedStatements();
    }

    public Set<String> getDeallocatedPreparedStatements()
    {
        return client.getDeallocatedPreparedStatements();
    }

    public String getStartedTransactionId()
    {
        return client.getStartedTransactionId();
    }

    public boolean isClearTransactionId()
    {
        return client.isClearTransactionId();
    }

    public Map<String, String> getAddedSessionFunctions()
    {
        return client.getAddedSessionFunctions();
    }

    public Set<String> getRemovedSessionFunctions()
    {
        return client.getRemovedSessionFunctions();
    }

    public boolean renderOutput(PrintStream out, OutputFormat outputFormat, boolean interactive)
    {
        Thread clientThread = Thread.currentThread();
        SignalHandler oldHandler = Signal.handle(SIGINT, signal -> {
            if (ignoreUserInterrupt.get() || client.isClientAborted()) {
                return;
            }
            client.close();
            clientThread.interrupt();
        });
        try {
            return renderQueryOutput(out, outputFormat, interactive);
        }
        finally {
            Signal.handle(SIGINT, oldHandler);
            Thread.interrupted(); // clear interrupt status
        }
    }

    private boolean renderQueryOutput(PrintStream out, OutputFormat outputFormat, boolean interactive)
    {
        StatusPrinter statusPrinter = null;
        @SuppressWarnings("resource")
        PrintStream errorChannel = interactive ? out : System.err;
        WarningsPrinter warningsPrinter = new PrintStreamWarningsPrinter(System.err);

        if (interactive) {
            statusPrinter = new StatusPrinter(client, out, debug);
            statusPrinter.printInitialStatusUpdates();
        }
        else {
            processInitialStatusUpdates(warningsPrinter);
        }

        // if running or finished
        if (client.isRunning() || (client.isFinished() && client.finalStatusInfo().getError() == null)) {
            QueryStatusInfo results = client.isRunning() ? client.currentStatusInfo() : client.finalStatusInfo();
            if (results.getUpdateType() != null) {
                renderUpdate(errorChannel, results);
            }
            else if (results.getColumns() == null) {
                errorChannel.printf("Query %s has no columns\n", results.getId());
                return false;
            }
            else {
                renderResults(out, outputFormat, interactive, results.getColumns());
            }
        }

        checkState(!client.isRunning());

        if (statusPrinter != null) {
            // Print all warnings at the end of the query
            new PrintStreamWarningsPrinter(System.err).print(client.finalStatusInfo().getWarnings(), true, true);
            statusPrinter.printFinalInfo();
        }
        else {
            // Print remaining warnings separated
            warningsPrinter.print(client.finalStatusInfo().getWarnings(), true, true);
        }

        if (client.isClientAborted()) {
            errorChannel.println("Query aborted by user");
            return false;
        }
        if (client.isClientError()) {
            errorChannel.println("Query is gone (server restarted?)");
            return false;
        }

        verify(client.isFinished());
        if (client.finalStatusInfo().getError() != null) {
            renderFailure(errorChannel);
            return false;
        }

        return true;
    }

    private void processInitialStatusUpdates(WarningsPrinter warningsPrinter)
    {
        while (client.isRunning() && (client.currentData().getData() == null)) {
            warningsPrinter.print(client.currentStatusInfo().getWarnings(), true, false);
            client.advance();
        }
        List<PrestoWarning> warnings;
        if (client.isRunning()) {
            warnings = client.currentStatusInfo().getWarnings();
        }
        else {
            warnings = client.finalStatusInfo().getWarnings();
        }
        warningsPrinter.print(warnings, false, true);
    }

    private void renderUpdate(PrintStream out, QueryStatusInfo results)
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
            throw new UncheckedIOException(e);
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
            throw new UncheckedIOException(e);
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
        try (Pager pager = Pager.create();
                ThreadInterruptor clientThread = new ThreadInterruptor();
                Writer writer = createWriter(pager);
                OutputHandler handler = createOutputHandler(format, writer, fieldNames)) {
            if (!pager.isNullPager()) {
                // ignore the user pressing ctrl-C while in the pager
                ignoreUserInterrupt.set(true);
                pager.getFinishFuture().thenRun(() -> {
                    ignoreUserInterrupt.set(false);
                    client.close();
                    clientThread.interrupt();
                });
            }
            handler.processRows(client);
        }
        catch (RuntimeException | IOException e) {
            if (client.isClientAborted() && !(e instanceof QueryAbortedException)) {
                throw new QueryAbortedException(e);
            }
            throw e;
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

    public void renderFailure(PrintStream out)
    {
        QueryStatusInfo results = client.finalStatusInfo();
        QueryError error = results.getError();
        checkState(error != null);

        out.printf("Query %s failed: %s%n", results.getId(), error.getMessage());
        if (debug && (error.getFailureInfo() != null)) {
            error.getFailureInfo().toException().printStackTrace(out);
        }
        if (error.getErrorLocation() != null) {
            renderErrorLocation(client.getQuery(), error.getErrorLocation(), out);
        }
        out.println();
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

    private static class PrintStreamWarningsPrinter
            extends AbstractWarningsPrinter
    {
        private final PrintStream printStream;

        PrintStreamWarningsPrinter(PrintStream printStream)
        {
            super(OptionalInt.empty());
            this.printStream = requireNonNull(printStream, "printStream is null");
        }

        @Override
        protected void print(List<String> warnings)
        {
            warnings.stream()
                    .forEach(printStream::println);
        }

        @Override
        protected void printSeparator()
        {
            printStream.println();
        }
    }
}
