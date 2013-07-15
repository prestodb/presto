package com.facebook.presto.cli;

import com.facebook.presto.cli.ClientOptions.OutputFormat;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.ErrorLocation;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
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

    public Query(StatementClient client)
    {
        this.client = checkNotNull(client, "client is null");
    }

    public void renderOutput(PrintStream out, OutputFormat outputFormat, boolean interactive)
    {
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
            renderQueryOutput(out, outputFormat, interactive);
        }
        finally {
            Signal.handle(SIGINT, oldHandler);
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
            if (results.getColumns() == null) {
                errorChannel.printf("Query %s has no columns\n", results.getId());
                return;
            }

            try {
                renderResults(out, outputFormat, interactive, results);
            }
            catch (QueryAbortedException e) {
                System.out.println("(query aborted by user)");
                client.close();
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

    private void renderResults(PrintStream out, OutputFormat format, boolean interactive, QueryResults results)
    {
        List<String> fieldNames = Lists.transform(results.getColumns(), Column.nameGetter());
        if (interactive) {
            pageOutput(Pager.LESS, format, fieldNames);
        }
        else {
            sendOutput(out, format, fieldNames);
        }
    }

    private void pageOutput(List<String> pagerCommand, OutputFormat format, List<String> fieldNames)
    {
        // ignore the user pressing ctrl-C while in the pager
        ignoreUserInterrupt.set(true);

        try (Writer writer = createWriter(Pager.create(pagerCommand));
                OutputHandler handler = createOutputHandler(format, writer, fieldNames)) {
            handler.processRows(client);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void sendOutput(PrintStream out, OutputFormat format, List<String> fieldNames)
    {
        try (OutputHandler handler = createOutputHandler(format, createWriter(out), fieldNames)) {
            handler.processRows(client);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
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
                return new CsvPrinter(fieldNames, writer, ',', false);
            case CSV_HEADER:
                return new CsvPrinter(fieldNames, writer, ',', true);
            case TSV:
                return new CsvPrinter(fieldNames, writer, '\t', false);
            case TSV_HEADER:
                return new CsvPrinter(fieldNames, writer, '\t', true);
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
}
