package com.facebook.presto.cli;

import com.facebook.presto.cli.ClientOptions.OutputFormat;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.client.ErrorLocation;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.cli.ClientOptions.OutputFormat.CSV_HEADER;
import static com.facebook.presto.cli.ClientOptions.OutputFormat.PAGED;
import static com.facebook.presto.cli.ClientOptions.OutputFormat.TSV_HEADER;
import static com.facebook.presto.cli.ConsolePrinter.REAL_TERMINAL;
import static com.facebook.presto.cli.OutputHandler.processOutput;
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

    public void renderOutput(PrintStream out, OutputFormat outputFormat)
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
            renderQueryOutput(out, outputFormat);
        }
        finally {
            Signal.handle(SIGINT, oldHandler);
        }
    }

    private void renderQueryOutput(PrintStream out, OutputFormat outputFormat)
    {
        StatusPrinter statusPrinter = null;
        @SuppressWarnings("resource")
        PrintStream errorChannel = (outputFormat == PAGED) ? out : System.err;

        if (outputFormat == PAGED) {
            statusPrinter = new StatusPrinter(client, out);
            statusPrinter.printInitialStatusUpdates();
        }

        if (client.isClosed()) {
            errorChannel.println("Query aborted by user");
            return;
        }
        if (client.isGone()) {
            errorChannel.println("Query is gone (server restarted?)");
            return;
        }

        QueryResults results = client.current();

        if (results.getStats().isDone()) {
            switch (results.getStats().getState()) {
                case "CANCELED":
                    errorChannel.printf("Query %s was canceled\n", results.getQueryId());
                    return;
                case "FAILED":
                    renderFailure(results, errorChannel);
                    return;
            }
        }

        List<String> fieldNames = Lists.transform(results.getColumns(), Column.nameGetter());
        switch (outputFormat) {
            case PAGED:
                pageOutput(Pager.LESS, fieldNames);
                break;
            default:
                sendOutput(out, outputFormat, fieldNames);
                break;
        }

        if (statusPrinter != null) {
            // print final info after the user exits from the pager
            statusPrinter.printFinalInfo();
        }
    }

    private void pageOutput(List<String> pagerCommand, List<String> fieldNames)
    {
        // ignore the user pressing ctrl-C while in the pager
        ignoreUserInterrupt.set(true);

        try (Pager pager = Pager.create(pagerCommand);
                OutputHandler handler = new AlignedTuplePrinter(fieldNames, createWriter(pager))) {
            processOutput(client, handler);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void sendOutput(PrintStream out, OutputFormat outputFormat, List<String> fieldNames)
    {
        try (OutputHandler handler = createOutputHandler(outputFormat, createWriter(out))) {
            if ((outputFormat == CSV_HEADER) || (outputFormat == TSV_HEADER)) {
                // add a header line with the field names
                handler.processRow(fieldNames);
            }
            processOutput(client, handler);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static OutputHandler createOutputHandler(OutputFormat outputFormat, OutputStreamWriter writer)
    {
        switch (outputFormat) {
            case CSV:
            case CSV_HEADER:
                return new CSVPrinter(writer, ',');
            case TSV:
            case TSV_HEADER:
                return new CSVPrinter(writer, '\t');
        }
        throw new RuntimeException(outputFormat + " not supported");
    }

    private static OutputStreamWriter createWriter(OutputStream out)
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
        if (results.getError() == null) {
            out.printf("Query %s failed for an unknown reason%n", results.getQueryId());
        }
        else {
            out.printf("Query %s failed: %s%n", results.getQueryId(), results.getError().getMessage());
            if (client.isDebug()) {
                renderStack(results, out);
            }
            renderErrorLocation(client.getQuery(), results, out);
        }
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
