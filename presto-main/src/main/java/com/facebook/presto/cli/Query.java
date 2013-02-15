package com.facebook.presto.cli;

import com.facebook.presto.cli.ClientOptions.OutputFormat;
import com.facebook.presto.execution.ErrorLocation;
import com.facebook.presto.execution.FailureInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OutputProcessor.OutputHandler;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.fusesource.jansi.Ansi;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.Closeable;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.cli.ClientOptions.OutputFormat.CSV_HEADER;
import static com.facebook.presto.cli.ClientOptions.OutputFormat.PAGED;
import static com.facebook.presto.cli.ClientOptions.OutputFormat.TSV_HEADER;
import static com.facebook.presto.cli.StatusPrinter.REAL_TERMINAL;
import static com.facebook.presto.operator.OutputProcessor.processOutput;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Query
        implements Closeable
{
    private static final Logger log = Logger.get(Query.class);

    private static final Signal SIGINT = new Signal("INT");
    private static final Duration PING_INTERVAL = new Duration(1, SECONDS);

    private final AtomicBoolean ignoreUserInterrupt = new AtomicBoolean();
    private final HttpQueryClient queryClient;

    public Query(HttpQueryClient queryClient)
    {
        this.queryClient = checkNotNull(queryClient, "queryClient is null");
    }

    public void renderOutput(PrintStream out, OutputFormat outputFormat)
    {
        SignalHandler oldHandler = Signal.handle(SIGINT, new SignalHandler()
        {
            @Override
            public void handle(Signal signal)
            {
                if (ignoreUserInterrupt.get() || queryClient.isCanceled()) {
                    return;
                }
                try {
                    if (!queryClient.cancelLeafStage()) {
                        queryClient.close();
                    }
                }
                catch (RuntimeException e) {
                    log.debug(e, "error canceling leaf stage");
                    queryClient.close();
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
            statusPrinter = new StatusPrinter(queryClient, out);
            statusPrinter.printInitialStatusUpdates();
        }
        else {
            // do the "wait for query ready" loop by hand....
            waitForResults();
        }

        QueryInfo queryInfo = queryClient.getQueryInfo(false);
        if (queryInfo == null) {
            if (queryClient.isCanceled()) {
                errorChannel.println("Query aborted by user");
            }
            else {
                errorChannel.println("Query is gone (server restarted?)");
            }
            return;
        }

        if (queryInfo.getState().isDone()) {
            switch (queryInfo.getState()) {
                case CANCELED:
                    errorChannel.printf("Query %s was canceled\n", queryInfo.getQueryId());
                    return;
                case FAILED:
                    renderFailure(queryInfo, errorChannel);
                    return;
            }
        }

        Operator operator = queryClient.getResultsOperator();
        List<String> fieldNames = queryInfo.getFieldNames();

        switch (outputFormat) {
            case PAGED:
                pageOutput(Pager.LESS, operator, fieldNames);
                break;
            default:
                sendOutput(out, outputFormat, operator, fieldNames);
                break;
        }

        if (statusPrinter != null) {
            // print final info after the user exits from the pager
            statusPrinter.printFinalInfo();
        }
    }

    private void waitForResults()
    {
        while (true) {
            try {
                QueryInfo queryInfo = queryClient.getQueryInfo(false);

                // if query is no longer running, finish
                if ((queryInfo == null) || queryInfo.getState().isDone()) {
                    break;
                }

                // check if there is there is pending output
                if (queryInfo.resultsPending()) {
                    break;
                }

                Uninterruptibles.sleepUninterruptibly(100, MILLISECONDS);
            }
            catch (Exception e) {
                throw propagate(e);
            }
        }
    }

    private void pageOutput(final List<String> pagerCommand, final Operator operator, final List<String> fieldNames)
    {
        // ignore the user pressing ctrl-C while in the pager
        ignoreUserInterrupt.set(true);

        withKeepalive(new Callable<Void>() {

            @Override
            public Void call()
                    throws Exception
            {
                try (Pager pager = Pager.create(pagerCommand)) {
                    @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
                    OutputStreamWriter writer = new OutputStreamWriter(pager, Charsets.UTF_8);
                    OutputHandler outputHandler = new AlignedTuplePrinter(fieldNames, writer);
                    processOutput(operator, outputHandler);
                }
                return null;
            }

        });
    }

    private <T> T withKeepalive(Callable<T> callable)
    {
        // ping the server while reading data to keep the query alive
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                queryClient.getQueryInfo(false);
            }
        }, 0, (long) PING_INTERVAL.toMillis(), MILLISECONDS);

        try {
            return callable.call();
        }
        catch (Exception e) {
            throw propagate(e);
        }
        finally {
            executor.shutdown();
        }
    }

    private void sendOutput(final PrintStream out, final OutputFormat outputFormat, final Operator operator, final List<String> fieldNames)
    {
        withKeepalive(new Callable<Void> () {

            @Override
            public Void call()
                    throws Exception
            {
                try (OutputStreamWriter osw = new OutputStreamWriter(out)) {
                    OutputHandler handler;
                    switch (outputFormat) {
                        case CSV:
                        case CSV_HEADER:
                            handler = new CSVPrinter(osw, ',');
                            break;
                        case TSV:
                        case TSV_HEADER:
                            handler = new CSVPrinter(osw, '\t');
                            break;
                        default:
                            throw new RuntimeException(outputFormat + " not supported.");
                    }

                    if (outputFormat == CSV_HEADER || outputFormat == TSV_HEADER) {
                        // Add a line with the field names.
                        handler.processRow(fieldNames);
                    }

                    processOutput(operator, handler);
                    osw.flush();
                }
                return null;
            }
        });
    }

    @Override
    public void close()
    {
        queryClient.close();
    }

    public void renderFailure(QueryInfo queryInfo, PrintStream out)
    {
        if (queryClient.isDebug()) {
            out.printf("Query %s failed:\n", queryInfo.getQueryId());
            renderStacks(queryInfo, out);
            renderQueryLocationError(queryInfo, out);
            return;
        }

        Set<String> failureMessages = ImmutableSet.copyOf(getFailureMessages(queryInfo));
        if (failureMessages.isEmpty()) {
            out.printf("Query %s failed for an unknown reason\n", queryInfo.getQueryId());
        }
        else if (failureMessages.size() == 1) {
            out.printf("Query %s failed: %s\n", queryInfo.getQueryId(), Iterables.getOnlyElement(failureMessages));
            renderQueryLocationError(queryInfo, out);
        }
        else {
            out.printf("Query %s failed:\n", queryInfo.getQueryId());
            for (String failureMessage : failureMessages) {
                out.println("    " + failureMessage);
            }
        }
    }

    private static void renderQueryLocationError(QueryInfo queryInfo, PrintStream out)
    {
        List<FailureInfo> failureInfos = getFailureInfos(queryInfo);
        if (failureInfos.size() == 1) {
            ErrorLocation location = failureInfos.get(0).getErrorLocation();
            if (location != null) {
                renderQueryLocationError(queryInfo.getQuery(), location, out);
            }
        }
    }

    private static void renderQueryLocationError(String query, ErrorLocation location, PrintStream out)
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

    private static void renderStacks(QueryInfo queryInfo, PrintStream out)
    {
        for (FailureInfo failureInfo : queryInfo.getFailures()) {
            failureInfo.toException().printStackTrace(out);
        }
        if (queryInfo.getOutputStage() != null) {
            renderStacks(queryInfo.getOutputStage(), out);
        }
    }

    private static void renderStacks(StageInfo stageInfo, PrintStream out)
    {
        if (!stageInfo.getFailures().isEmpty()) {
            out.printf("Stage %s failed:\n", stageInfo.getStageId());
            for (FailureInfo failureInfo : stageInfo.getFailures()) {
                failureInfo.toException().printStackTrace(out);
            }
        }
        for (TaskInfo taskInfo : stageInfo.getTasks()) {
            renderStacks(taskInfo, out);
        }
        for (StageInfo subStageInfo : stageInfo.getSubStages()) {
            renderStacks(subStageInfo, out);
        }
    }

    private static void renderStacks(TaskInfo taskInfo, PrintStream out)
    {
        if (!taskInfo.getFailures().isEmpty()) {
            out.printf("Task %s failed:\n", taskInfo.getTaskId());
            for (FailureInfo failureInfo : taskInfo.getFailures()) {
                failureInfo.toException().printStackTrace(out);
            }
        }
    }

    public static List<String> getFailureMessages(QueryInfo queryInfo)
    {
        return Lists.transform(getFailureInfos(queryInfo), FailureInfo.messageGetter());
    }

    public static List<FailureInfo> getFailureInfos(QueryInfo queryInfo)
    {
        ImmutableList.Builder<FailureInfo> builder = ImmutableList.builder();
        builder.addAll(queryInfo.getFailures());
        if (queryInfo.getOutputStage() != null) {
            builder.addAll(getFailureInfos(queryInfo.getOutputStage()));
        }
        return builder.build();
    }

    public static List<FailureInfo> getFailureInfos(StageInfo stageInfo)
    {
        ImmutableList.Builder<FailureInfo> builder = ImmutableList.builder();
        for (FailureInfo failureInfo : stageInfo.getFailures()) {
            builder.add(failureInfo);
        }
        for (TaskInfo taskInfo : stageInfo.getTasks()) {
            builder.addAll(taskInfo.getFailures());
        }
        for (StageInfo subStageInfo : stageInfo.getSubStages()) {
            builder.addAll(getFailureInfos(subStageInfo));
        }
        return builder.build();
    }
}
