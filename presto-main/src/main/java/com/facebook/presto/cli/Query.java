package com.facebook.presto.cli;

import com.facebook.presto.execution.FailureInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.server.HttpQueryClient;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.Closeable;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.operator.OutputProcessor.OutputHandler;
import static com.facebook.presto.operator.OutputProcessor.processOutput;
import static com.google.common.base.Preconditions.checkNotNull;
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

    public void renderOutput(PrintStream out)
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
                        queryClient.cancelQuery();
                    }
                }
                catch (RuntimeException e) {
                    log.debug(e, "error canceling leaf stage");
                    queryClient.cancelQuery();
                }
            }
        });
        try {
            renderQueryOutput(out);
        }
        finally {
            Signal.handle(SIGINT, oldHandler);
        }
    }

    private void renderQueryOutput(PrintStream out)
    {
        StatusPrinter statusPrinter = new StatusPrinter(queryClient, out);
        statusPrinter.printInitialStatusUpdates();

        QueryInfo queryInfo = queryClient.getQueryInfo(false);
        if (queryInfo == null) {
            if (queryClient.isCanceled()) {
                out.println("Query aborted by user");
            }
            else {
                out.println("Query is gone (server restarted?)");
            }
            return;
        }

        if (queryInfo.getState().isDone()) {
            switch (queryInfo.getState()) {
                case CANCELED:
                    out.printf("Query %s was canceled\n", queryInfo.getQueryId());
                    return;
                case FAILED:
                    renderFailure(queryInfo, out);
                    return;
            }
        }

        Operator operator = queryClient.getResultsOperator();
        List<String> fieldNames = queryInfo.getFieldNames();

        pageOutput(Pager.LESS, operator, fieldNames);

        // print final info after the user exits from the pager
        statusPrinter.printFinalInfo();
    }

    private void pageOutput(List<String> pagerCommand, Operator operator, List<String> fieldNames)
    {
        // ignore the user pressing ctrl-C while in the pager
        ignoreUserInterrupt.set(true);

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

        // start pager as subprocess and write output to it
        try (Pager pager = Pager.create(pagerCommand)) {
            @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
            OutputStreamWriter writer = new OutputStreamWriter(pager, Charsets.UTF_8);
            OutputHandler outputHandler = new AlignedTuplePrinter(fieldNames, writer);
            processOutput(operator, outputHandler);
        }
        finally {
            executor.shutdown();
        }
    }

    @Override
    public void close()
    {
        queryClient.cancelQuery();
    }

    public void renderFailure(QueryInfo queryInfo, PrintStream out)
    {
        if (queryClient.isDebug()) {
            out.printf("Query %s failed:\n", queryInfo.getQueryId());
            renderStacks(queryInfo, out);
            return;
        }

        Set<String> failureMessages = ImmutableSet.copyOf(getFailureMessages(queryInfo));
        if (failureMessages.isEmpty()) {
            out.printf("Query %s failed for an unknown reason\n", queryInfo.getQueryId());
        }
        else if (failureMessages.size() == 1) {
            out.printf("Query %s failed: %s\n", queryInfo.getQueryId(), Iterables.getOnlyElement(failureMessages));
        }
        else {
            out.printf("Query %s failed:\n", queryInfo.getQueryId());
            for (String failureMessage : failureMessages) {
                out.println("    " + failureMessage);
            }
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
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (FailureInfo failureInfo : queryInfo.getFailures()) {
            builder.add(failureInfo.getMessage());
        }
        if (queryInfo.getOutputStage() != null) {
            builder.addAll(getFailureMessages(queryInfo.getOutputStage()));
        }
        return builder.build();
    }

    public static List<String> getFailureMessages(StageInfo stageInfo)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (FailureInfo failureInfo : stageInfo.getFailures()) {
            builder.add(failureInfo.getMessage());
        }
        for (TaskInfo taskInfo : stageInfo.getTasks()) {
            builder.addAll(getFailureMessages(taskInfo));
        }
        for (StageInfo subStageInfo : stageInfo.getSubStages()) {
            builder.addAll(getFailureMessages(subStageInfo));
        }
        return builder.build();
    }

    private static List<String> getFailureMessages(TaskInfo taskInfo)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (FailureInfo failureInfo : taskInfo.getFailures()) {
            builder.add(failureInfo.getMessage());
        }
        return builder.build();
    }
}
