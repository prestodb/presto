package com.facebook.presto.cli;

import com.facebook.presto.execution.FailureInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OutputProcessor;
import com.facebook.presto.server.HttpQueryClient;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.Closeable;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.operator.OutputProcessor.OutputHandler;
import static com.facebook.presto.operator.OutputProcessor.OutputStats;
import static com.google.common.base.Preconditions.checkNotNull;

public class Query
        implements Closeable
{
    private static final Signal SIGINT = new Signal("INT");

    private final HttpQueryClient queryClient;
    private final boolean debug;

    public Query(HttpQueryClient queryClient, boolean debug)
    {
        this.queryClient = checkNotNull(queryClient, "queryClient is null");
        this.debug = debug;
    }

    public void renderOutput(PrintStream out)
    {
        SignalHandler oldHandler = Signal.handle(SIGINT, new SignalHandler()
        {
            @Override
            public void handle(Signal signal)
            {
                close();
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
            out.println("Query is gone (server restarted?)");
        }
        else if (queryInfo.getState().isDone()) {
            if (queryInfo.getState() == QueryState.CANCELED) {
                out.printf("Query %s was canceled\n", queryInfo.getQueryId());
            }
            else if (queryInfo.getState() == QueryState.FAILED) {
                renderFailure(queryInfo, out);
            }
            else {
                out.printf("Query %s finished with no output\n", queryInfo.getQueryId());
            }
        }
        else {
            Operator operator = queryClient.getResultsOperator();
            List<String> fieldNames = queryInfo.getFieldNames();

            OutputStats stats = pageOutput(Pager.LESS, operator, fieldNames);

            // print final info after the user exits from the pager
            statusPrinter.printFinalInfo(stats);
        }
    }

    private static OutputStats pageOutput(List<String> pagerCommand, Operator operator, List<String> fieldNames)
    {
        try (Pager pager = Pager.create(pagerCommand)) {
            @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
            OutputStreamWriter writer = new OutputStreamWriter(pager, Charsets.UTF_8);
            OutputHandler outputHandler = new AlignedTuplePrinter(fieldNames, writer);
            OutputProcessor processor = new OutputProcessor(operator, outputHandler);
            return processor.process();
        }
    }

    @Override
    public void close()
    {
        queryClient.destroy();
    }

    public void renderFailure(QueryInfo queryInfo, PrintStream out)
    {
        if (debug) {
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
