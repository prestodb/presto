package com.facebook.presto.server;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.sql.planner.Partition;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;

public class TaskScheduler
{
    private static final String ROOT_OUTPUT_BUFFER_NAME = "out";
    private final ExecutorService executor;
    private final HttpClient httpClient;
    private final JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec;
    private final JsonCodec<TaskInfo> taskInfoCodec;

    @Inject
    public TaskScheduler(@ForScheduler HttpClient httpClient, JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec, JsonCodec<TaskInfo> taskInfoCodec)
    {
        this.executor = Executors.newCachedThreadPool(); // todo remove this... pool is never really used
        this.queryFragmentRequestCodec = checkNotNull(queryFragmentRequestCodec, "queryFragmentRequestCodec is null");
        this.taskInfoCodec = checkNotNull(taskInfoCodec, "taskInfoCodec is null");
        this.httpClient = checkNotNull(httpClient, "httpClient is null");

    }

    public void schedule(StageExecutionPlan stageExecutionPlan, ConcurrentMap<String, List<HttpTaskClient>> stageTasks)
    {
        scheduleFragment(stageExecutionPlan, ImmutableList.of(ROOT_OUTPUT_BUFFER_NAME), stageTasks);
    }

    private void scheduleFragment(final StageExecutionPlan stageExecutionPlan,
            final List<String> outputIds,
            final ConcurrentMap<String, List<HttpTaskClient>> stages)
    {
        // schedule the child fragments with an output for each partition
        for (StageExecutionPlan dependency : stageExecutionPlan.getDependencies()) {
            scheduleFragment(dependency, toOutputIds(stageExecutionPlan.getPartitions()), stages);
        }

        // create a task for each partition
        List<HttpTaskClient> taskClients = ImmutableList.copyOf(transform(stageExecutionPlan.getPartitions(), new Function<Partition, HttpTaskClient>()
        {
            @Override
            public HttpTaskClient apply(Partition partition)
            {
                // get fragment sources
                Map<String, ExchangePlanFragmentSource> exchangeSources = getExchangeSources(partition.getNode(), stageExecutionPlan, stages);

                Node node = partition.getNode();
                QueryFragmentRequest queryFragmentRequest = new QueryFragmentRequest(stageExecutionPlan.getFragment(), partition.getSplits(), exchangeSources, outputIds);
                Request request = preparePost()
                        .setUri(uriBuilderFrom(node.getHttpUri()).replacePath("/v1/presto/task").build())
                        .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                        .setBodyGenerator(jsonBodyGenerator(queryFragmentRequestCodec, queryFragmentRequest))
                        .build();

                JsonResponse<TaskInfo> response = httpClient.execute(request, createFullJsonResponseHandler(jsonCodec(TaskInfo.class)));
                checkState(response.getStatusCode() == 201,
                        "Expected response code from %s to be 201, but was %d: %s",
                        request.getUri(),
                        response.getStatusCode(),
                        response.getStatusMessage());
                String location = response.getHeader("Location");
                checkState(location != null);

                TaskInfo taskInfo = response.getValue();

                // todo we don't need a QueryDriverProvider
                return new HttpTaskClient(taskInfo.getTaskId(),
                        URI.create(location),
                        "unused",
                        taskInfo.getTupleInfos(),
                        httpClient,
                        executor,
                        taskInfoCodec);
            }
        }));

        // record the stage
        stages.put(stageExecutionPlan.getStageId(), taskClients);

        // todo if this is a blocking task
        // waitForRunning(taskClients);
    }

    private Map<String, ExchangePlanFragmentSource> getExchangeSources(Node node, StageExecutionPlan stageExecutionPlan, ConcurrentMap<String, List<HttpTaskClient>> stages)
    {
        ImmutableMap.Builder<String, ExchangePlanFragmentSource> exchangeSources = ImmutableMap.builder();
        for (StageExecutionPlan dependency : stageExecutionPlan.getDependencies()) {
            // get locations for the dependent stage
            List<HttpTaskClient> tasks = stages.get(dependency.getStageId());
            ImmutableMap.Builder<String, URI> sources = ImmutableMap.builder();
            for (HttpTaskClient provider : tasks) {
                sources.put(provider.getTaskId(), provider.getLocation());
            }

            ExchangePlanFragmentSource exchangeSource = new ExchangePlanFragmentSource(sources.build(),
                    node.getNodeIdentifier(),
                    tasks.get(0).getTupleInfos());
            exchangeSources.put(dependency.getStageId(), exchangeSource);
        }
        return exchangeSources.build();
    }

    private static void waitForRunning(List<HttpTaskClient> taskClients)
    {
        while (true) {
            long start = System.nanoTime();

            // remove tasks that have started running
            taskClients = ImmutableList.copyOf(filter(taskClients, new Predicate<HttpTaskClient>()
            {
                @Override
                public boolean apply(HttpTaskClient taskClient)
                {
                    TaskInfo taskInfo = taskClient.getTaskInfo();
                    if (taskInfo == null) {
                        return false;
                    }
                    TaskState state = taskInfo.getState();
                    return state == TaskState.PLANNED || state == TaskState.QUEUED;
                }
            }));

            // if no tasks are left, the stage has started
            if (taskClients.isEmpty()) {
                return;
            }

            // sleep for 100ms (minus the time we spent fetching the task states)
            Duration duration = Duration.nanosSince(start);
            long waitTime = (long) (100 - duration.toMillis());
            if (waitTime > 0) {
                try {
                    Thread.sleep(waitTime);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    private ImmutableList<String> toOutputIds(List<Partition> partitions)
    {
        return ImmutableList.copyOf(transform(partitions, new Function<Partition, String>()
        {
            @Override
            public String apply(Partition partition)
            {
                return partition.getNode().getNodeIdentifier();
            }
        }));
    }
}
