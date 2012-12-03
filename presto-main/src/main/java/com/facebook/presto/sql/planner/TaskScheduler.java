package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.server.ExchangePlanFragmentSource;
import com.facebook.presto.server.HttpTaskClient;
import com.facebook.presto.server.QueryFragmentRequest;
import com.facebook.presto.server.QueryState.State;
import com.facebook.presto.server.QueryTaskInfo;
import com.facebook.presto.server.TableScanPlanFragmentSource;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.SplitAssignments;
import com.facebook.presto.split.SplitManager;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;

public class TaskScheduler
{
    private final NodeManager nodeManager;
    private final SplitManager splitManager;
    private final ExecutorService executor;
    private final ApacheHttpClient httpClient;
    private final JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec;
    private final JsonCodec<QueryTaskInfo> queryTaskInfoCodec;

    private final Random random = new Random();

    @Inject
    public TaskScheduler(NodeManager nodeManager,
            SplitManager splitManager,
            JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec,
            JsonCodec<QueryTaskInfo> queryTaskInfoCodec)
    {
        this.nodeManager = nodeManager;
        this.splitManager = splitManager;
        this.executor = Executors.newCachedThreadPool(); // todo remove this... pool is never really used
        this.queryFragmentRequestCodec = queryFragmentRequestCodec;
        this.queryTaskInfoCodec = queryTaskInfoCodec;

        httpClient = new ApacheHttpClient(new HttpClientConfig()
                .setConnectTimeout(new Duration(5, TimeUnit.MINUTES))
                .setReadTimeout(new Duration(5, TimeUnit.MINUTES)));
    }

    public Integer schedule(List<PlanFragment> fragments, ConcurrentMap<Integer, List<HttpTaskClient>> stages)
    {
        // todo find root
        PlanFragment rootPlanFragment = fragments.get(fragments.size() - 1);
        scheduleFragment(rootPlanFragment, ImmutableList.of("out"), fragments, stages);
        return rootPlanFragment.getId();
    }

    private void scheduleFragment(final PlanFragment currentFragment,
            final List<String> outputIds,
            List<PlanFragment> allFragments,
            final ConcurrentMap<Integer, List<HttpTaskClient>> stages)
    {
        // get partitions for this fragment
        List<Partition> partitions;
        if (currentFragment.isPartitioned()) {
            // partitioned plan is based on an underlying table scan or distributed aggregation
            partitions = getPartitions(currentFragment.getRoot());
        }
        else {
            // create a single partition on a random node for this fragment
            ArrayList<Node> nodes = new ArrayList<>(nodeManager.getActiveNodes());
            Preconditions.checkState(!nodes.isEmpty(), "Cluster does not have any active nodes");
            Collections.shuffle(nodes, random);
            Node node = nodes.get(0);
            partitions = ImmutableList.of(new Partition(node, ImmutableList.<PlanFragmentSource>of()));
        }

        // schedule the child fragments with an output for each partition
        for (PlanFragment childPlanFragment : getChildPlanFragments(currentFragment.getRoot(), allFragments)) {
            scheduleFragment(childPlanFragment, toOutputIds(partitions), allFragments, stages);
        }

        // create a task for each partition
        List<HttpTaskClient> taskClients = ImmutableList.copyOf(transform(partitions, new Function<Partition, HttpTaskClient>()
        {
            @Override
            public HttpTaskClient apply(Partition partition)
            {
                // get fragment sources
                Map<String, ExchangePlanFragmentSource> exchangeSources = getExchangeSources(partition, currentFragment.getRoot(), stages);
                Preconditions.checkState(exchangeSources.size() <= 1, "Expected single source");

                Node node = partition.getNode();
                Request request = preparePost()
                        .setUri(uriBuilderFrom(node.getHttpUri()).replacePath("/v1/presto/task").build())
                        .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                        .setBodyGenerator(jsonBodyGenerator(queryFragmentRequestCodec, new QueryFragmentRequest(currentFragment, partition.getSplits(), exchangeSources, outputIds)))
                        .build();

                JsonResponse<QueryTaskInfo> response = httpClient.execute(request, createFullJsonResponseHandler(jsonCodec(QueryTaskInfo.class)));
                Preconditions.checkState(response.getStatusCode() == 201,
                        "Expected response code from %s to be 201, but was %d: %s",
                        request.getUri(),
                        response.getStatusCode(),
                        response.getStatusMessage());
                String location = response.getHeader("Location");
                Preconditions.checkState(location != null);

                QueryTaskInfo queryTaskInfo = response.getValue();

                // schedule table scan task on remote node
                // todo we don't need a QueryDriverProvider
                return new HttpTaskClient(queryTaskInfo.getTaskId(),
                        URI.create(location),
                        "unused",
                        queryTaskInfo.getTupleInfos(),
                        httpClient,
                        executor,
                        queryTaskInfoCodec);
            }
        }));

        // record the stage
        stages.put(currentFragment.getId(), taskClients);

        // todo if this is a blocking task
        waitForRunning(taskClients);
    }


    private List<Partition> getPartitions(PlanNode plan)
    {
        if (plan instanceof TableScan) {
            final TableScan tableScan = (TableScan) plan;

            // get splits for table
            Iterable<SplitAssignments> splitAssignments = splitManager.getSplitAssignments(tableScan.getTable());

            // divide splits amongst the nodes
            Multimap<Node, Split> nodeSplits = SplitAssignments.randomNodeAssignment(random, splitAssignments);

            // create a partition for each node
            ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
            for (Entry<Node, Collection<Split>> entry : nodeSplits.asMap().entrySet()) {
                List<PlanFragmentSource> sources = ImmutableList.copyOf(transform(entry.getValue(), new Function<Split, PlanFragmentSource>()
                {
                    @Override
                    public PlanFragmentSource apply(Split split)
                    {
                        return new TableScanPlanFragmentSource(split);
                    }
                }));
                partitions.add(new Partition(entry.getKey(), sources));
            }
            return partitions.build();
        }
        else if (plan instanceof ExchangeNode) {
            // exchange node is not partitioned
            return ImmutableList.of();
        }
        else if (plan instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) plan;
            List<Partition> leftPartitions = getPartitions(joinNode.getLeft());
            List<Partition> rightPartitions = getPartitions(joinNode.getRight());
            if (!leftPartitions.isEmpty() && !rightPartitions.isEmpty()) {
                throw new IllegalArgumentException("Both left and right join nodes are partitioned");
            }
            if (!leftPartitions.isEmpty()) {
                return leftPartitions;
            } else {
                return rightPartitions;
            }
        }
        else if (plan instanceof ProjectNode) {
            return getPartitions(((ProjectNode) plan).getSource());
        }
        else if (plan instanceof FilterNode) {
            return getPartitions(((FilterNode) plan).getSource());
        }
        else if (plan instanceof OutputPlan) {
            return getPartitions(((OutputPlan) plan).getSource());
        }
        else if (plan instanceof AggregationNode) {
            return getPartitions(((AggregationNode) plan).getSource());
        }
        else if (plan instanceof LimitNode) {
            return getPartitions(((LimitNode) plan).getSource());
        }
        else if (plan instanceof TopNNode) {
            return getPartitions(((TopNNode) plan).getSource());
        }
        else {
            throw new UnsupportedOperationException("not yet implemented: " + plan.getClass().getName());
        }
    }

    private List<PlanFragment> getChildPlanFragments(PlanNode plan, List<PlanFragment> fragments)
    {
        if (plan instanceof ExchangeNode) {
            int sourceFragmentId = ((ExchangeNode) plan).getSourceFragmentId();
            return ImmutableList.of(fragments.get(sourceFragmentId));
        }
        else if (plan instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) plan;
            List<PlanFragment> leftChildPlanFragments = getChildPlanFragments(joinNode.getLeft(), fragments);
            List<PlanFragment> rightChildPlanFragments = getChildPlanFragments(joinNode.getRight(), fragments);
            return ImmutableList.<PlanFragment>builder().addAll(leftChildPlanFragments).addAll(rightChildPlanFragments).build();
        }
        else if (plan instanceof ProjectNode) {
            return getChildPlanFragments(((ProjectNode) plan).getSource(), fragments);
        }
        else if (plan instanceof FilterNode) {
            return getChildPlanFragments(((FilterNode) plan).getSource(), fragments);
        }
        else if (plan instanceof OutputPlan) {
            return getChildPlanFragments(((OutputPlan) plan).getSource(), fragments);
        }
        else if (plan instanceof AggregationNode) {
            return getChildPlanFragments(((AggregationNode) plan).getSource(), fragments);
        }
        else if (plan instanceof LimitNode) {
            return getChildPlanFragments(((LimitNode) plan).getSource(), fragments);
        }
        else if (plan instanceof TopNNode) {
            return getChildPlanFragments(((TopNNode) plan).getSource(), fragments);
        }
        else if (plan instanceof TableScan) {
            return ImmutableList.of();
        }
        else {
            throw new UnsupportedOperationException("not yet implemented: " + plan.getClass().getName());
        }
    }

    private Map<String, ExchangePlanFragmentSource> getExchangeSources(Partition partition, PlanNode plan, ConcurrentMap<Integer, List<HttpTaskClient>> stages)
    {
        if (plan instanceof ExchangeNode) {
            int sourceFragmentId = ((ExchangeNode) plan).getSourceFragmentId();

            // get locations for the table scan tasks
            List<HttpTaskClient> queryTasks = stages.get(sourceFragmentId);
            ImmutableMap.Builder<String, URI> sources = ImmutableMap.builder();
            for (HttpTaskClient provider : queryTasks) {
                sources.put(provider.getTaskId(), provider.getLocation());
            }

            // create a single exchange source
            ExchangePlanFragmentSource exchangeSource = new ExchangePlanFragmentSource(sources.build(),
                    partition.getNode().getNodeIdentifier(),
                    queryTasks.get(0).getTupleInfos());
            return ImmutableMap.of(String.valueOf(sourceFragmentId), exchangeSource);
        }
        else if (plan instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) plan;
            Map<String, ExchangePlanFragmentSource> leftExchangeSources = getExchangeSources(partition, joinNode.getLeft(), stages);
            Map<String, ExchangePlanFragmentSource> rightExchangeSources = getExchangeSources(partition, joinNode.getRight(), stages);
            return ImmutableMap.<String, ExchangePlanFragmentSource>builder().putAll(leftExchangeSources).putAll(rightExchangeSources).build();
        }
        else if (plan instanceof ProjectNode) {
            return getExchangeSources(partition, ((ProjectNode) plan).getSource(), stages);
        }
        else if (plan instanceof FilterNode) {
            return getExchangeSources(partition, ((FilterNode) plan).getSource(), stages);
        }
        else if (plan instanceof OutputPlan) {
            return getExchangeSources(partition, ((OutputPlan) plan).getSource(), stages);
        }
        else if (plan instanceof AggregationNode) {
            return getExchangeSources(partition, ((AggregationNode) plan).getSource(), stages);
        }
        else if (plan instanceof LimitNode) {
            return getExchangeSources(partition, ((LimitNode) plan).getSource(), stages);
        }
        else if (plan instanceof TopNNode) {
            return getExchangeSources(partition, ((TopNNode) plan).getSource(), stages);
        }
        else if (plan instanceof TableScan) {
            return ImmutableMap.of();
        }
        else {
            throw new UnsupportedOperationException("not yet implemented: " + plan.getClass().getName());
        }
    }

    private static void waitForRunning(List<HttpTaskClient> taskClients)
    {
        while (true) {
            long start = System.nanoTime();

            taskClients = ImmutableList.copyOf(filter(taskClients, new Predicate<HttpTaskClient>()
            {
                @Override
                public boolean apply(HttpTaskClient taskClient)
                {
                    QueryTaskInfo queryInfo = taskClient.getQueryTaskInfo();
                    return queryInfo != null && queryInfo.getState() == State.PREPARING;
                }
            }));
            if (taskClients.isEmpty()) {
                return;
            }

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

    private static class Partition
    {
        private final Node node;
        private final List<PlanFragmentSource> splits;

        private Partition(Node node, List<PlanFragmentSource> splits)
        {
            this.node = node;
            this.splits = splits;
        }

        public Node getNode()
        {
            return node;
        }

        public List<PlanFragmentSource> getSplits()
        {
            return splits;
        }
    }

    private ImmutableList<String> toOutputIds(List<Partition> parentPartitions)
    {
        return ImmutableList.copyOf(transform(parentPartitions, new Function<Partition, String>()
        {
            @Override
            public String apply(Partition partition)
            {
                return partition.getNode().getNodeIdentifier();
            }
        }));
    }
}
