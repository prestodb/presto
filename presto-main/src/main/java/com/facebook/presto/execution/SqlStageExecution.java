/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.SplitAssignments;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.Partition;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.sql.planner.TableScanPlanFragmentSource;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.FailureInfo.toFailures;
import static com.facebook.presto.execution.StageInfo.stageStateGetter;
import static com.facebook.presto.execution.TaskInfo.taskStateGetter;
import static com.facebook.presto.sql.planner.Partition.nodeIdentifierGetter;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class SqlStageExecution
        implements StageExecution
{
    private static final Logger log = Logger.get(SqlStageExecution.class);

    private final String queryId;
    private final String stageId;
    private final URI location;
    private final PlanFragment plan;
    private final List<TupleInfo> tupleInfos;
    private final Map<PlanFragmentId, SqlStageExecution> subStages;

    private final Collection<RemoteTask> tasks = new LinkedBlockingQueue<>();

    private final NodeManager nodeManager; // only used to grab a single random node for a unpartitioned query
    private final Optional<Iterable<SplitAssignments>> splits;
    private final RemoteTaskFactory remoteTaskFactory;
    private final Session session; // only used for remote task factory
    private final AtomicReference<QueryState> queryState;
    private final Random random = new Random();

    // Changes to state must happen within a synchronized lock.
    // The only reason we use an atomic reference here is so read-only threads don't have to block.
    @GuardedBy("this")
    private final AtomicReference<StageState> stageState = new AtomicReference<>(StageState.PLANNED);

    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

    public SqlStageExecution(String queryId,
            String stageId,
            URI location,
            PlanFragment plan,
            Iterable<? extends StageExecution> subStages,
            NodeManager nodeManager,
            Optional<Iterable<SplitAssignments>> splits,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            AtomicReference<QueryState> queryState)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(plan, "plan is null");
        Preconditions.checkNotNull(subStages, "subStages is null");

        this.queryId = queryId;
        this.stageId = stageId;
        this.location = location;
        this.plan = plan;
        this.subStages = IterableTransformer.on(subStages)
                .cast(SqlStageExecution.class)
                .uniqueIndex(fragmentIdGetter())
                .immutableMap();


        this.nodeManager = nodeManager;
        this.splits = splits;
        this.remoteTaskFactory = remoteTaskFactory;
        this.session = session;
        this.queryState = queryState;

        tupleInfos = ImmutableList.copyOf(IterableTransformer.on(plan.getRoot().getOutputSymbols())
                .transform(Functions.forMap(plan.getSymbols()))
                .transform(com.facebook.presto.sql.analyzer.Type.toRaw())
                .transform(new Function<Type, TupleInfo>()
                {
                    @Override
                    public TupleInfo apply(Type input)
                    {
                        return new TupleInfo(input);
                    }
                })
                .list());
    }

    @Override
    public String getStageId()
    {
        return stageId;
    }

    @Override
    public List<StageExecution> getSubStages()
    {
        return ImmutableList.<StageExecution>copyOf(subStages.values());
    }

    @Override
    public ExchangePlanFragmentSource getExchangeSourceFor(String outputId)
    {
        Preconditions.checkNotNull(outputId, "outputId is null");

        // get locations for the dependent stage
        ImmutableMap.Builder<String, URI> sources = ImmutableMap.builder();
        for (RemoteTask task : tasks) {
            sources.put(task.getTaskId(), task.getTaskInfo().getSelf());
        }

        return new ExchangePlanFragmentSource(sources.build(), outputId, tupleInfos);
    }

    @Override
    public StageInfo getStageInfo()
    {
        List<TaskInfo> taskInfos = IterableTransformer.on(tasks).transform(taskInfoGetter()).list();
        List<StageInfo> subStageInfos = IterableTransformer.on(subStages.values()).transform(stageInfoGetter()).list();

        return new StageInfo(queryId,
                stageId,
                stageState.get(),
                location,
                plan,
                tupleInfos,
                taskInfos,
                subStageInfos,
                toFailures(failureCauses));
    }

    public void startTasks(List<String> outputIds)
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not start while holding a lock on this");

        // transition to scheduling
        synchronized (this) {
            if (!stageState.compareAndSet(StageState.PLANNED, StageState.SCHEDULING)) {
                // stage has already been started, has been canceled or has no tasks due to partition pruning
                return;
            }
        }

        // determine partitions
        List<Partition> partitions;
        if (!splits.isPresent()) {
            // create a single partition on a random node for this fragment
            ArrayList<Node> nodes = new ArrayList<>(nodeManager.getActiveNodes());
            Preconditions.checkState(!nodes.isEmpty(), "Cluster does not have any active nodes");
            Collections.shuffle(nodes, random);
            Node node = nodes.get(0);
            partitions = ImmutableList.of(new Partition(node, ImmutableList.<PlanFragmentSource>of()));
        } else {
            // divide splits amongst the nodes
            Multimap<Node, Split> nodeSplits = SplitAssignments.balancedNodeAssignment(queryState, splits.get());

            // create a partition for each node
            ImmutableList.Builder<Partition> partitionBuilder = ImmutableList.builder();
            for (Entry<Node, Collection<Split>> entry : nodeSplits.asMap().entrySet()) {
                List<PlanFragmentSource> sources = ImmutableList.copyOf(transform(entry.getValue(), new Function<Split, PlanFragmentSource>()
                {
                    @Override
                    public PlanFragmentSource apply(Split split)
                    {
                        return new TableScanPlanFragmentSource(split);
                    }
                }));
                partitionBuilder.add(new Partition(entry.getKey(), sources));
            }
            partitions = partitionBuilder.build();
        }

        // start sub-stages (starts bottom-up)
        // tell the sub-stages to create an output buffer for each node
        List<String> nodeIds = IterableTransformer.on(partitions).transform(nodeIdentifierGetter()).list();
        for (StageExecution subStage : subStages.values()) {
            subStage.startTasks(nodeIds);
        }

        Set<ExchangeNode> exchanges = IterableTransformer.on(plan.getSources())
                .select(Predicates.instanceOf(ExchangeNode.class))
                .cast(ExchangeNode.class)
                .set();

        // plan tasks
        int nextTaskId = 0;
        for (Partition partition : partitions) {
            String nodeIdentifier = partition.getNode().getNodeIdentifier();

            ImmutableMap.Builder<PlanNodeId, PlanFragmentSource> fixedSources = ImmutableMap.builder();
            for (ExchangeNode exchange : exchanges) {
                StageExecution childStage = subStages.get(exchange.getSourceFragmentId());
                ExchangePlanFragmentSource source = childStage.getExchangeSourceFor(nodeIdentifier);

                fixedSources.put(exchange.getId(), source);
            }

            String taskId = stageId + '.' + nextTaskId++;
            RemoteTask task = remoteTaskFactory.createRemoteTask(session,
                    queryId,
                    stageId,
                    taskId,
                    partition.getNode(),
                    plan,
                    fixedSources.build(),
                    outputIds);

            tasks.add(task);

            try {
                task.start();
                for (PlanFragmentSource source : partition.getSplits()) {
                    TableScanPlanFragmentSource tableScanSource = (TableScanPlanFragmentSource) source;
                    task.addSource(plan.getPartitionedSource(), tableScanSource.getSplit());
                }
                task.noMoreSources();
            }
            catch (Throwable e) {
                synchronized (this) {
                    failureCauses.add(e);
                    stageState.set(StageState.FAILED);
                }
                log.error(e, "Stage %s failed to start", stageId);
                cancel();
                throw Throwables.propagate(e);
            }

        }
        stageState.set(StageState.SCHEDULED);
    }

    @Override
    public void updateState()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not update state while holding a lock on this");

        // propagate update to tasks and stages
        for (RemoteTask task : tasks) {
            try {
                task.updateState();
            }
            catch (Exception e) {
                log.debug(e, "Error updating task info");
            }
        }
        for (StageExecution subStage : subStages.values()) {
            subStage.updateState();
        }

        synchronized (this) {
            StageState currentState = stageState.get();
            if (currentState.isDone()) {
                return;
            }

            List<StageState> subStageStates = ImmutableList.copyOf(transform(transform(subStages.values(), stageInfoGetter()), stageStateGetter()));
            if (any(subStageStates, equalTo(StageState.FAILED))) {
                stageState.set(StageState.FAILED);
            } else {
                List<TaskState> taskStates = ImmutableList.copyOf(transform(transform(tasks, taskInfoGetter()), taskStateGetter()));
                if (any(taskStates, equalTo(TaskState.FAILED))) {
                    stageState.set(StageState.FAILED);
                } else if (currentState != StageState.PLANNED && currentState != StageState.SCHEDULING) {
                    // all tasks are now scheduled, so we can check the finished state
                    if (all(taskStates, TaskState.inDoneState())) {
                        stageState.set(StageState.FINISHED);
                    }
                    else if (any(taskStates, equalTo(TaskState.RUNNING))) {
                        stageState.set(StageState.RUNNING);
                    }
                    else if (any(taskStates, equalTo(TaskState.QUEUED))) {
                        stageState.set(StageState.SCHEDULED);
                    }
                }
            }
        }

        if (stageState.get().isDone()) {
            // finish tasks and stages
            cancelAll();
        }
    }

    @Override
    public void cancel()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not cancel while holding a lock on this");

        // transition to canceled state, only if not already finished
        synchronized (this) {
            if (stageState.get().isDone()) {
                return;
            }
            log.debug("Cancelling stage %s", stageId);
            stageState.set(StageState.CANCELED);
        }

        cancelAll();
    }

    private void cancelAll()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not cancel while holding a lock on this");

        // propagate update to tasks and stages
        for (RemoteTask task : tasks) {
            task.cancel();
        }
        for (StageExecution subStage : subStages.values()) {
            subStage.cancel();
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("stageId", stageId)
                .add("location", location)
                .add("stageState", stageState.get())
                .toString();
    }

    public static Function<RemoteTask, TaskInfo> taskInfoGetter()
    {
        return new Function<RemoteTask, TaskInfo>()
        {
            @Override
            public TaskInfo apply(RemoteTask remoteTask)
            {
                return remoteTask.getTaskInfo();
            }
        };
    }
    public static Function<StageExecution, StageInfo> stageInfoGetter()
    {
        return new Function<StageExecution, StageInfo>()
        {
            @Override
            public StageInfo apply(StageExecution stage)
            {
                return stage.getStageInfo();
            }
        };
    }

    private Function<SqlStageExecution, PlanFragmentId> fragmentIdGetter()
    {
        return new Function<SqlStageExecution, PlanFragmentId>()
        {
            @Override
            public PlanFragmentId apply(SqlStageExecution input)
            {
                return input.plan.getId();
            }
        };
    }
}
