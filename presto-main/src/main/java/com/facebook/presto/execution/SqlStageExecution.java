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
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;

import javax.annotation.Nullable;
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
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.FailureInfo.toFailures;
import static com.facebook.presto.execution.StageInfo.stageStateGetter;
import static com.facebook.presto.execution.TaskInfo.taskStateGetter;
import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class SqlStageExecution
        implements SafeStageExecution
{

    private static final Logger log = Logger.get(SqlStageExecution.class);

    // NOTE: DO NOT call methods on the parent while holding a lock on the child.  Locks
    // are always acquired top down in the tree, so calling a method on the parent while
    // holding a lock on the 'this' could cause a deadlock.
    @Nullable
    private final SafeStageExecution parent;
    private final String queryId;
    private final String stageId;
    private final URI location;
    private final PlanFragment fragment;
    private final List<TupleInfo> tupleInfos;
    private final Map<PlanFragmentId, SafeStageExecution> subStages;

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

    @GuardedBy("this")
    private final Set<String> outputBuffers = new TreeSet<>();
    @GuardedBy("this")
    private boolean noMoreOutputIds;

    private final ExecutorService executor = Executors.newCachedThreadPool(threadsNamed("stage-scheduler-%n"));

    public SqlStageExecution(String queryId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeManager nodeManager,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            AtomicReference<QueryState> queryState)
    {
        this(null, queryId, new AtomicInteger(), locationFactory, plan, nodeManager, remoteTaskFactory, session, queryState);
    }

    private SqlStageExecution(@Nullable SqlStageExecution parent,
            String queryId,
            AtomicInteger nextStageId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeManager nodeManager,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            AtomicReference<QueryState> queryState)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(nextStageId, "nextStageId is null");
        Preconditions.checkNotNull(locationFactory, "locationFactory is null");
        Preconditions.checkNotNull(plan, "plan is null");
        Preconditions.checkNotNull(nodeManager, "nodeManager is null");
        Preconditions.checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(queryState, "queryState is null");

        this.parent = parent;
        this.queryId = queryId;
        this.stageId = queryId + "." + nextStageId.getAndIncrement();
        this.location = locationFactory.createStageLocation(queryId, stageId);
        this.fragment = plan.getFragment();
        this.splits = plan.getSplits();
        this.nodeManager = nodeManager;
        this.remoteTaskFactory = remoteTaskFactory;
        this.session = session;
        this.queryState = queryState;

        tupleInfos = fragment.getTupleInfos();

        ImmutableMap.Builder<PlanFragmentId, SafeStageExecution> subStages = ImmutableMap.builder();
        for (StageExecutionPlan subStagePlan : plan.getSubStages()) {
            PlanFragmentId subStageFragmentId = subStagePlan.getFragment().getId();
            SqlStageExecution subStage = new SqlStageExecution(this, queryId, nextStageId, locationFactory, subStagePlan, nodeManager, remoteTaskFactory, session, queryState);
            subStages.put(subStageFragmentId, subStage);
        }
        this.subStages = subStages.build();
    }

    @Override
    public void cancelStage(String stageId)
    {
        if (stageId.equals(this.stageId)) {
            cancel();
        }
        else {
            for (SafeStageExecution subStage : subStages.values()) {
                subStage.cancelStage(stageId);
            }
        }
    }

    @Override
    @VisibleForTesting
    public StageState getState()
    {
        return stageState.get();
    }

    public StageInfo getStageInfo()
    {
        List<TaskInfo> taskInfos = IterableTransformer.on(tasks).transform(taskInfoGetter()).list();
        List<StageInfo> subStageInfos = IterableTransformer.on(subStages.values()).transform(stageInfoGetter()).list();

        return new StageInfo(queryId,
                stageId,
                stageState.get(),
                location,
                fragment,
                tupleInfos,
                taskInfos,
                subStageInfos,
                toFailures(failureCauses));
    }

    private synchronized Set<String> getOutputBuffers()
    {
        return ImmutableSet.copyOf(outputBuffers);
    }

    public synchronized void addOutputBuffer(String outputId)
    {
        Preconditions.checkNotNull(outputId, "outputId is null");
        Preconditions.checkArgument(!outputBuffers.contains(outputId), "Stage already has an output %s", outputId);

        outputBuffers.add(outputId);

        // wake up worker thread waiting for new buffers
        this.notifyAll();
    }

    public synchronized void noMoreOutputBuffers()
    {
        noMoreOutputIds = true;

        // wake up worker thread waiting for new buffers
        this.notifyAll();
    }

    private synchronized Multimap<PlanNodeId, URI> getExchangeLocations()
    {
        ImmutableMultimap.Builder<PlanNodeId, URI> exchangeLocations = ImmutableMultimap.builder();
        for (PlanNode planNode : fragment.getSources()) {
            if (planNode instanceof ExchangeNode) {
                ExchangeNode exchangeNode = (ExchangeNode) planNode;
                SafeStageExecution subStage = subStages.get(exchangeNode.getSourceFragmentId());
                Preconditions.checkState(subStage != null, "Unknown sub stage %s, known stages %s", exchangeNode.getSourceFragmentId(), subStages.keySet());
                exchangeLocations.putAll(exchangeNode.getId(), subStage.getTaskLocations());
            }
        }
        return exchangeLocations.build();
    }

    @Override
    @VisibleForTesting
    public synchronized List<URI> getTaskLocations()
    {
        ImmutableList.Builder<URI> locations = ImmutableList.builder();
        for (RemoteTask task : tasks) {
            locations.add(task.getTaskInfo().getSelf());
        }
        return locations.build();
    }

    public Future<?> start()
    {
        return scheduleStartTasks();
    }

    @Override
    @VisibleForTesting
    public Future<?> scheduleStartTasks()
    {
        // start sub-stages (starts bottom-up)
        for (SafeStageExecution subStage : subStages.values()) {
            subStage.scheduleStartTasks();
        }
        return executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                startTasks();
            }
        });
    }

    private void startTasks()
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
            partitions = ImmutableList.of(new Partition(node, ImmutableList.<Split>of()));
        }
        else {
            // divide splits amongst the nodes
            Multimap<Node, Split> nodeSplits = SplitAssignments.balancedNodeAssignment(queryState, splits.get());

            // create a partition for each node
            ImmutableList.Builder<Partition> partitionBuilder = ImmutableList.builder();
            for (Entry<Node, Collection<Split>> entry : nodeSplits.asMap().entrySet()) {
                partitionBuilder.add(new Partition(entry.getKey(), entry.getValue()));
            }
            partitions = partitionBuilder.build();
        }

        // plan tasks
        int nextTaskId = 0;
        for (Partition partition : partitions) {
            // stop is stage is already done
            if (getState().isDone()) {
                return;
            }

            String nodeIdentifier = partition.getNode().getNodeIdentifier();
            String taskId = stageId + '.' + nextTaskId++;

            RemoteTask task = remoteTaskFactory.createRemoteTask(session,
                    queryId,
                    stageId,
                    taskId,
                    partition.getNode(),
                    fragment,
                    getExchangeLocations(),
                    getOutputBuffers());

            try {
                // start the task
                task.start(partition.getSplits());

                // record this task
                tasks.add(task);

                // stop is stage is already done
                if (getState().isDone()) {
                    return;
                }

                // tell the sub stages to create a buffer for this task
                for (SafeStageExecution subStage : subStages.values()) {
                    subStage.addOutputBuffer(nodeIdentifier);
                }

                // no more splits
                // todo move this
                task.noMoreSplits();
            }
            catch (Throwable e) {
                synchronized (this) {
                    failureCauses.add(e);
                    transitionToState(StageState.FAILED);
                }
                log.error(e, "Stage %s failed to start", stageId);
                cancel();
                throw Throwables.propagate(e);
            }
        }

        transitionToState(StageState.SCHEDULED);

        notifyParentSubStageFinishedScheduling();

        // tell sub stages there will be no more output buffers
        for (SafeStageExecution subStage : subStages.values()) {
            subStage.noMoreOutputBuffers();
        }

        // add the missing exchanges output buffers
        addNewExchangesAndBuffers();
    }

    private void notifyParentSubStageFinishedScheduling()
    {
        // calling this while holding a lock on 'this' can cause a deadlock
        Preconditions.checkState(!Thread.holdsLock(this), "Parent can not be notified while holding a lock on the child");
        if (parent != null) {
            parent.subStageFinishedScheduling();
        }
    }

    @VisibleForTesting
    public synchronized void subStageFinishedScheduling()
    {
        this.notifyAll();
    }

    private void addNewExchangesAndBuffers()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not add exchanges or buffers to tasks while holding a lock on this");

        while (!getState().isDone()) {
            // before updating check if this is the last update we need to make
            boolean done = exchangesAndBuffersComplete();

            // update tasks
            Multimap<PlanNodeId, URI> exchangeLocations = getExchangeLocations();
            Set<String> outputBuffers = getOutputBuffers();
            for (RemoteTask task : tasks) {
                task.addExchangeLocations(exchangeLocations, done);
                task.addOutputBuffers(outputBuffers, done);
            }

            if (done) {
                return;
            }

            waitForMoreData(exchangeLocations, outputBuffers);
        }
    }

    private boolean exchangesAndBuffersComplete()
    {
        for (SafeStageExecution subStage : subStages.values()) {
            switch (subStage.getState()) {
                case PLANNED:
                case SCHEDULING:
                    return false;
            }
        }
        synchronized (this) {
            return noMoreOutputIds;
        }
    }

    private synchronized void waitForMoreData(Multimap<PlanNodeId, URI> exchangeLocations, Set<String> outputBuffers)
    {
        while (true) {
            // if next loop will finish, don't wait
            if (exchangesAndBuffersComplete()) {
                return;
            }

            // data has already changed, don't wait
            if (!outputBuffers.equals(getOutputBuffers())) {
                return;
            }
            if (!exchangeLocations.equals(getExchangeLocations())) {
                return;
            }

            try {
                this.wait(1000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
    }

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
        for (SafeStageExecution subStage : subStages.values()) {
            subStage.updateState();
        }

        synchronized (this) {
            StageState currentState = stageState.get();
            if (currentState.isDone()) {
                return;
            }

            List<StageState> subStageStates = ImmutableList.copyOf(transform(transform(subStages.values(), stageInfoGetter()), stageStateGetter()));
            if (any(subStageStates, equalTo(StageState.FAILED))) {
                StageState doneState = StageState.FAILED;
                transitionToState(doneState);
            }
            else {
                List<TaskState> taskStates = ImmutableList.copyOf(transform(transform(tasks, taskInfoGetter()), taskStateGetter()));
                if (any(taskStates, equalTo(TaskState.FAILED))) {
                    transitionToState(StageState.FAILED);
                }
                else if (currentState != StageState.PLANNED && currentState != StageState.SCHEDULING) {
                    // all tasks are now scheduled, so we can check the finished state
                    if (all(taskStates, TaskState.inDoneState())) {
                        transitionToState(StageState.FINISHED);
                    }
                    else if (any(taskStates, equalTo(TaskState.RUNNING))) {
                        transitionToState(StageState.RUNNING);
                    }
                    else if (any(taskStates, equalTo(TaskState.QUEUED))) {
                        transitionToState(StageState.SCHEDULED);
                    }
                }
            }
        }

        if (stageState.get().isDone()) {
            // finish tasks and stages
            cancelAll();
        }
    }

    public void cancel()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not cancel while holding a lock on this");

        // transition to canceled state, only if not already finished
        synchronized (this) {
            if (stageState.get().isDone()) {
                return;
            }
            log.debug("Cancelling stage %s", stageId);
            transitionToState(StageState.CANCELED);
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
        for (SafeStageExecution subStage : subStages.values()) {
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

    private void transitionToState(StageState newState)
    {
        StageState oldState = stageState.getAndSet(newState);
        if (oldState != newState) {
            log.debug("Stage %s is %s", stageId, newState);
        }
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

    public static Function<SafeStageExecution, StageInfo> stageInfoGetter()
    {
        return new Function<SafeStageExecution, StageInfo>()
        {
            @Override
            public StageInfo apply(SafeStageExecution stage)
            {
                return stage.getStageInfo();
            }
        };
    }
}

/*
 * Since the execution is a tree of SqlStateExecutions, each stage can directly access
 * the private fields and methods of stages up and down the tree.  To prevent accidental
 * errors, each stage reference parents and children using this interface so direct
 * access is not possible.
 */
interface SafeStageExecution
{
    StageInfo getStageInfo();

    StageState getState();

    Future<?> scheduleStartTasks();

    void addOutputBuffer(String nodeIdentifier);

    void noMoreOutputBuffers();

    Iterable<? extends URI> getTaskLocations();

    void subStageFinishedScheduling();

    void updateState();

    void cancelStage(String stageId);

    void cancel();
}


