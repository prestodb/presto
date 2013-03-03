/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.SplitAssignments;
import com.facebook.presto.sql.analyzer.Session;
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
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

    private final ConcurrentMap<Node, RemoteTask> tasks = new ConcurrentHashMap<>();

    private final NodeManager nodeManager; // only used to grab a single random node for a unpartitioned query
    private final Optional<Iterable<SplitAssignments>> splits;
    private final RemoteTaskFactory remoteTaskFactory;
    private final Session session; // only used for remote task factory
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
            Session session)
    {
        this(null, queryId, new AtomicInteger(), locationFactory, plan, nodeManager, remoteTaskFactory, session);
    }

    private SqlStageExecution(@Nullable SqlStageExecution parent,
            String queryId,
            AtomicInteger nextStageId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeManager nodeManager,
            RemoteTaskFactory remoteTaskFactory,
            Session session)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(nextStageId, "nextStageId is null");
        Preconditions.checkNotNull(locationFactory, "locationFactory is null");
        Preconditions.checkNotNull(plan, "plan is null");
        Preconditions.checkNotNull(nodeManager, "nodeManager is null");
        Preconditions.checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        Preconditions.checkNotNull(session, "session is null");

        this.parent = parent;
        this.queryId = queryId;
        this.stageId = queryId + "." + nextStageId.getAndIncrement();
        this.location = locationFactory.createStageLocation(queryId, stageId);
        this.fragment = plan.getFragment();
        this.splits = plan.getSplits();
        this.nodeManager = nodeManager;
        this.remoteTaskFactory = remoteTaskFactory;
        this.session = session;

        tupleInfos = fragment.getTupleInfos();

        ImmutableMap.Builder<PlanFragmentId, SafeStageExecution> subStages = ImmutableMap.builder();
        for (StageExecutionPlan subStagePlan : plan.getSubStages()) {
            PlanFragmentId subStageFragmentId = subStagePlan.getFragment().getId();
            SqlStageExecution subStage = new SqlStageExecution(this, queryId, nextStageId, locationFactory, subStagePlan, nodeManager, remoteTaskFactory, session);
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
        List<TaskInfo> taskInfos = IterableTransformer.on(tasks.values()).transform(taskInfoGetter()).list();
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
        for (RemoteTask task : tasks.values()) {
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
        try {
            Preconditions.checkState(!Thread.holdsLock(this), "Can not start while holding a lock on this");

            // transition to scheduling
            synchronized (this) {
                if (!stageState.compareAndSet(StageState.PLANNED, StageState.SCHEDULING)) {
                    // stage has already been started, has been canceled or has no tasks due to partition pruning
                    return;
                }
            }

            // determine partitions
            AtomicInteger nextTaskId = new AtomicInteger(0);
            if (!splits.isPresent()) {
                // create a single partition on a random node for this fragment
                ArrayList<Node> nodes = new ArrayList<>(nodeManager.getActiveNodes());
                Preconditions.checkState(!nodes.isEmpty(), "Cluster does not have any active nodes");
                Collections.shuffle(nodes, random);
                Node randomNode = nodes.get(0);

                scheduleTask(nextTaskId, randomNode, null);
            }
            else {
                // remember how many splits have been assigned to each node
                final Object2IntOpenHashMap<Node> nodeSplits = new Object2IntOpenHashMap<>();
                Comparator<Node> byAssignedSplitsCount = new Comparator<Node>()
                {
                    @Override
                    public int compare(Node o1, Node o2)
                    {
                        return Ints.compare(nodeSplits.getInt(o1), nodeSplits.getInt(o2));
                    }
                };

                for (SplitAssignments assignment : splits.get()) {
                    // if query has been canceled, exit cleanly; query will never run regardless
                    if (getState().isDone()) {
                        break;
                    }

                    // for each split, pick the node with the smallest number of assignments
                    Node chosen = Ordering.from(byAssignedSplitsCount).min(assignment.getNodes());
                    nodeSplits.add(chosen, 1);

                    RemoteTask task = tasks.get(chosen);
                    if (task == null) {
                        scheduleTask(nextTaskId, chosen, assignment.getSplit());
                    }
                    else {
                        task.addSplit(assignment.getSplit());
                    }
                }

                for (RemoteTask task : tasks.values()) {
                    task.noMoreSplits();
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
        catch (Throwable e) {
            // some exceptions can occur when the query finishes early
            if (!getState().isDone()) {
                transitionToState(StageState.FAILED);
                log.error(e, "Error while starting stage %s", stageId);
                throw e;
            }
        }
    }

    private RemoteTask scheduleTask(AtomicInteger nextTaskId, Node node, @Nullable Split initialSplit)
    {
        String nodeIdentifier = node.getNodeIdentifier();
        String taskId = stageId + '.' + nextTaskId.getAndIncrement();

        RemoteTask task = remoteTaskFactory.createRemoteTask(session,
                queryId,
                stageId,
                taskId,
                node,
                fragment,
                getExchangeLocations(),
                getOutputBuffers());

        try {
            // start the task
            task.start(initialSplit);

            // record this task
            tasks.put(node, task);

            // stop is stage is already done
            if (getState().isDone()) {
                return task;
            }

            // tell the sub stages to create a buffer for this task
            for (SafeStageExecution subStage : subStages.values()) {
                subStage.addOutputBuffer(nodeIdentifier);
            }

            return task;
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
            boolean exchangesComplete = exchangesAreComplete();
            boolean outputComplete;
            synchronized (this) {
                outputComplete = noMoreOutputIds;
            }

            // update tasks
            Multimap<PlanNodeId, URI> exchangeLocations = getExchangeLocations();
            Set<String> outputBuffers = getOutputBuffers();
            for (RemoteTask task : tasks.values()) {
                task.addExchangeLocations(exchangeLocations, exchangesComplete);
                task.addOutputBuffers(outputBuffers, outputComplete);
            }

            if (exchangesComplete && outputComplete) {
                return;
            }

            waitForMoreExchangesAndBuffers(exchangeLocations, exchangesComplete, outputBuffers, outputComplete);
        }
    }

    private synchronized void waitForMoreExchangesAndBuffers(Multimap<PlanNodeId, URI> exchangeLocations,
            boolean exchangesComplete,
            Set<String> outputBuffers,
            boolean outputComplete)
    {
        while (true) {
            // if next loop will finish, don't wait
            if (exchangesAreComplete() && noMoreOutputIds) {
                return;
            }

            // data has already changed, don't wait
            if (exchangesComplete != exchangesAreComplete()) {
                return;
            }
            if (outputComplete != noMoreOutputIds) {
                return;
            }
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

    private boolean exchangesAreComplete()
    {
        for (SafeStageExecution subStage : subStages.values()) {
            switch (subStage.getState()) {
                case PLANNED:
                case SCHEDULING:
                    return false;
            }
        }
        return true;
    }

    public void updateState()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not update state while holding a lock on this");

        // propagate update to tasks and stages
        for (RemoteTask task : tasks.values()) {
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
                List<TaskState> taskStates = ImmutableList.copyOf(transform(transform(tasks.values(), taskInfoGetter()), taskStateGetter()));
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
        for (RemoteTask task : tasks.values()) {
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


