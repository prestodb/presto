/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.execution.NodeScheduler.NodeSelector;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.spi.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.OutputReceiver;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.SetThreadName;
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
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.execution.StageInfo.stageStateGetter;
import static com.facebook.presto.execution.TaskInfo.taskStateGetter;
import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class SqlStageExecution
        implements StageExecutionNode
{
    private static final Logger log = Logger.get(SqlStageExecution.class);

    // NOTE: DO NOT call methods on the parent while holding a lock on the child.  Locks
    // are always acquired top down in the tree, so calling a method on the parent while
    // holding a lock on the 'this' could cause a deadlock.
    // This is only here to aid in debugging
    @Nullable
    private final StageExecutionNode parent;
    private final StageId stageId;
    private final URI location;
    private final PlanFragment fragment;
    private final List<TupleInfo> tupleInfos;
    private final Map<PlanFragmentId, StageExecutionNode> subStages;
    private final Map<PlanNodeId, OutputReceiver> outputReceivers;

    private final ConcurrentMap<Node, RemoteTask> tasks = new ConcurrentHashMap<>();

    private final Optional<DataSource> dataSource;
    private final RemoteTaskFactory remoteTaskFactory;
    private final Session session; // only used for remote task factory
    private final int maxPendingSplitsPerNode;

    private final StateMachine<StageState> stageState;

    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

    @GuardedBy("this")
    private final Set<String> outputBuffers = new TreeSet<>();
    @GuardedBy("this")
    private boolean noMoreOutputIds;

    private final ExecutorService executor;

    private final StageStats stageStats = new StageStats();

    private final NodeSelector nodeSelector;

    public SqlStageExecution(QueryId queryId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            int maxPendingSplitsPerNode,
            ExecutorService executor)
    {
        this(null, queryId, new AtomicInteger(), locationFactory, plan, nodeScheduler, remoteTaskFactory, session, maxPendingSplitsPerNode, executor);
    }

    private SqlStageExecution(@Nullable StageExecutionNode parent,
            QueryId queryId,
            AtomicInteger nextStageId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            int maxPendingSplitsPerNode,
            ExecutorService executor)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(nextStageId, "nextStageId is null");
        Preconditions.checkNotNull(locationFactory, "locationFactory is null");
        Preconditions.checkNotNull(plan, "plan is null");
        Preconditions.checkNotNull(nodeScheduler, "nodeScheduler is null");
        Preconditions.checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkArgument(maxPendingSplitsPerNode > 0, "maxPendingSplitsPerNode must be greater than 0");
        Preconditions.checkNotNull(executor, "executor is null");

        this.stageId = new StageId(queryId, String.valueOf(nextStageId.getAndIncrement()));
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
            this.parent = parent;
            this.location = locationFactory.createStageLocation(stageId);
            this.fragment = plan.getFragment();
            this.outputReceivers = plan.getOutputReceivers();
            this.dataSource = plan.getDataSource();
            this.remoteTaskFactory = remoteTaskFactory;
            this.session = session;
            this.maxPendingSplitsPerNode = maxPendingSplitsPerNode;
            this.executor = executor;

            tupleInfos = fragment.getTupleInfos();

            ImmutableMap.Builder<PlanFragmentId, StageExecutionNode> subStages = ImmutableMap.builder();
            for (StageExecutionPlan subStagePlan : plan.getSubStages()) {
                PlanFragmentId subStageFragmentId = subStagePlan.getFragment().getId();
                StageExecutionNode subStage = new SqlStageExecution(this,
                        queryId,
                        nextStageId,
                        locationFactory,
                        subStagePlan,
                        nodeScheduler,
                        remoteTaskFactory,
                        session,
                        maxPendingSplitsPerNode, executor);

                subStage.addStateChangeListener(new StateChangeListener<StageInfo>()
                {
                    @Override
                    public void stateChanged(StageInfo stageInfo)
                    {
                        doUpdateState();
                    }
                });

                subStages.put(subStageFragmentId, subStage);
            }
            this.subStages = subStages.build();

            String dataSourceName = dataSource.isPresent() ? dataSource.get().getDataSourceName() : null;
            this.nodeSelector = nodeScheduler.createNodeSelector(dataSourceName, Ordering.natural().onResultOf(new Function<Node, Integer>()
            {
                @Override
                public Integer apply(Node input)
                {
                    RemoteTask task = tasks.get(input);
                    return task == null ? 0 : task.getQueuedSplits();
                }
            }));
            stageState = new StateMachine<>("stage " + stageId, this.executor, StageState.PLANNED);
            stageState.addStateChangeListener(new StateChangeListener<StageState>()
        {
            @Override
            public void stateChanged(StageState newValue)
            {
                log.debug("Stage %s is %s", stageId, newValue);
            }
        });
        }
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
            if (stageId.equals(this.stageId)) {
                cancel();
            }
            else {
                for (StageExecutionNode subStage : subStages.values()) {
                    subStage.cancelStage(stageId);
                }
            }
        }
    }

    @Override
    @VisibleForTesting
    public StageState getState()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
            return stageState.get();
        }
    }

    public StageInfo getStageInfo()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
            List<TaskInfo> taskInfos = IterableTransformer.on(tasks.values()).transform(taskInfoGetter()).list();
            List<StageInfo> subStageInfos = IterableTransformer.on(subStages.values()).transform(stageInfoGetter()).list();

            return new StageInfo(stageId,
                    stageState.get(),
                    location,
                    fragment,
                    tupleInfos,
                    stageStats.snapshot(),
                    taskInfos,
                    subStageInfos,
                    toFailures(failureCauses));
        }
    }

    private synchronized Set<String> getOutputBuffers()
    {
        return ImmutableSet.copyOf(outputBuffers);
    }

    public synchronized void addOutputBuffer(String outputId)
    {
        Preconditions.checkNotNull(outputId, "outputId is null");
        Preconditions.checkArgument(!outputBuffers.contains(outputId), "Stage already has an output %s", outputId);

        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
            outputBuffers.add(outputId);

            // wake up worker thread waiting for new buffers
            this.notifyAll();
        }
    }

    public synchronized void noMoreOutputBuffers()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
            noMoreOutputIds = true;

            // wake up worker thread waiting for new buffers
            this.notifyAll();
        }
    }

    @Override
    public void addStateChangeListener(final StateChangeListener<StageInfo> stateChangeListener)
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
            stageState.addStateChangeListener(new StateChangeListener<StageState>()
        {
            @Override
            public void stateChanged(StageState newValue)
            {
                stateChangeListener.stateChanged(getStageInfo());
            }
        });
        }
    }

    private Multimap<PlanNodeId, URI> getExchangeLocations()
    {
        ImmutableMultimap.Builder<PlanNodeId, URI> exchangeLocations = ImmutableMultimap.builder();
        for (PlanNode planNode : fragment.getSources()) {
            if (planNode instanceof ExchangeNode) {
                ExchangeNode exchangeNode = (ExchangeNode) planNode;
                for (PlanFragmentId planFragmentId : exchangeNode.getSourceFragmentIds()) {
                    StageExecutionNode subStage = subStages.get(planFragmentId);
                    Preconditions.checkState(subStage != null, "Unknown sub stage %s, known stages %s", planFragmentId, subStages.keySet());
                    exchangeLocations.putAll(exchangeNode.getId(), subStage.getTaskLocations());
                }
            }
        }
        return exchangeLocations.build();
    }

    @Override
    @VisibleForTesting
    public synchronized List<URI> getTaskLocations()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
            ImmutableList.Builder<URI> locations = ImmutableList.builder();
            for (RemoteTask task : tasks.values()) {
                locations.add(task.getTaskInfo().getSelf());
            }
            return locations.build();
        }
    }

    public Future<?> start()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
            return scheduleStartTasks();
        }
    }

    @Override
    @VisibleForTesting
    public Future<?> scheduleStartTasks()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
            // start sub-stages (starts bottom-up)
            for (StageExecutionNode subStage : subStages.values()) {
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
    }

    private void startTasks()
    {
        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
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
                if (!dataSource.isPresent()) {
                    // create a single partition on a random node for this fragment
                    scheduleTask(nextTaskId, nodeSelector.selectRandomNode(), null);
                }
                else {
                    long getSplitStart = System.nanoTime();
                    for (Split split : dataSource.get().getSplits()) {
                        stageStats.addGetSplitDuration(Duration.nanosSince(getSplitStart));

                        long scheduleSplitStart = System.nanoTime();
                        Node chosen = chooseNode(nodeSelector, split);

                        // if query has been canceled, exit cleanly; query will never run regardless
                        if (getState().isDone()) {
                            break;
                        }

                        RemoteTask task = tasks.get(chosen);
                        if (task == null) {
                            scheduleTask(nextTaskId, chosen, split);
                            stageStats.addScheduleTaskDuration(Duration.nanosSince(scheduleSplitStart));
                        }
                        else {
                            task.addSplit(split);
                            stageStats.addAddSplitDuration(Duration.nanosSince(scheduleSplitStart));
                        }

                        getSplitStart = System.nanoTime();
                    }

                    for (RemoteTask task : tasks.values()) {
                        task.noMoreSplits();
                    }
                }

                stageState.set(StageState.SCHEDULED);

                // tell sub stages there will be no more output buffers
                for (StageExecutionNode subStage : subStages.values()) {
                    subStage.noMoreOutputBuffers();
                }

                // add the missing exchanges output buffers
                addNewExchangesAndBuffers(true);
            }
            catch (Throwable e) {
                // some exceptions can occur when the query finishes early
                if (!getState().isDone()) {
                    synchronized (this) {
                        failureCauses.add(e);
                        stageState.set(StageState.FAILED);
                    }
                    log.error(e, "Error while starting stage %s", stageId);
                    cancelAll();
                    throw e;
                }
                Throwables.propagateIfInstanceOf(e, Error.class);
                log.debug(e, "Error while starting stage in done query %s", stageId);
            }
            finally {
                doUpdateState();
            }
        }
    }

    private Node chooseNode(NodeSelector nodeSelector, Split split)
    {
        while (true) {
            // if query has been canceled, exit
            if (getState().isDone()) {
                return null;
            }

            // for each split, pick the node with the smallest number of assignments
            Node chosen = nodeSelector.selectNode(split);

            // if the chosen node doesn't have too many tasks already, return
            RemoteTask task = tasks.get(chosen);
            if (task == null || task.getQueuedSplits() < maxPendingSplitsPerNode) {
                return chosen;
            }

            synchronized (this) {
                // otherwise wait for some tasks to complete
                try {
                    // todo this adds latency: replace this wait with an event listener
                    TimeUnit.SECONDS.timedWait(this, 1);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
            }

            addNewExchangesAndBuffers(false);
        }
    }

    private RemoteTask scheduleTask(AtomicInteger nextTaskId, Node node, @Nullable Split initialSplit)
    {
        String nodeIdentifier = node.getNodeIdentifier();
        TaskId taskId = new TaskId(stageId, String.valueOf(nextTaskId.getAndIncrement()));

        RemoteTask task = remoteTaskFactory.createRemoteTask(session,
                taskId,
                node,
                fragment,
                initialSplit,
                outputReceivers,
                getExchangeLocations(),
                getOutputBuffers());

        task.addStateChangeListener(new StateChangeListener<TaskInfo>()
        {
            @Override
            public void stateChanged(TaskInfo taskInfo)
            {
                doUpdateState();
            }
        });

        // create and update task
        task.start();

        // record this task
        tasks.put(node, task);

        // update in case task finished before listener was registered
        doUpdateState();

        // stop if stage is already done
        if (getState().isDone()) {
            return task;
        }

        // tell the sub stages to create a buffer for this task
        for (StageExecutionNode subStage : subStages.values()) {
            subStage.addOutputBuffer(nodeIdentifier);
        }

        return task;
    }

    private void addNewExchangesAndBuffers(boolean waitUntilFinished)
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

            if (waitUntilFinished) {
                waitForMoreExchangesAndBuffers(exchangeLocations, exchangesComplete, outputBuffers, outputComplete);
            }
        }
    }

    private synchronized void waitForMoreExchangesAndBuffers(Multimap<PlanNodeId, URI> exchangeLocations,
            boolean exchangesComplete,
            Set<String> outputBuffers,
            boolean outputComplete)
    {
        while (!getState().isDone() ) {
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
                // todo this adds latency: replace this wait with an event listener
                TimeUnit.SECONDS.timedWait(this, 1);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
    }

    private boolean exchangesAreComplete()
    {
        for (StageExecutionNode subStage : subStages.values()) {
            switch (subStage.getState()) {
                case PLANNED:
                case SCHEDULING:
                    return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    public void doUpdateState()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not doUpdateState while holding a lock on this");

        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
            synchronized (this) {
                // wake up worker thread waiting for state changes
                this.notifyAll();

                StageState currentState = stageState.get();
                if (currentState.isDone()) {
                    return;
                }

                List<StageState> subStageStates = ImmutableList.copyOf(transform(transform(subStages.values(), stageInfoGetter()), stageStateGetter()));
                if (any(subStageStates, equalTo(StageState.FAILED))) {
                    stageState.set(StageState.FAILED);
                }
                else {
                    List<TaskState> taskStates = ImmutableList.copyOf(transform(transform(tasks.values(), taskInfoGetter()), taskStateGetter()));
                    if (any(taskStates, equalTo(TaskState.FAILED))) {
                        stageState.set(StageState.FAILED);
                    }
                    else if (currentState != StageState.PLANNED && currentState != StageState.SCHEDULING) {
                        // all tasks are now scheduled, so we can check the finished state
                        if (all(taskStates, TaskState.inDoneState())) {
                            stageState.set(StageState.FINISHED);
                        }
                        else if (any(taskStates, equalTo(TaskState.RUNNING))) {
                            stageState.set(StageState.RUNNING);
                        }
                    }
                }
            }

            if (stageState.get().isDone()) {
                // finish tasks and stages
                cancelAll();
            }
        }
    }

    public void cancel()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not cancel while holding a lock on this");

        try (SetThreadName setThreadName = new SetThreadName("Stage-%s", stageId)){
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
    }

    private void cancelAll()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not cancel while holding a lock on this");

        // propagate update to tasks and stages
        for (RemoteTask task : tasks.values()) {
            task.cancel();
        }
        for (StageExecutionNode subStage : subStages.values()) {
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

    public static Function<StageExecutionNode, StageInfo> stageInfoGetter()
    {
        return new Function<StageExecutionNode, StageInfo>()
        {
            @Override
            public StageInfo apply(StageExecutionNode stage)
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
interface StageExecutionNode
{
    StageInfo getStageInfo();

    StageState getState();

    Future<?> scheduleStartTasks();

    void addOutputBuffer(String nodeIdentifier);

    void noMoreOutputBuffers();

    Iterable<? extends URI> getTaskLocations();

    void addStateChangeListener(StateChangeListener<StageInfo> stateChangeListener);

    void cancelStage(StageId stageId);

    void cancel();
}


