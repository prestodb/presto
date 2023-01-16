/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution.resourceGroups;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.execution.ManagedQueryExecution;
import com.facebook.presto.execution.SqlQueryExecution;
import com.facebook.presto.execution.resourceGroups.WeightedFairQueue.Usage;
import com.facebook.presto.server.QueryStateInfo;
import com.facebook.presto.server.ResourceGroupInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits;
import com.facebook.presto.spi.resourceGroups.ResourceGroupState;
import com.facebook.presto.spi.resourceGroups.SchedulingPolicy;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.facebook.presto.SystemSessionProperties.getQueryPriority;
import static com.facebook.presto.common.ErrorType.USER_ERROR;
import static com.facebook.presto.server.QueryStateInfo.createQueryStateInfo;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_RESOURCE_GROUP;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits.NO_LIMITS;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.CAN_QUEUE;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.CAN_RUN;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.FULL;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.FAIR;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.QUERY_PRIORITY;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED_FAIR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.math.LongMath.saturatedAdd;
import static com.google.common.math.LongMath.saturatedMultiply;
import static com.google.common.math.LongMath.saturatedSubtract;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import static java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * Resource groups form a tree, and all access to a group is guarded by the root of the tree.
 * Queries are submitted to leaf groups. Never to intermediate groups. Intermediate groups
 * aggregate resource consumption from their children, and may have their own limitations that
 * are enforced.
 */
@ThreadSafe
public class InternalResourceGroup
        implements ResourceGroup
{
    public static final int DEFAULT_WEIGHT = 1;

    private final InternalResourceGroup root;
    private final Optional<InternalResourceGroup> parent;
    private final ResourceGroupId id;
    private final BiConsumer<InternalResourceGroup, Boolean> jmxExportListener;
    private final Executor executor;
    private final boolean staticResourceGroup;
    private final Function<ResourceGroupId, Optional<ResourceGroupRuntimeInfo>> additionalRuntimeInfo;
    private final Predicate<InternalResourceGroup> shouldWaitForResourceManagerUpdate;
    private final ReentrantReadWriteLock lock;
    // Configuration
    // =============
    @GuardedBy("root")
    private long softMemoryLimitBytes = Long.MAX_VALUE;
    @GuardedBy("root")
    private int softConcurrencyLimit;
    @GuardedBy("root")
    private int hardConcurrencyLimit;
    @GuardedBy("root")
    private int maxQueuedQueries;
    @GuardedBy("root")
    private long softCpuLimitMillis = Long.MAX_VALUE;
    @GuardedBy("root")
    private long hardCpuLimitMillis = Long.MAX_VALUE;
    @GuardedBy("root")
    private long cpuQuotaGenerationMillisPerSecond = Long.MAX_VALUE;
    @GuardedBy("root")
    private int schedulingWeight = DEFAULT_WEIGHT;
    @GuardedBy("root")
    private SchedulingPolicy schedulingPolicy = FAIR;
    @GuardedBy("root")
    private boolean jmxExport;
    @GuardedBy("root")
    private ResourceGroupQueryLimits perQueryLimits = NO_LIMITS;

    // Live data structures
    // ====================
    @GuardedBy("root")
    private final Map<String, InternalResourceGroup> subGroups = new HashMap<>();
    // Sub groups with queued queries, that have capacity to run them
    // That is, they must return true when internalStartNext() is called on them
    @GuardedBy("root")
    private Queue<InternalResourceGroup> eligibleSubGroups = new FifoQueue<>();
    // Sub groups whose memory usage may be out of date. Most likely because they have a running query.
    @GuardedBy("root")
    private final Set<InternalResourceGroup> dirtySubGroups = new HashSet<>();
    @GuardedBy("root")
    private TieredQueue<ManagedQueryExecution> queuedQueries = new TieredQueue<>(FifoQueue::new);
    @GuardedBy("root")
    private final Set<ManagedQueryExecution> runningQueries = new HashSet<>();
    @GuardedBy("root")
    private int descendantRunningQueries;
    @GuardedBy("root")
    private int descendantQueuedQueries;
    // Memory usage is cached because it changes very rapidly while queries are running, and would be expensive to track continuously
    @GuardedBy("root")
    private long cachedMemoryUsageBytes;
    @GuardedBy("root")
    private long cpuUsageMillis;
    @GuardedBy("root")
    private long lastStartMillis;
    @GuardedBy("root")
    private final CounterStat timeBetweenStartsSec = new CounterStat();

    @GuardedBy("root")
    private AtomicLong lastRunningQueryStartTime = new AtomicLong(currentTimeMillis());
    @GuardedBy("root")
    private AtomicBoolean isDirty = new AtomicBoolean();

    protected InternalResourceGroup(
            Optional<InternalResourceGroup> parent,
            String name,
            BiConsumer<InternalResourceGroup, Boolean> jmxExportListener,
            Executor executor,
            boolean staticResourceGroup,
            Function<ResourceGroupId, Optional<ResourceGroupRuntimeInfo>> additionalRuntimeInfo,
            Predicate<InternalResourceGroup> shouldWaitForResourceManagerUpdate)
    {
        this.parent = requireNonNull(parent, "parent is null");
        this.jmxExportListener = requireNonNull(jmxExportListener, "jmxExportListener is null");
        this.executor = requireNonNull(executor, "executor is null");
        requireNonNull(name, "name is null");
        if (parent.isPresent()) {
            id = new ResourceGroupId(parent.get().id, name);
            root = parent.get().root;
            this.lock = parent.get().lock;
        }
        else {
            id = new ResourceGroupId(name);
            root = this;
            this.lock = new ReentrantReadWriteLock();
        }
        this.staticResourceGroup = staticResourceGroup;
        this.additionalRuntimeInfo = requireNonNull(additionalRuntimeInfo, "additionalRuntimeInfo is null");
        this.shouldWaitForResourceManagerUpdate = requireNonNull(shouldWaitForResourceManagerUpdate, "shouldWaitForResourceManagerUpdate is null");
    }

    public ResourceGroupInfo getResourceGroupInfo(boolean includeQueryInfo, boolean summarizeSubgroups, boolean includeStaticSubgroupsOnly)
    {
        acquireReadLock();
        try {
            return new ResourceGroupInfo(
                    id,
                    getState(),
                    schedulingPolicy,
                    schedulingWeight,
                    DataSize.succinctBytes(softMemoryLimitBytes),
                    softConcurrencyLimit,
                    hardConcurrencyLimit,
                    maxQueuedQueries,
                    DataSize.succinctBytes(cachedMemoryUsageBytes),
                    getQueuedQueries(),
                    getRunningQueries(),
                    eligibleSubGroups.size(),
                    subGroups.values().stream()
                            .filter(group -> group.getRunningQueries() + group.getQueuedQueries() > 0)
                            .filter(group -> !includeStaticSubgroupsOnly || group.isStaticResourceGroup())
                            .map(group -> summarizeSubgroups ? group.getSummaryInfo() : group.getResourceGroupInfo(includeQueryInfo, false, includeStaticSubgroupsOnly))
                            .collect(toImmutableList()),
                    includeQueryInfo ? getAggregatedRunningQueriesInfo() : null);
        }
        finally {
            releaseReadLock();
        }
    }

    public ResourceGroupInfo getInfo()
    {
        acquireReadLock();
        try {
            return new ResourceGroupInfo(
                    id,
                    getState(),
                    schedulingPolicy,
                    schedulingWeight,
                    DataSize.succinctBytes(softMemoryLimitBytes),
                    softConcurrencyLimit,
                    hardConcurrencyLimit,
                    maxQueuedQueries,
                    DataSize.succinctBytes(cachedMemoryUsageBytes),
                    getQueuedQueries(),
                    getRunningQueries(),
                    eligibleSubGroups.size(),
                    subGroups.values().stream()
                            .filter(group -> group.getRunningQueries() + group.getQueuedQueries() > 0)
                            .map(InternalResourceGroup::getSummaryInfo)
                            .collect(toImmutableList()),
                    null);
        }
        finally {
            releaseReadLock();
        }
    }

    private ResourceGroupInfo getSummaryInfo()
    {
        acquireReadLock();
        try {
            return new ResourceGroupInfo(
                    id,
                    getState(),
                    schedulingPolicy,
                    schedulingWeight,
                    DataSize.succinctBytes(softMemoryLimitBytes),
                    softConcurrencyLimit,
                    hardConcurrencyLimit,
                    maxQueuedQueries,
                    DataSize.succinctBytes(cachedMemoryUsageBytes),
                    getQueuedQueries(),
                    getRunningQueries(),
                    eligibleSubGroups.size(),
                    null,
                    null);
        }
        finally {
            releaseReadLock();
        }
    }

    boolean isStaticResourceGroup()
    {
        return staticResourceGroup;
    }

    private ResourceGroupState getState()
    {
        acquireReadLock();
        try {
            if (canRunMore()) {
                return CAN_RUN;
            }
            else if (canQueueMore()) {
                return CAN_QUEUE;
            }
            else {
                return FULL;
            }
        }
        finally {
            releaseReadLock();
        }
    }

    private List<QueryStateInfo> getAggregatedRunningQueriesInfo()
    {
        acquireReadLock();
        try {
            if (subGroups.isEmpty()) {
                return runningQueries.stream()
                        .map(ManagedQueryExecution::getBasicQueryInfo)
                        .map(queryInfo -> createQueryStateInfo(queryInfo))
                        .collect(toImmutableList());
            }

            return subGroups.values().stream()
                    .map(InternalResourceGroup::getAggregatedRunningQueriesInfo)
                    .flatMap(List::stream)
                    .collect(toImmutableList());
        }
        finally {
            releaseReadLock();
        }
    }

    public List<ResourceGroupInfo> getPathToRoot()
    {
        acquireReadLock();
        try {
            ImmutableList.Builder<ResourceGroupInfo> builder = ImmutableList.builder();
            InternalResourceGroup group = this;
            while (group != null) {
                builder.add(group.getInfo());
                group = group.parent.orElse(null);
            }

            return builder.build();
        }
        finally {
            releaseReadLock();
        }
    }

    @Override
    public ResourceGroupId getId()
    {
        return id;
    }

    @Managed
    public int getRunningQueries()
    {
        acquireReadLock();
        try {
            return runningQueries.size() + descendantRunningQueries;
        }
        finally {
            releaseReadLock();
        }
    }

    private int getAggregatedRunningQueries()
    {
        synchronized (root) {
            int aggregatedRunningQueries = runningQueries.size() + descendantRunningQueries;
            Optional<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo = getAdditionalRuntimeInfo();
            if (resourceGroupRuntimeInfo.isPresent()) {
                aggregatedRunningQueries += resourceGroupRuntimeInfo.get().getRunningQueries() + resourceGroupRuntimeInfo.get().getDescendantRunningQueries();
            }
            return aggregatedRunningQueries;
        }
    }

    @Managed
    public int getQueuedQueries()
    {
        acquireReadLock();
        try {
            return queuedQueries.size() + descendantQueuedQueries;
        }
        finally {
            releaseReadLock();
        }
    }

    @Managed
    public int getWaitingQueuedQueries()
    {
        acquireReadLock();
        try {
            // For leaf group, when no queries can run, all queued queries are waiting for resources on this resource group.
            if (subGroups.isEmpty()) {
                return queuedQueries.size();
            }

            // For internal groups, when no queries can run, only queries that could run on its subgroups are waiting for resources on this group.
            int waitingQueuedQueries = 0;
            for (InternalResourceGroup subGroup : subGroups.values()) {
                if (subGroup.canRunMore()) {
                    waitingQueuedQueries += min(subGroup.getQueuedQueries(), subGroup.getHardConcurrencyLimit() - subGroup.getRunningQueries());
                }
            }

            return waitingQueuedQueries;
        }
        finally {
            releaseReadLock();
        }
    }

    @Override
    public DataSize getSoftMemoryLimit()
    {
        acquireReadLock();
        try {
            return new DataSize(softMemoryLimitBytes, BYTE);
        }
        finally {
            releaseReadLock();
        }
    }

    @Override
    public void setSoftMemoryLimit(DataSize limit)
    {
        acquireWriteLock();
        try {
            boolean oldCanRun = canRunMore();
            this.softMemoryLimitBytes = limit.toBytes();
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
        finally {
            releaseWriteLock();
        }
    }

    @Override
    public Duration getSoftCpuLimit()
    {
        acquireReadLock();
        try {
            return new Duration(softCpuLimitMillis, MILLISECONDS);
        }
        finally {
            releaseReadLock();
        }
    }

    @Override
    public void setSoftCpuLimit(Duration limit)
    {
        acquireWriteLock();
        try {
            if (limit.toMillis() > hardCpuLimitMillis) {
                setHardCpuLimit(limit);
            }
            boolean oldCanRun = canRunMore();
            this.softCpuLimitMillis = limit.toMillis();
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
        finally {
            releaseWriteLock();
        }
    }

    @Override
    public Duration getHardCpuLimit()
    {
        acquireReadLock();
        try {
            return new Duration(hardCpuLimitMillis, MILLISECONDS);
        }
        finally {
            releaseReadLock();
        }
    }

    @Override
    public void setHardCpuLimit(Duration limit)
    {
        acquireWriteLock();
        try {
            if (limit.toMillis() < softCpuLimitMillis) {
                setSoftCpuLimit(limit);
            }
            boolean oldCanRun = canRunMore();
            this.hardCpuLimitMillis = limit.toMillis();
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
        finally {
            releaseWriteLock();
        }
    }

    @Override
    public long getCpuQuotaGenerationMillisPerSecond()
    {
        acquireReadLock();
        try {
            return cpuQuotaGenerationMillisPerSecond;
        }
        finally {
            releaseReadLock();
        }
    }

    @Override
    public void setCpuQuotaGenerationMillisPerSecond(long rate)
    {
        checkArgument(rate > 0, "Cpu quota generation must be positive");
        acquireWriteLock();
        try {
            cpuQuotaGenerationMillisPerSecond = rate;
        }
        finally {
            releaseWriteLock();
        }
    }

    @Override
    public int getSoftConcurrencyLimit()
    {
        acquireReadLock();
        try {
            return softConcurrencyLimit;
        }
        finally {
            releaseReadLock();
        }
    }

    @Override
    public void setSoftConcurrencyLimit(int softConcurrencyLimit)
    {
        checkArgument(softConcurrencyLimit >= 0, "softConcurrencyLimit is negative");
        acquireWriteLock();
        try {
            boolean oldCanRun = canRunMore();
            this.softConcurrencyLimit = softConcurrencyLimit;
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
        finally {
            releaseWriteLock();
        }
    }

    @Managed
    @Override
    public int getHardConcurrencyLimit()
    {
        acquireReadLock();
        try {
            return hardConcurrencyLimit;
        }
        finally {
            releaseReadLock();
        }
    }

    @Managed
    @Override
    public void setHardConcurrencyLimit(int hardConcurrencyLimit)
    {
        checkArgument(hardConcurrencyLimit >= 0, "hardConcurrencyLimit is negative");
        acquireWriteLock();
        try {
            boolean oldCanRun = canRunMore();
            this.hardConcurrencyLimit = hardConcurrencyLimit;
            if (canRunMore() != oldCanRun) {
                updateEligibility();
            }
        }
        finally {
            releaseWriteLock();
        }
    }

    @Managed
    @Override
    public int getMaxQueuedQueries()
    {
        acquireReadLock();
        try {
            return maxQueuedQueries;
        }
        finally {
            releaseReadLock();
        }
    }

    @Managed
    @Override
    public void setMaxQueuedQueries(int maxQueuedQueries)
    {
        checkArgument(maxQueuedQueries >= 0, "maxQueuedQueries is negative");
        acquireWriteLock();
        try {
            this.maxQueuedQueries = maxQueuedQueries;
        }
        finally {
            releaseWriteLock();
        }
    }

    @Managed
    @Nested
    public CounterStat getTimeBetweenStartsSec()
    {
        return timeBetweenStartsSec;
    }

    @Override
    public int getSchedulingWeight()
    {
        acquireReadLock();
        try {
            return schedulingWeight;
        }
        finally {
            releaseReadLock();
        }
    }

    @Override
    public void setSchedulingWeight(int weight)
    {
        checkArgument(weight > 0, "weight must be positive");
        acquireWriteLock();
        try {
            this.schedulingWeight = weight;
            if (parent.isPresent() && parent.get().schedulingPolicy == WEIGHTED && parent.get().eligibleSubGroups.contains(this)) {
                parent.get().addOrUpdateSubGroup(this);
            }
        }
        finally {
            releaseWriteLock();
        }
    }

    @Override
    public SchedulingPolicy getSchedulingPolicy()
    {
        acquireReadLock();
        try {
            return schedulingPolicy;
        }
        finally {
            releaseReadLock();
        }
    }

    @Override
    public void setSchedulingPolicy(SchedulingPolicy policy)
    {
        acquireWriteLock();
        try {
            if (policy == schedulingPolicy) {
                return;
            }

            if (parent.isPresent() && parent.get().schedulingPolicy == QUERY_PRIORITY) {
                checkArgument(policy == QUERY_PRIORITY, "Parent of %s uses query priority scheduling, so %s must also", id, id);
            }

            // Switch to the appropriate queue implementation to implement the desired policy
            Queue<InternalResourceGroup> queue;
            TieredQueue<ManagedQueryExecution> queryQueue;
            switch (policy) {
                case FAIR:
                    queue = new FifoQueue<>();
                    queryQueue = new TieredQueue<>(FifoQueue::new);
                    break;
                case WEIGHTED:
                    queue = new StochasticPriorityQueue<>();
                    queryQueue = new TieredQueue<>(StochasticPriorityQueue::new);
                    break;
                case WEIGHTED_FAIR:
                    queue = new WeightedFairQueue<>();
                    queryQueue = new TieredQueue<>(IndexedPriorityQueue::new);
                    break;
                case QUERY_PRIORITY:
                    // Sub groups must use query priority to ensure ordering
                    for (InternalResourceGroup group : subGroups.values()) {
                        group.setSchedulingPolicy(QUERY_PRIORITY);
                    }
                    queue = new IndexedPriorityQueue<>();
                    queryQueue = new TieredQueue<>(IndexedPriorityQueue::new);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported scheduling policy: " + policy);
            }
            schedulingPolicy = policy;
            while (!eligibleSubGroups.isEmpty()) {
                InternalResourceGroup group = eligibleSubGroups.poll();
                addOrUpdateSubGroup(queue, group);
            }
            eligibleSubGroups = queue;
            while (!queuedQueries.isEmpty()) {
                ManagedQueryExecution query = queuedQueries.poll();
                queryQueue.addOrUpdate(query, getQueryPriority(query.getSession()));
            }
            queuedQueries = queryQueue;
        }
        finally {
            releaseWriteLock();
        }
    }

    @Override
    public boolean getJmxExport()
    {
        acquireReadLock();
        try {
            return jmxExport;
        }
        finally {
            releaseReadLock();
        }
    }

    @Override
    public void setJmxExport(boolean export)
    {
        acquireWriteLock();
        try {
            jmxExport = export;
        }
        finally {
            releaseWriteLock();
        }
        jmxExportListener.accept(this, export);
    }

    @Override
    public void setPerQueryLimits(ResourceGroupQueryLimits perQueryLimits)
    {
        acquireWriteLock();
        try {
            this.perQueryLimits = perQueryLimits;
        }
        finally {
            releaseWriteLock();
        }
    }

    @Override
    public ResourceGroupQueryLimits getPerQueryLimits()
    {
        acquireReadLock();
        try {
            return perQueryLimits;
        }
        finally {
            releaseReadLock();
        }
    }

    public InternalResourceGroup getOrCreateSubGroup(String name, boolean staticSegment)
    {
        requireNonNull(name, "name is null");
        acquireWriteLock();
        try {
            checkArgument(runningQueries.isEmpty() && queuedQueries.isEmpty(), "Cannot add sub group to %s while queries are running", id);
            if (subGroups.containsKey(name)) {
                return subGroups.get(name);
            }
            // parent segments size equals to subgroup segment index
            int subGroupSegmentIndex = id.getSegments().size();
            InternalResourceGroup subGroup = new InternalResourceGroup(
                    Optional.of(this),
                    name,
                    jmxExportListener,
                    executor,
                    staticResourceGroup && staticSegment,
                    additionalRuntimeInfo,
                    shouldWaitForResourceManagerUpdate);
            // Sub group must use query priority to ensure ordering
            if (schedulingPolicy == QUERY_PRIORITY) {
                subGroup.setSchedulingPolicy(QUERY_PRIORITY);
            }
            subGroups.put(name, subGroup);
            return subGroup;
        }
        finally {
            releaseWriteLock();
        }
    }

    public int getRunningTaskCount()
    {
        if (subGroups().isEmpty()) {
            return runningQueries.stream()
                    .filter(SqlQueryExecution.class::isInstance)
                    .mapToInt(query -> ((SqlQueryExecution) query).getRunningTaskCount())
                    .sum();
        }

        int taskCount = 0;
        for (InternalResourceGroup subGroup : subGroups()) {
            taskCount += subGroup.getRunningTaskCount();
        }

        return taskCount;
    }

    protected void setDirty()
    {
        acquireWriteLock();
        try {
            this.isDirty.set(true);
            dirtySubGroups.addAll(subGroups());
            subGroups().forEach(InternalResourceGroup::setDirty);
        }
        finally {
            releaseWriteLock();
        }
    }

    public void run(ManagedQueryExecution query)
    {
        acquireWriteLock();
        try {
            if (!subGroups.isEmpty()) {
                throw new PrestoException(INVALID_RESOURCE_GROUP, format("Cannot add queries to %s. It is not a leaf group.", id));
            }
            // Check all ancestors for capacity
            InternalResourceGroup group = this;
            boolean canQueue = true;
            boolean canRun = true;
            while (true) {
                canQueue &= group.canQueueMore();
                canRun &= group.canRunMore();
                if (!group.parent.isPresent()) {
                    break;
                }
                group = group.parent.get();
            }
            if (!canQueue && !canRun) {
                query.fail(new QueryQueueFullException(id));
                return;
            }
            query.setResourceGroupQueryLimits(perQueryLimits);
            if (canRun && queuedQueries.isEmpty()) {
                startInBackground(query);
            }
            else {
                enqueueQuery(query);
            }
            query.addStateChangeListener(state -> {
                if (state.isDone()) {
                    queryFinished(query);
                }
            });
        }
        finally {
            releaseWriteLock();
        }
    }

    private void enqueueQuery(ManagedQueryExecution query)
    {
        checkState(getWriteLock().isHeldByCurrentThread(), "Must hold lock to enqueue a query");
        acquireWriteLock();
        try {
            int priority = getQueryPriority(query.getSession());
            if (query.isRetry()) {
                queuedQueries.prioritize(query, priority);
            }
            else {
                queuedQueries.addOrUpdate(query, priority);
            }
            InternalResourceGroup group = this;
            while (group.parent.isPresent()) {
                group.parent.get().descendantQueuedQueries++;
                group = group.parent.get();
            }
            updateEligibility();
        }
        finally {
            releaseWriteLock();
        }
    }

    // This method must be called whenever the group's eligibility to run more queries may have changed.
    private void updateEligibility()
    {
        checkState(getWriteLock().isHeldByCurrentThread(), "Must hold lock to update eligibility");
        acquireWriteLock();
        try {
            if (!parent.isPresent()) {
                return;
            }
            if (isEligibleToStartNext()) {
                parent.get().addOrUpdateSubGroup(this);
            }
            else {
                if (queuedQueries.isEmpty() && eligibleSubGroups.isEmpty()) {
                    parent.get().eligibleSubGroups.remove(this);
                    lastStartMillis = 0;
                }
            }
            parent.get().updateEligibility();
        }
        finally {
            releaseWriteLock();
        }
    }

    private void startInBackground(ManagedQueryExecution query)
    {
        checkState(getWriteLock().isHeldByCurrentThread(), "Must hold lock to start a query");
        acquireWriteLock();
        try {
            runningQueries.add(query);
            InternalResourceGroup group = this;
            while (group.parent.isPresent()) {
                group.parent.get().descendantRunningQueries++;
                group.parent.get().dirtySubGroups.add(group);
                group = group.parent.get();
            }
            updateEligibility();
            executor.execute(query::startWaitingForResources);
            group = this;
            long lastRunningQueryStartTimeMillis = currentTimeMillis();
            lastRunningQueryStartTime.set(lastRunningQueryStartTimeMillis);
            while (group.parent.isPresent()) {
                group.parent.get().lastRunningQueryStartTime.set(lastRunningQueryStartTimeMillis);
                group = group.parent.get();
            }
        }
        finally {
            releaseWriteLock();
        }
    }

    private void queryFinished(ManagedQueryExecution query)
    {
        acquireWriteLock();
        try {
            if (!runningQueries.contains(query) && !queuedQueries.contains(query)) {
                // Query has already been cleaned up
                return;
            }
            // Only count the CPU time if the query succeeded, or the failure was the fault of the user
            if (!query.getErrorCode().isPresent() || query.getErrorCode().get().getType() == USER_ERROR) {
                InternalResourceGroup group = this;
                while (group != null) {
                    group.cpuUsageMillis = saturatedAdd(group.cpuUsageMillis, query.getTotalCpuTime().toMillis());
                    group = group.parent.orElse(null);
                }
            }
            if (runningQueries.contains(query)) {
                runningQueries.remove(query);
                InternalResourceGroup group = this;
                while (group.parent.isPresent()) {
                    group.parent.get().descendantRunningQueries--;
                    group = group.parent.get();
                }
            }
            else {
                queuedQueries.remove(query);
                InternalResourceGroup group = this;
                while (group.parent.isPresent()) {
                    group.parent.get().descendantQueuedQueries--;
                    group = group.parent.get();
                }
            }
            updateEligibility();
        }
        finally {
            releaseWriteLock();
        }
    }

    // Memory usage stats are expensive to maintain, so this method must be called periodically to update them
    protected void internalRefreshStats()
    {
        checkState(getWriteLock().isHeldByCurrentThread(), "Must hold lock to refresh stats");
        acquireWriteLock();
        try {
            if (subGroups.isEmpty()) {
                cachedMemoryUsageBytes = 0;
                for (ManagedQueryExecution query : runningQueries) {
                    cachedMemoryUsageBytes += query.getUserMemoryReservation().toBytes();
                }
                Optional<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo = getAdditionalRuntimeInfo();
                resourceGroupRuntimeInfo.ifPresent(groupRuntimeInfo -> cachedMemoryUsageBytes += groupRuntimeInfo.getMemoryUsageBytes());
            }
            else {
                for (Iterator<InternalResourceGroup> iterator = dirtySubGroups.iterator(); iterator.hasNext(); ) {
                    InternalResourceGroup subGroup = iterator.next();
                    long oldMemoryUsageBytes = subGroup.cachedMemoryUsageBytes;
                    cachedMemoryUsageBytes -= oldMemoryUsageBytes;
                    subGroup.internalRefreshStats();
                    cachedMemoryUsageBytes += subGroup.cachedMemoryUsageBytes;
                    if (!subGroup.isDirty()) {
                        iterator.remove();
                    }
                    if (oldMemoryUsageBytes != subGroup.cachedMemoryUsageBytes || subGroup.isDirty.get()) {
                        subGroup.updateEligibility();
                        subGroup.isDirty.set(false);
                    }
                }
            }
        }
        finally {
            releaseWriteLock();
        }
    }

    protected void internalGenerateCpuQuota(long elapsedSeconds)
    {
        checkState(getWriteLock().isHeldByCurrentThread(), "Must hold lock to generate cpu quota");
        acquireWriteLock();
        try {
            long newQuota = saturatedMultiply(elapsedSeconds, cpuQuotaGenerationMillisPerSecond);
            cpuUsageMillis = saturatedSubtract(cpuUsageMillis, newQuota);
            if (cpuUsageMillis < 0 || cpuUsageMillis == Long.MAX_VALUE) {
                cpuUsageMillis = 0;
            }
            for (InternalResourceGroup group : subGroups.values()) {
                group.internalGenerateCpuQuota(elapsedSeconds);
            }
        }
        finally {
            releaseWriteLock();
        }
    }

    protected boolean internalStartNext()
    {
        checkState(getWriteLock().isHeldByCurrentThread(), "Must hold lock to find next query");
        acquireWriteLock();
        try {
            if (!canRunMore()) {
                return false;
            }

            ManagedQueryExecution query = queuedQueries.poll();
            if (query != null) {
                startInBackground(query);
                return true;
            }

            // Remove even if the sub group still has queued queries, so that it goes to the back of the queue
            InternalResourceGroup subGroup = eligibleSubGroups.poll();
            if (subGroup == null) {
                return false;
            }
            boolean started = subGroup.internalStartNext();
            if (started) {
                long currentTime = System.currentTimeMillis();
                if (lastStartMillis != 0) {
                    timeBetweenStartsSec.update(Math.max(0, (currentTime - lastStartMillis) / 1000));
                }
                lastStartMillis = currentTime;

                descendantQueuedQueries--;

                // Don't call updateEligibility here, as we're in a recursive call, and don't want to repeatedly update our ancestors.
                if (subGroup.isEligibleToStartNext()) {
                    addOrUpdateSubGroup(subGroup);
                }
            }
            else {
                //If subGroup not able to start the query, we should add it back.
                addOrUpdateSubGroup(subGroup);
            }

            return started;
        }
        finally {
            releaseWriteLock();
        }
    }

    private void addOrUpdateSubGroup(Queue<InternalResourceGroup> queue, InternalResourceGroup group)
    {
        if (schedulingPolicy == WEIGHTED_FAIR) {
            ((WeightedFairQueue<InternalResourceGroup>) queue).addOrUpdate(group, new Usage(group.getSchedulingWeight(), group.getAggregatedRunningQueries()));
        }
        else {
            ((UpdateablePriorityQueue<InternalResourceGroup>) queue).addOrUpdate(group, getSubGroupSchedulingPriority(schedulingPolicy, group));
        }
    }

    private void addOrUpdateSubGroup(InternalResourceGroup group)
    {
        addOrUpdateSubGroup(eligibleSubGroups, group);
    }

    private static long getSubGroupSchedulingPriority(SchedulingPolicy policy, InternalResourceGroup group)
    {
        if (policy == QUERY_PRIORITY) {
            return group.getHighestQueryPriority();
        }
        else {
            return group.computeSchedulingWeight();
        }
    }

    private long computeSchedulingWeight()
    {
        if (getAggregatedRunningQueries() >= softConcurrencyLimit) {
            return schedulingWeight;
        }

        return (long) Integer.MAX_VALUE * schedulingWeight;
    }

    private boolean isDirty()
    {
        acquireReadLock();
        try {
            return runningQueries.size() + descendantRunningQueries > 0 || isDirty.get();
        }
        finally {
            releaseReadLock();
        }
    }

    private boolean isEligibleToStartNext()
    {
        acquireReadLock();
        try {
            if (!canRunMore()) {
                return false;
            }
            return !queuedQueries.isEmpty() || !eligibleSubGroups.isEmpty();
        }
        finally {
            releaseReadLock();
        }
    }

    private int getHighestQueryPriority()
    {
        acquireReadLock();
        try {
            checkState(queuedQueries.getLowPriorityQueue() instanceof IndexedPriorityQueue, "Queued queries not ordered");
            if (queuedQueries.isEmpty()) {
                return 0;
            }
            return getQueryPriority(queuedQueries.peek().getSession());
        }
        finally {
            releaseReadLock();
        }
    }

    private boolean canQueueMore()
    {
        acquireReadLock();
        try {
            Optional<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo = getAdditionalRuntimeInfo();
            if (resourceGroupRuntimeInfo.isPresent()) {
                return descendantQueuedQueries + queuedQueries.size() + resourceGroupRuntimeInfo.get().getQueuedQueries() + resourceGroupRuntimeInfo.get().getDescendantQueuedQueries() < maxQueuedQueries;
            }
            return descendantQueuedQueries + queuedQueries.size() < maxQueuedQueries;
        }
        finally {
            releaseReadLock();
        }
    }

    private boolean canRunMore()
    {
        acquireReadLock();
        try {
            if (cpuUsageMillis >= hardCpuLimitMillis) {
                return false;
            }

            if (shouldWaitForResourceManagerUpdate()) {
                return false;
            }

            if (((RootInternalResourceGroup) root).isTaskLimitExceeded()) {
                return false;
            }

            int hardConcurrencyLimit = getHardConcurrencyLimitBasedOnCpuUsage();

            int totalRunningQueries = runningQueries.size() + descendantRunningQueries;

            Optional<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfo = getAdditionalRuntimeInfo();
            if (resourceGroupRuntimeInfo.isPresent()) {
                totalRunningQueries += resourceGroupRuntimeInfo.get().getRunningQueries() + resourceGroupRuntimeInfo.get().getDescendantRunningQueries();
            }

            return totalRunningQueries < hardConcurrencyLimit && cachedMemoryUsageBytes <= softMemoryLimitBytes;
        }
        finally {
            releaseReadLock();
        }
    }

    protected int getHardConcurrencyLimitBasedOnCpuUsage()
    {
        acquireReadLock();
        try {
            int hardConcurrencyLimit = this.hardConcurrencyLimit;
            if (cpuUsageMillis >= softCpuLimitMillis) {
                // TODO: Consider whether cpu limit math should be performed on softConcurrency or hardConcurrency
                // Linear penalty between soft and hard limit
                double penalty = (cpuUsageMillis - softCpuLimitMillis) / (double) (hardCpuLimitMillis - softCpuLimitMillis);
                hardConcurrencyLimit = (int) Math.floor(hardConcurrencyLimit * (1 - penalty));
                // Always penalize by at least one
                hardConcurrencyLimit = min(this.hardConcurrencyLimit - 1, hardConcurrencyLimit);
                // Always allow at least one running query
                hardConcurrencyLimit = Math.max(1, hardConcurrencyLimit);
            }

            return hardConcurrencyLimit;
        }
        finally {
            releaseReadLock();
        }
    }

    public Collection<InternalResourceGroup> subGroups()
    {
        acquireReadLock();
        try {
            return subGroups.values();
        }
        finally {
            releaseReadLock();
        }
    }

    protected long getLastRunningQueryStartTime()
    {
        acquireReadLock();
        try {
            return lastRunningQueryStartTime.get();
        }
        finally {
            releaseReadLock();
        }
    }

    private boolean shouldWaitForResourceManagerUpdate()
    {
        acquireReadLock();
        try {
            return shouldWaitForResourceManagerUpdate.test(this);
        }
        finally {
            releaseReadLock();
        }
    }

    private Optional<ResourceGroupRuntimeInfo> getAdditionalRuntimeInfo()
    {
        acquireReadLock();
        try {
            return additionalRuntimeInfo.apply(getId());
        }
        finally {
            releaseReadLock();
        }
    }

    protected void acquireReadLock()
    {
        getReadLock().lock();
    }

    private ReadLock getReadLock()
    {
        return root.lock.readLock();
    }

    protected void acquireWriteLock()
    {
        getWriteLock().lock();
    }

    private WriteLock getWriteLock()
    {
        return root.lock.writeLock();
    }

    protected void releaseReadLock()
    {
        getReadLock().unlock();
    }

    protected void releaseWriteLock()
    {
        getWriteLock().unlock();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InternalResourceGroup)) {
            return false;
        }
        InternalResourceGroup that = (InternalResourceGroup) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @ThreadSafe
    public static final class RootInternalResourceGroup
            extends InternalResourceGroup
    {
        private AtomicBoolean taskLimitExceeded = new AtomicBoolean();

        public RootInternalResourceGroup(
                String name,
                BiConsumer<InternalResourceGroup, Boolean> jmxExportListener,
                Executor executor,
                Function<ResourceGroupId, Optional<ResourceGroupRuntimeInfo>> additionalRuntimeInfo,
                Predicate<InternalResourceGroup> shouldWaitForResourceManagerUpdate)
        {
            super(Optional.empty(),
                    name,
                    jmxExportListener,
                    executor,
                    true,
                    additionalRuntimeInfo,
                    shouldWaitForResourceManagerUpdate);
        }

        public void processQueuedQueries()
        {
            acquireWriteLock();
            try {
                internalRefreshStats();

                while (internalStartNext()) {
                    // start all the queries we can
                }
            }
            finally {
                releaseWriteLock();
            }
        }

        public void generateCpuQuota(long elapsedSeconds)
        {
            acquireWriteLock();
            try {
                if (elapsedSeconds > 0) {
                    internalGenerateCpuQuota(elapsedSeconds);
                }
            }
            finally {
                releaseWriteLock();
            }
        }

        public void setTaskLimitExceeded(boolean exceeded)
        {
            taskLimitExceeded.set(exceeded);
        }

        private boolean isTaskLimitExceeded()
        {
            return taskLimitExceeded.get();
        }
    }
}
