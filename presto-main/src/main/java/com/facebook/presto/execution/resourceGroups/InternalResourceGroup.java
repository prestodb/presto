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

import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.resourceGroups.WeightedFairQueue.Usage;
import com.facebook.presto.server.QueryStateInfo;
import com.facebook.presto.server.ResourceGroupStateInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupInfo;
import com.facebook.presto.spi.resourceGroups.ResourceGroupState;
import com.facebook.presto.spi.resourceGroups.SchedulingPolicy;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;

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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

import static com.facebook.presto.SystemSessionProperties.getQueryPriority;
import static com.facebook.presto.server.QueryStateInfo.createQueryStateInfo;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
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
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

    protected final ReentrantReadWriteLock lock;
    private final Optional<InternalResourceGroup> parent;
    private final ResourceGroupId id;
    private final BiConsumer<InternalResourceGroup, Boolean> jmxExportListener;
    private final Executor executor;

    private final Map<String, InternalResourceGroup> subGroups = new HashMap<>();
    // Sub groups with queued queries, that have capacity to run them
    // That is, they must return true when internalStartNext() is called on them
    private Queue<InternalResourceGroup> eligibleSubGroups = new FifoQueue<>();
    // Sub groups whose memory usage may be out of date. Most likely because they have a running query.
    private final Set<InternalResourceGroup> dirtySubGroups = new HashSet<>();
    private long softMemoryLimitBytes;
    private int softConcurrencyLimit;
    private int hardConcurrencyLimit;
    private int maxQueuedQueries;
    private long softCpuLimitMillis = Long.MAX_VALUE;
    private long hardCpuLimitMillis = Long.MAX_VALUE;
    private long cpuUsageMillis;
    private long cpuQuotaGenerationMillisPerSecond = Long.MAX_VALUE;
    private int descendantRunningQueries;
    private int descendantQueuedQueries;
    // Memory usage is cached because it changes very rapidly while queries are running, and would be expensive to track continuously
    private long cachedMemoryUsageBytes;
    private int schedulingWeight = DEFAULT_WEIGHT;
    private UpdateablePriorityQueue<QueryExecution> queuedQueries = new FifoQueue<>();
    private final Set<QueryExecution> runningQueries = new HashSet<>();
    private SchedulingPolicy schedulingPolicy = FAIR;
    private boolean jmxExport;
    private Duration queuedTimeLimit = new Duration(Long.MAX_VALUE, MILLISECONDS);
    private Duration runningTimeLimit = new Duration(Long.MAX_VALUE, MILLISECONDS);

    protected InternalResourceGroup(Optional<InternalResourceGroup> parent, String name, BiConsumer<InternalResourceGroup, Boolean> jmxExportListener, Executor executor)
    {
        this.parent = requireNonNull(parent, "parent is null");
        this.jmxExportListener = requireNonNull(jmxExportListener, "jmxExportListener is null");
        this.executor = requireNonNull(executor, "executor is null");
        requireNonNull(name, "name is null");
        if (parent.isPresent()) {
            id = new ResourceGroupId(parent.get().id, name);
            lock = parent.get().lock;
        }
        else {
            id = new ResourceGroupId(name);
            lock = new ReentrantReadWriteLock();
        }
    }

    public ResourceGroupInfo getInfo()
    {
        lock.readLock().lock();
        try {
            checkState(!subGroups.isEmpty() || (descendantRunningQueries == 0 && descendantQueuedQueries == 0), "Leaf resource group has descendant queries.");

            List<ResourceGroupInfo> infos = subGroups.values().stream()
                    .map(InternalResourceGroup::getInfo)
                    .collect(toImmutableList());

            return new ResourceGroupInfo(
                    id,
                    DataSize.succinctBytes(softMemoryLimitBytes),
                    hardConcurrencyLimit,
                    softConcurrencyLimit,
                    runningTimeLimit,
                    maxQueuedQueries,
                    queuedTimeLimit,
                    getState(),
                    eligibleSubGroups.size(),
                    DataSize.succinctBytes(cachedMemoryUsageBytes),
                    runningQueries.size() + descendantRunningQueries,
                    queuedQueries.size() + descendantQueuedQueries,
                    infos);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public ResourceGroupStateInfo getStateInfo()
    {
        lock.readLock().lock();
        try {
            return new ResourceGroupStateInfo(
                    id,
                    getState(),
                    DataSize.succinctBytes(softMemoryLimitBytes),
                    DataSize.succinctBytes(cachedMemoryUsageBytes),
                    softConcurrencyLimit,
                    hardConcurrencyLimit,
                    maxQueuedQueries,
                    runningTimeLimit,
                    queuedTimeLimit,
                    getAggregatedRunningQueriesInfo(),
                    queuedQueries.size() + descendantQueuedQueries,
                    subGroups.values().stream()
                            .map(subGroup -> new ResourceGroupInfo(
                                    subGroup.getId(),
                                    DataSize.succinctBytes(subGroup.softMemoryLimitBytes),
                                    subGroup.softConcurrencyLimit,
                                    subGroup.hardConcurrencyLimit,
                                    subGroup.runningTimeLimit,
                                    subGroup.maxQueuedQueries,
                                    subGroup.queuedTimeLimit,
                                    subGroup.getState(),
                                    subGroup.eligibleSubGroups.size(),
                                    DataSize.succinctBytes(subGroup.cachedMemoryUsageBytes),
                                    subGroup.runningQueries.size() + subGroup.descendantRunningQueries,
                                    subGroup.queuedQueries.size() + subGroup.descendantQueuedQueries))
                            .collect(toImmutableList()));
        }
        finally {
            lock.readLock().unlock();
        }
    }

    private ResourceGroupState getState()
    {
        lock.readLock().lock();
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
            lock.readLock().unlock();
        }
    }

    private List<QueryStateInfo> getAggregatedRunningQueriesInfo()
    {
        lock.readLock().lock();
        try {
            if (subGroups.isEmpty()) {
                return runningQueries.stream()
                        .map(QueryExecution::getQueryInfo)
                        .map(queryInfo -> createQueryStateInfo(queryInfo, Optional.of(id), Optional.empty()))
                        .collect(toImmutableList());
            }
            return subGroups.values().stream()
                    .map(InternalResourceGroup::getAggregatedRunningQueriesInfo)
                    .flatMap(List::stream)
                    .collect(toImmutableList());
        }
        finally {
            lock.readLock().unlock();
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
        lock.readLock().lock();
        try {
            return runningQueries.size() + descendantRunningQueries;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Managed
    public int getQueuedQueries()
    {
        lock.readLock().lock();
        try {
            return queuedQueries.size() + descendantQueuedQueries;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Managed
    public int getWaitingQueuedQueries()
    {
        lock.readLock().lock();
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
            lock.readLock().unlock();
        }
    }

    @Override
    public DataSize getSoftMemoryLimit()
    {
        lock.readLock().lock();
        try {
            return new DataSize(softMemoryLimitBytes, BYTE);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void setSoftMemoryLimit(DataSize limit)
    {
        lock.writeLock().lock();
        try {
            boolean oldCanRun = canRunMore();
            this.softMemoryLimitBytes = limit.toBytes();
            if (canRunMore() != oldCanRun) {
                updateEligiblility();
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Duration getSoftCpuLimit()
    {
        lock.readLock().lock();
        try {
            return new Duration(softCpuLimitMillis, MILLISECONDS);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void setSoftCpuLimit(Duration limit)
    {
        lock.writeLock().lock();
        try {
            if (limit.toMillis() > hardCpuLimitMillis) {
                setHardCpuLimit(limit);
            }
            boolean oldCanRun = canRunMore();
            this.softCpuLimitMillis = limit.toMillis();
            if (canRunMore() != oldCanRun) {
                updateEligiblility();
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Duration getHardCpuLimit()
    {
        lock.readLock().lock();
        try {
            return new Duration(hardCpuLimitMillis, MILLISECONDS);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void setHardCpuLimit(Duration limit)
    {
        lock.writeLock().lock();
        try {
            if (limit.toMillis() < softCpuLimitMillis) {
                setSoftCpuLimit(limit);
            }
            boolean oldCanRun = canRunMore();
            this.hardCpuLimitMillis = limit.toMillis();
            if (canRunMore() != oldCanRun) {
                updateEligiblility();
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long getCpuQuotaGenerationMillisPerSecond()
    {
        lock.readLock().lock();
        try {
            return cpuQuotaGenerationMillisPerSecond;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void setCpuQuotaGenerationMillisPerSecond(long rate)
    {
        checkArgument(rate > 0, "Cpu quota generation must be positive");
        lock.writeLock().lock();
        try {
            cpuQuotaGenerationMillisPerSecond = rate;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int getSoftConcurrencyLimit()
    {
        lock.readLock().lock();
        try {
            return softConcurrencyLimit;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void setSoftConcurrencyLimit(int softConcurrencyLimit)
    {
        checkArgument(softConcurrencyLimit >= 0, "softConcurrencyLimit is negative");
        lock.writeLock().lock();
        try {
            boolean oldCanRun = canRunMore();
            this.softConcurrencyLimit = softConcurrencyLimit;
            if (canRunMore() != oldCanRun) {
                updateEligiblility();
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Managed
    @Override
    public int getHardConcurrencyLimit()
    {
        lock.readLock().lock();
        try {
            return hardConcurrencyLimit;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Managed
    @Override
    public void setHardConcurrencyLimit(int hardConcurrencyLimit)
    {
        checkArgument(hardConcurrencyLimit >= 0, "hardConcurrencyLimit is negative");
        lock.writeLock().lock();
        try {
            boolean oldCanRun = canRunMore();
            this.hardConcurrencyLimit = hardConcurrencyLimit;
            if (canRunMore() != oldCanRun) {
                updateEligiblility();
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Managed
    @Override
    public int getMaxQueuedQueries()
    {
        lock.readLock().lock();
        try {
            return maxQueuedQueries;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Managed
    @Override
    public void setMaxQueuedQueries(int maxQueuedQueries)
    {
        checkArgument(maxQueuedQueries >= 0, "maxQueuedQueries is negative");
        lock.writeLock().lock();
        try {
            this.maxQueuedQueries = maxQueuedQueries;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int getSchedulingWeight()
    {
        lock.readLock().lock();
        try {
            return schedulingWeight;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void setSchedulingWeight(int weight)
    {
        checkArgument(weight > 0, "weight must be positive");
        lock.writeLock().lock();
        try {
            this.schedulingWeight = weight;
            if (parent.isPresent() && parent.get().schedulingPolicy == WEIGHTED && parent.get().eligibleSubGroups.contains(this)) {
                parent.get().addOrUpdateSubGroup(this);
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public SchedulingPolicy getSchedulingPolicy()
    {
        lock.readLock().lock();
        try {
            return schedulingPolicy;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void setSchedulingPolicy(SchedulingPolicy policy)
    {
        lock.writeLock().lock();
        try {
            if (policy == schedulingPolicy) {
                return;
            }

            if (parent.isPresent() && parent.get().schedulingPolicy == QUERY_PRIORITY) {
                checkArgument(policy == QUERY_PRIORITY, "Parent of %s uses query priority scheduling, so %s must also", id, id);
            }

            // Switch to the appropriate queue implementation to implement the desired policy
            Queue<InternalResourceGroup> queue;
            UpdateablePriorityQueue<QueryExecution> queryQueue;
            switch (policy) {
                case FAIR:
                    queue = new FifoQueue<>();
                    queryQueue = new FifoQueue<>();
                    break;
                case WEIGHTED:
                    queue = new StochasticPriorityQueue<>();
                    queryQueue = new StochasticPriorityQueue<>();
                    break;
                case WEIGHTED_FAIR:
                    queue = new WeightedFairQueue<>();
                    queryQueue = new IndexedPriorityQueue<>();
                    break;
                case QUERY_PRIORITY:
                    // Sub groups must use query priority to ensure ordering
                    for (InternalResourceGroup group : subGroups.values()) {
                        group.setSchedulingPolicy(QUERY_PRIORITY);
                    }
                    queue = new IndexedPriorityQueue<>();
                    queryQueue = new IndexedPriorityQueue<>();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported scheduling policy: " + policy);
            }
            while (!eligibleSubGroups.isEmpty()) {
                InternalResourceGroup group = eligibleSubGroups.poll();
                addOrUpdateSubGroup(group);
            }
            eligibleSubGroups = queue;
            while (!queuedQueries.isEmpty()) {
                QueryExecution query = queuedQueries.poll();
                queryQueue.addOrUpdate(query, getQueryPriority(query.getSession()));
            }
            queuedQueries = queryQueue;
            schedulingPolicy = policy;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean getJmxExport()
    {
        lock.readLock().lock();
        try {
            return jmxExport;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void setJmxExport(boolean export)
    {
        lock.writeLock().lock();
        try {
            jmxExport = export;
        }
        finally {
            lock.writeLock().unlock();
        }
        jmxExportListener.accept(this, export);
    }

    @Override
    public Duration getQueuedTimeLimit()
    {
        lock.readLock().lock();
        try {
            return queuedTimeLimit;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void setQueuedTimeLimit(Duration queuedTimeLimit)
    {
        lock.writeLock().lock();
        try {
            this.queuedTimeLimit = queuedTimeLimit;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Duration getRunningTimeLimit()
    {
        lock.readLock().lock();
        try {
            return runningTimeLimit;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void setRunningTimeLimit(Duration runningTimeLimit)
    {
        lock.writeLock().lock();
        try {
            this.runningTimeLimit = runningTimeLimit;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public InternalResourceGroup getOrCreateSubGroup(String name)
    {
        requireNonNull(name, "name is null");
        lock.writeLock().lock();
        try {
            checkArgument(runningQueries.isEmpty() && queuedQueries.isEmpty(), "Cannot add sub group to %s while queries are running", id);
            if (subGroups.containsKey(name)) {
                return subGroups.get(name);
            }
            InternalResourceGroup subGroup = new InternalResourceGroup(Optional.of(this), name, jmxExportListener, executor);
            // Sub group must use query priority to ensure ordering
            if (schedulingPolicy == QUERY_PRIORITY) {
                subGroup.setSchedulingPolicy(QUERY_PRIORITY);
            }
            subGroups.put(name, subGroup);
            return subGroup;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public void run(QueryExecution query)
    {
        lock.writeLock().lock();
        try {
            checkState(subGroups.isEmpty(), "Cannot add queries to %s. It is not a leaf group.", id);
            // Check all ancestors for capacity
            query.setResourceGroup(id);
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
            if (canRun) {
                startInBackground(query);
            }
            else {
                enqueueQuery(query);
            }
            query.addStateChangeListener(state -> {
                if (state.isDone()) {
                    lock.writeLock().lock();
                    try {
                        queryFinished(query);
                    }
                    finally {
                        lock.writeLock().unlock();
                    }
                }
            });
            if (query.getState().isDone()) {
                queryFinished(query);
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private void enqueueQuery(QueryExecution query)
    {
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock to enqueue a query");
        queuedQueries.addOrUpdate(query, getQueryPriority(query.getSession()));
        InternalResourceGroup group = this;
        while (group.parent.isPresent()) {
            group.parent.get().descendantQueuedQueries++;
            group = group.parent.get();
        }
        updateEligiblility();
    }

    // This method must be called whenever the group's eligibility to run more queries may have changed.
    private void updateEligiblility()
    {
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock to update eligibility");
        if (!parent.isPresent()) {
            return;
        }
        if (isEligibleToStartNext()) {
            parent.get().addOrUpdateSubGroup(this);
        }
        else {
            parent.get().eligibleSubGroups.remove(this);
        }
        parent.get().updateEligiblility();
    }

    private void startInBackground(QueryExecution query)
    {
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock to start a query");
        runningQueries.add(query);
        InternalResourceGroup group = this;
        while (group.parent.isPresent()) {
            group.parent.get().descendantRunningQueries++;
            group.parent.get().dirtySubGroups.add(group);
            group = group.parent.get();
        }
        updateEligiblility();
        executor.execute(query::start);
    }

    private void queryFinished(QueryExecution query)
    {
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock");
        if (!runningQueries.contains(query) && !queuedQueries.contains(query)) {
            // Query has already been cleaned up
            return;
        }
        // Only count the CPU time if the query succeeded, or the failure was the fault of the user
        if (query.getState() == QueryState.FINISHED || query.getQueryInfo().getErrorType() == USER_ERROR) {
            InternalResourceGroup group = this;
            while (group != null) {
                try {
                    group.cpuUsageMillis = Math.addExact(group.cpuUsageMillis, query.getTotalCpuTime().toMillis());
                }
                catch (ArithmeticException e) {
                    group.cpuUsageMillis = Long.MAX_VALUE;
                }
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
        updateEligiblility();
    }

    // Memory usage stats are expensive to maintain, so this method must be called periodically to update them
    protected void internalRefreshStats()
    {
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock to refresh stats");
        if (subGroups.isEmpty()) {
            cachedMemoryUsageBytes = 0;
            for (QueryExecution query : runningQueries) {
                cachedMemoryUsageBytes += query.getTotalMemoryReservation();
            }
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
                if (oldMemoryUsageBytes != subGroup.cachedMemoryUsageBytes) {
                    subGroup.updateEligiblility();
                }
            }
        }
    }

    protected void internalGenerateCpuQuota(long elapsedSeconds)
    {
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock to generate cpu quota");
        long newQuota;
        try {
            newQuota = Math.multiplyExact(elapsedSeconds, cpuQuotaGenerationMillisPerSecond);
        }
        catch (ArithmeticException e) {
            newQuota = Long.MAX_VALUE;
        }
        try {
            cpuUsageMillis = Math.subtractExact(cpuUsageMillis, newQuota);
        }
        catch (ArithmeticException e) {
            cpuUsageMillis = 0;
        }
        cpuUsageMillis = Math.max(0, cpuUsageMillis);
        for (InternalResourceGroup group : subGroups.values()) {
            group.internalGenerateCpuQuota(elapsedSeconds);
        }
    }

    protected boolean internalStartNext()
    {
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock to find next query");
        if (!canRunMore()) {
            return false;
        }
        QueryExecution query = queuedQueries.poll();
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
        checkState(started, "Eligible sub group had no queries to run");
        descendantQueuedQueries--;
        // Don't call updateEligibility here, as we're in a recursive call, and don't want to repeatedly update our ancestors.
        if (subGroup.isEligibleToStartNext()) {
            addOrUpdateSubGroup(subGroup);
        }
        return true;
    }

    protected void enforceTimeLimits()
    {
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock to enforce time limits");
        for (InternalResourceGroup group : subGroups.values()) {
            group.enforceTimeLimits();
        }
        for (QueryExecution query : runningQueries) {
            Duration runningTime = query.getQueryInfo().getQueryStats().getExecutionTime();
            if (runningQueries.contains(query) && runningTime != null && runningTime.compareTo(runningTimeLimit) > 0) {
                query.fail(new PrestoException(EXCEEDED_TIME_LIMIT, "query exceeded resource group runtime limit"));
            }
        }
        for (QueryExecution query : queuedQueries) {
            Duration elapsedTime = query.getQueryInfo().getQueryStats().getElapsedTime();
            if (queuedQueries.contains(query) && elapsedTime != null && elapsedTime.compareTo(queuedTimeLimit) > 0) {
                query.fail(new PrestoException(EXCEEDED_TIME_LIMIT, "query exceeded resource group queued time limit"));
            }
        }
    }

    private void addOrUpdateSubGroup(InternalResourceGroup group)
    {
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock");
        if (schedulingPolicy == WEIGHTED_FAIR) {
            ((WeightedFairQueue<InternalResourceGroup>) eligibleSubGroups).addOrUpdate(group, new Usage(group.getSchedulingWeight(), group.getRunningQueries()));
        }
        else {
            ((UpdateablePriorityQueue<InternalResourceGroup>) eligibleSubGroups).addOrUpdate(group, getSubGroupSchedulingPriority(schedulingPolicy, group));
        }
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
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock");
        if (runningQueries.size() + descendantRunningQueries >= softConcurrencyLimit) {
            return schedulingWeight;
        }

        return (long) Integer.MAX_VALUE * schedulingWeight;
    }

    private boolean isDirty()
    {
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock");
        return runningQueries.size() + descendantRunningQueries > 0;
    }

    private boolean isEligibleToStartNext()
    {
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock");
        if (!canRunMore()) {
            return false;
        }
        return !queuedQueries.isEmpty() || !eligibleSubGroups.isEmpty();
    }

    private int getHighestQueryPriority()
    {
        checkState(lock.isWriteLockedByCurrentThread(), "Must hold write lock");
        checkState(queuedQueries instanceof IndexedPriorityQueue, "Queued queries not ordered");
        if (queuedQueries.isEmpty()) {
            return 0;
        }
        return getQueryPriority(queuedQueries.peek().getSession());
    }

    private boolean canQueueMore()
    {
        lock.readLock().lock();
        try {
            return descendantQueuedQueries + queuedQueries.size() < maxQueuedQueries;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    private boolean canRunMore()
    {
        lock.readLock().lock();
        try {
            if (cpuUsageMillis >= hardCpuLimitMillis) {
                return false;
            }

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
            return runningQueries.size() + descendantRunningQueries < hardConcurrencyLimit &&
                    cachedMemoryUsageBytes <= softMemoryLimitBytes;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public Collection<InternalResourceGroup> subGroups()
    {
        lock.readLock().lock();
        try {
            return subGroups.values();
        }
        finally {
            lock.readLock().unlock();
        }
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
        public RootInternalResourceGroup(String name, BiConsumer<InternalResourceGroup, Boolean> jmxExportListener, Executor executor)
        {
            super(Optional.empty(), name, jmxExportListener, executor);
        }

        public void processQueuedQueries()
        {
            lock.writeLock().lock();
            try {
                internalRefreshStats();
                enforceTimeLimits();
                while (internalStartNext()) {
                    // start all the queries we can
                }
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        public void generateCpuQuota(long elapsedSeconds)
        {
            lock.writeLock().lock();
            try {
                if (elapsedSeconds > 0) {
                    internalGenerateCpuQuota(elapsedSeconds);
                }
            }
            finally {
                lock.writeLock().unlock();
            }
        }
    }
}
