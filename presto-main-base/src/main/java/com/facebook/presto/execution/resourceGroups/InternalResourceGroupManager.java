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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.execution.ManagedQueryExecution;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroup.RootInternalResourceGroup;
import com.facebook.presto.execution.scheduler.clusterOverload.ClusterOverloadStateListener;
import com.facebook.presto.execution.scheduler.clusterOverload.ClusterResourceChecker;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.resourcemanager.ResourceGroupService;
import com.facebook.presto.server.ResourceGroupInfo;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerContext;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManagerFactory;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;
import com.facebook.presto.util.PeriodicTaskExecutor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.ThreadSafe;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;
import org.weakref.jmx.ObjectNames;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.execution.resourceGroups.LegacyResourceGroupConfigurationManager.HARD_CONCURRENCY_LIMIT;
import static com.facebook.presto.execution.resourceGroups.LegacyResourceGroupConfigurationManager.MAX_QUEUED_QUERIES;
import static com.facebook.presto.spi.StandardErrorCode.MISSING_RESOURCE_GROUP_SELECTOR;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public final class InternalResourceGroupManager<C>
        implements ResourceGroupManager<C>, ClusterOverloadStateListener
{
    private static final Logger log = Logger.get(InternalResourceGroupManager.class);
    private static final File RESOURCE_GROUPS_CONFIGURATION = new File("etc/resource-groups.properties");
    private static final String CONFIGURATION_MANAGER_PROPERTY_NAME = "resource-groups.configuration-manager";
    private static final int REFRESH_EXECUTOR_POOL_SIZE = 2;

    private final ScheduledExecutorService refreshExecutor = newScheduledThreadPool(REFRESH_EXECUTOR_POOL_SIZE, daemonThreadsNamed("resource-group-manager-refresher-%d-" + REFRESH_EXECUTOR_POOL_SIZE));
    private final PeriodicTaskExecutor resourceGroupRuntimeExecutor;
    private final List<RootInternalResourceGroup> rootGroups = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<ResourceGroupId, InternalResourceGroup> groups = new ConcurrentHashMap<>();
    private final AtomicReference<ResourceGroupConfigurationManager<C>> configurationManager;
    private final ResourceGroupConfigurationManagerContext configurationManagerContext;
    private final ResourceGroupConfigurationManager<?> initializingConfigurationManager;
    private final MBeanExporter exporter;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicLong lastCpuQuotaGenerationNanos = new AtomicLong(System.nanoTime());
    private final Map<String, ResourceGroupConfigurationManagerFactory> configurationManagerFactories = new ConcurrentHashMap<>();
    private final AtomicBoolean taskLimitExceeded = new AtomicBoolean();
    private final int maxTotalRunningTaskCountToNotExecuteNewQuery;
    private final AtomicLong lastSchedulingCycleRunTimeMs = new AtomicLong(0L);
    private final ResourceGroupService resourceGroupService;
    private final AtomicReference<Map<ResourceGroupId, ResourceGroupRuntimeInfo>> resourceGroupRuntimeInfos = new AtomicReference<>(ImmutableMap.of());
    private final AtomicReference<Map<ResourceGroupId, ResourceGroupRuntimeInfo>> resourceGroupRuntimeInfosSnapshot = new AtomicReference<>(ImmutableMap.of());
    private final AtomicLong lastUpdatedResourceGroupRuntimeInfo = new AtomicLong(-1L);
    private final double concurrencyThreshold;
    private final Duration resourceGroupRuntimeInfoRefreshInterval;
    private final boolean isResourceManagerEnabled;
    private final QueryManagerConfig queryManagerConfig;
    private final InternalNodeManager nodeManager;
    private AtomicBoolean isConfigurationManagerLoaded;
    private final ClusterResourceChecker clusterResourceChecker;

    @Inject
    public InternalResourceGroupManager(
            ClusterMemoryPoolManager memoryPoolManager,
            QueryManagerConfig queryManagerConfig,
            NodeInfo nodeInfo,
            MBeanExporter exporter,
            ResourceGroupService resourceGroupService,
            ServerConfig serverConfig,
            InternalNodeManager nodeManager,
            ClusterResourceChecker clusterResourceChecker)
    {
        this.queryManagerConfig = requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.nodeManager = requireNonNull(nodeManager, "node manager is null");
        this.configurationManagerContext = new ResourceGroupConfigurationManagerContextInstance(memoryPoolManager, nodeInfo.getEnvironment());
        this.initializingConfigurationManager = new InitializingConfigurationManager();
        this.configurationManager = new AtomicReference(cast(initializingConfigurationManager));
        this.maxTotalRunningTaskCountToNotExecuteNewQuery = queryManagerConfig.getMaxTotalRunningTaskCountToNotExecuteNewQuery();
        this.resourceGroupService = requireNonNull(resourceGroupService, "resourceGroupService is null");
        this.concurrencyThreshold = queryManagerConfig.getConcurrencyThresholdToEnableResourceGroupRefresh();
        this.resourceGroupRuntimeInfoRefreshInterval = queryManagerConfig.getResourceGroupRunTimeInfoRefreshInterval();
        this.isResourceManagerEnabled = requireNonNull(serverConfig, "serverConfig is null").isResourceManagerEnabled();
        this.resourceGroupRuntimeExecutor = new PeriodicTaskExecutor(resourceGroupRuntimeInfoRefreshInterval.toMillis(), refreshExecutor, this::refreshResourceGroupRuntimeInfo);
        configurationManagerFactories.putIfAbsent(LegacyResourceGroupConfigurationManager.NAME, new LegacyResourceGroupConfigurationManager.Factory());
        this.isConfigurationManagerLoaded = new AtomicBoolean(false);
        this.clusterResourceChecker = clusterResourceChecker;
    }

    @Override
    public ResourceGroupInfo getResourceGroupInfo(ResourceGroupId id, boolean includeQueryInfo, boolean summarizeSubgroups, boolean includeStaticSubgroupsOnly)
    {
        checkArgument(groups.containsKey(id), "Group %s does not exist", id);
        return groups.get(id).getResourceGroupInfo(includeQueryInfo, summarizeSubgroups, includeStaticSubgroupsOnly);
    }

    @Override
    public List<ResourceGroupInfo> getPathToRoot(ResourceGroupId id)
    {
        checkArgument(groups.containsKey(id), "Group %s does not exist", id);
        return groups.get(id).getPathToRoot();
    }

    @Override
    public List<ResourceGroupInfo> getRootResourceGroups()
    {
        ImmutableList.Builder<ResourceGroupInfo> builder = ImmutableList.builder();
        rootGroups.forEach(group -> builder.add(group.getInfo()));
        return builder.build();
    }

    @Override
    public void submit(ManagedQueryExecution queryExecution, SelectionContext<C> selectionContext, Executor executor)
    {
        checkState(configurationManager.get() != null, "configurationManager not set");
        createGroupIfNecessary(selectionContext, executor);
        groups.get(selectionContext.getResourceGroupId()).run(queryExecution);
    }

    @Override
    public SelectionContext<C> selectGroup(SelectionCriteria criteria)
    {
        return configurationManager.get().match(criteria)
                .orElseThrow(() -> new PrestoException(MISSING_RESOURCE_GROUP_SELECTOR, "Query did not match any selection rule"));
    }

    @Override
    public void addConfigurationManagerFactory(ResourceGroupConfigurationManagerFactory factory)
    {
        if (configurationManagerFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Resource group configuration manager '%s' is already registered", factory.getName()));
        }
    }

    @Override
    public void loadConfigurationManager()
            throws Exception
    {
        if (RESOURCE_GROUPS_CONFIGURATION.exists()) {
            Map<String, String> properties = new HashMap<>(loadProperties(RESOURCE_GROUPS_CONFIGURATION));

            String configurationManagerName = properties.remove(CONFIGURATION_MANAGER_PROPERTY_NAME);
            checkArgument(!isNullOrEmpty(configurationManagerName),
                    "Resource groups configuration %s does not contain %s", RESOURCE_GROUPS_CONFIGURATION.getAbsoluteFile(), CONFIGURATION_MANAGER_PROPERTY_NAME);

            setConfigurationManager(configurationManagerName, properties);
        }
        else {
            Map<String, String> legacyProperties = ImmutableMap.of(
                    HARD_CONCURRENCY_LIMIT, Integer.toString(queryManagerConfig.getMaxConcurrentQueries()),
                    MAX_QUEUED_QUERIES, Integer.toString(queryManagerConfig.getMaxQueuedQueries()));
            setConfigurationManager(LegacyResourceGroupConfigurationManager.NAME, legacyProperties);
        }
        isConfigurationManagerLoaded.set(true);
    }

    private void setConfigurationManager(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading resource group configuration manager --");

        ResourceGroupConfigurationManagerFactory configurationManagerFactory = configurationManagerFactories.get(name);
        checkState(configurationManagerFactory != null, "Resource group configuration manager %s is not registered", name);

        ResourceGroupConfigurationManager<C> configurationManager = cast(configurationManagerFactory.create(ImmutableMap.copyOf(properties), configurationManagerContext));
        checkState(this.configurationManager.compareAndSet(cast(initializingConfigurationManager), configurationManager), "configurationManager already set");

        log.info("-- Loaded resource group configuration manager %s --", name);
    }

    /**
     * for use in testing to override the default configuration manager configured for the server
     */
    @VisibleForTesting
    public void forceSetConfigurationManager(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading new resource group configuration manager --");

        ResourceGroupConfigurationManagerFactory configurationManagerFactory = configurationManagerFactories.get(name);
        checkState(configurationManagerFactory != null, "Resource group configuration manager %s is not registered", name);

        ResourceGroupConfigurationManager<C> configurationManager = cast(configurationManagerFactory.create(ImmutableMap.copyOf(properties), configurationManagerContext));
        this.configurationManager.set(configurationManager);

        log.info("-- Loaded resource group configuration manager %s --", name);
    }

    @SuppressWarnings("ObjectEquality")
    @VisibleForTesting
    public ResourceGroupConfigurationManager<C> getConfigurationManager()
    {
        ResourceGroupConfigurationManager<C> manager = configurationManager.get();
        checkState(manager != initializingConfigurationManager, "cannot fetch initializing manager");
        return manager;
    }

    @PreDestroy
    public void destroy()
    {
        // Unregister from cluster overload state changes
        clusterResourceChecker.removeListener(this);
        refreshExecutor.shutdownNow();
        resourceGroupRuntimeExecutor.stop();
    }

    @PostConstruct
    public void start()
    {
        if (started.compareAndSet(false, true)) {
            refreshExecutor.scheduleWithFixedDelay(() -> {
                try {
                    refreshAndStartQueries();
                    lastSchedulingCycleRunTimeMs.getAndSet(currentTimeMillis());
                }
                catch (Throwable t) {
                    log.error(t, "Error while executing refreshAndStartQueries");
                    throw t;
                }
            }, 1, 1, MILLISECONDS);
            if (isResourceManagerEnabled) {
                resourceGroupRuntimeExecutor.start();
            }

            // Register as listener for cluster overload state changes
            clusterResourceChecker.addListener(this);
        }
    }

    @Override
    public List<ResourceGroupRuntimeInfo> getResourceGroupRuntimeInfos()
    {
        ImmutableList.Builder<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfos = ImmutableList.builder();
        rootGroups.forEach(resourceGroup -> buildResourceGroupRuntimeInfo(resourceGroupRuntimeInfos, resourceGroup));
        return resourceGroupRuntimeInfos.build();
    }

    @Override
    public boolean isConfigurationManagerLoaded()
    {
        return isConfigurationManagerLoaded.get();
    }

    private void buildResourceGroupRuntimeInfo(ImmutableList.Builder<ResourceGroupRuntimeInfo> resourceGroupRuntimeInfos, InternalResourceGroup resourceGroup)
    {
        if (!resourceGroup.subGroups().isEmpty()) {
            resourceGroup.subGroups().stream().forEach(subGroup -> buildResourceGroupRuntimeInfo(resourceGroupRuntimeInfos, subGroup));
            return;
        }
        if (resourceGroup.getQueuedQueries() > 0 || resourceGroup.getRunningQueries() > 0) {
            ResourceGroupRuntimeInfo.Builder resourceGroupRuntimeInfo = ResourceGroupRuntimeInfo.builder(resourceGroup.getId());
            resourceGroupRuntimeInfo.addRunningQueries(resourceGroup.getRunningQueries());
            resourceGroupRuntimeInfo.addQueuedQueries(resourceGroup.getQueuedQueries());
            resourceGroupRuntimeInfo.setResourceGroupSpecInfo(new ResourceGroupSpecInfo(resourceGroup.getSoftConcurrencyLimit()));
            resourceGroupRuntimeInfos.add(resourceGroupRuntimeInfo.build());
        }
    }

    private void refreshResourceGroupRuntimeInfo()
    {
        try {
            List<ResourceGroupRuntimeInfo> resourceGroupInfos = resourceGroupService.getResourceGroupInfo();
            resourceGroupRuntimeInfos.set(resourceGroupInfos.stream().collect(toImmutableMap(ResourceGroupRuntimeInfo::getResourceGroupId, i -> i)));
            lastUpdatedResourceGroupRuntimeInfo.set(currentTimeMillis());
            boolean updatedSnapshot = updateResourceGroupsSnapshot();
            if (updatedSnapshot) {
                rootGroups.forEach(group -> group.setDirty());
            }
        }
        catch (Throwable t) {
            log.error(t, "Error while executing refreshResourceGroupRuntimeInfo");
        }
    }

    private void refreshAndStartQueries()
    {
        long nanoTime = System.nanoTime();
        long elapsedSeconds = NANOSECONDS.toSeconds(nanoTime - lastCpuQuotaGenerationNanos.get());
        if (elapsedSeconds > 0) {
            // Only advance our clock on second boundaries to avoid calling generateCpuQuota() too frequently, and because it would be a no-op for zero seconds.
            lastCpuQuotaGenerationNanos.addAndGet(elapsedSeconds * 1_000_000_000L);
        }
        else if (elapsedSeconds < 0) {
            // nano time has overflowed
            lastCpuQuotaGenerationNanos.set(nanoTime);
        }

        if (maxTotalRunningTaskCountToNotExecuteNewQuery != Integer.MAX_VALUE) {
            taskLimitExceeded.set(getTotalRunningTaskCount() > maxTotalRunningTaskCountToNotExecuteNewQuery);
        }

        for (RootInternalResourceGroup group : rootGroups) {
            try {
                if (elapsedSeconds > 0) {
                    group.generateCpuQuota(elapsedSeconds);
                }
            }
            catch (RuntimeException e) {
                log.error(e, "Exception while generation cpu quota for %s", group);
            }
            try {
                group.setTaskLimitExceeded(taskLimitExceeded.get());
                group.processQueuedQueries();
            }
            catch (RuntimeException e) {
                log.error(e, "Exception while processing queued queries for %s", group);
            }
        }
    }

    private boolean updateResourceGroupsSnapshot()
    {
        if (!isResourceManagerEnabled) {
            return false;
        }
        Map<ResourceGroupId, ResourceGroupRuntimeInfo> snapshotValue = resourceGroupRuntimeInfos.getAndAccumulate(
                resourceGroupRuntimeInfosSnapshot.get(),
                (current, update) -> current != update ? update : null);
        if (snapshotValue != null) {
            resourceGroupRuntimeInfosSnapshot.set(snapshotValue);
            return true;
        }
        return false;
    }

    @VisibleForTesting
    public Map<ResourceGroupId, ResourceGroupRuntimeInfo> getResourceGroupRuntimeInfosSnapshot()
    {
        return resourceGroupRuntimeInfosSnapshot.get();
    }

    private synchronized void createGroupIfNecessary(SelectionContext<C> context, Executor executor)
    {
        ResourceGroupId id = context.getResourceGroupId();
        if (!groups.containsKey(id)) {
            InternalResourceGroup group;
            if (id.getParent().isPresent()) {
                createGroupIfNecessary(new SelectionContext<>(id.getParent().get(), context.getContext()), executor);
                InternalResourceGroup parent = groups.get(id.getParent().get());
                requireNonNull(parent, "parent is null");
                // parent segments size equals to subgroup segment index
                int subGroupSegmentIndex = parent.getId().getSegments().size();
                group = parent.getOrCreateSubGroup(id.getLastSegment(), !context.getFirstDynamicSegmentPosition().equals(OptionalInt.of(subGroupSegmentIndex)));
            }
            else {
                RootInternalResourceGroup root;
                if (!isResourceManagerEnabled) {
                    root = new RootInternalResourceGroup(id.getSegments().get(0), this::exportGroup, executor, ignored -> Optional.empty(), rg -> false, nodeManager, clusterResourceChecker);
                }
                else {
                    root = new RootInternalResourceGroup(
                            id.getSegments().get(0),
                            this::exportGroup,
                            executor,
                            resourceGroupId -> Optional.ofNullable(resourceGroupRuntimeInfosSnapshot.get().get(resourceGroupId)),
                            rg -> shouldWaitForResourceManagerUpdate(
                                    rg,
                                    resourceGroupRuntimeInfosSnapshot::get,
                                    lastUpdatedResourceGroupRuntimeInfo::get,
                                    concurrencyThreshold),
                            nodeManager,
                            clusterResourceChecker);
                }
                group = root;
                rootGroups.add(root);
            }
            configurationManager.get().configure(group, context);
            checkState(groups.put(id, group) == null, "Unexpected existing resource group");
        }
    }

    private void exportGroup(InternalResourceGroup group, Boolean export)
    {
        String objectName = ObjectNames.builder(InternalResourceGroup.class, group.getId().toString()).build();
        try {
            if (export) {
                exporter.export(objectName, group);
            }
            else {
                exporter.unexport(objectName);
            }
        }
        catch (JmxException e) {
            log.error(e, "Error %s resource group %s", export ? "exporting" : "unexporting", group.getId());
        }
    }

    private static boolean shouldWaitForResourceManagerUpdate(
            InternalResourceGroup resourceGroup,
            Supplier<Map<ResourceGroupId, ResourceGroupRuntimeInfo>> resourceGroupRuntimeInfos,
            LongSupplier lastUpdatedResourceGroupRuntimeInfo,
            double concurrencyThreshold)
    {
        int hardConcurrencyLimit = resourceGroup.getHardConcurrencyLimitBasedOnCpuUsage();
        int totalRunningQueries = resourceGroup.getRunningQueries();
        ResourceGroupRuntimeInfo resourceGroupRuntimeInfo = resourceGroupRuntimeInfos.get().get(resourceGroup.getId());
        if (resourceGroupRuntimeInfo != null) {
            totalRunningQueries += resourceGroupRuntimeInfo.getRunningQueries() + resourceGroupRuntimeInfo.getDescendantRunningQueries();
        }
        return totalRunningQueries >= (hardConcurrencyLimit * concurrencyThreshold) && lastUpdatedResourceGroupRuntimeInfo.getAsLong() <= resourceGroup.getLastRunningQueryStartTime();
    }

    @Managed
    public int getQueriesQueuedOnInternal()
    {
        int queriesQueuedInternal = 0;
        for (RootInternalResourceGroup rootGroup : rootGroups) {
            synchronized (rootGroup) {
                queriesQueuedInternal += getQueriesQueuedOnInternal(rootGroup);
            }
        }

        return queriesQueuedInternal;
    }

    @Override
    public void onClusterEnteredOverloadedState()
    {
        // Resource groups will handle overload state through their existing admission control logic
        // No additional action needed here as queries will be queued automatically
    }

    @Override
    public void onClusterExitedOverloadedState()
    {
        log.info("Cluster exited overloaded state, updating eligibility for all resource groups");
        for (RootInternalResourceGroup rootGroup : rootGroups) {
            synchronized (rootGroup) {
                rootGroup.updateEligibilityRecursively(rootGroup);
            }
        }
    }

    @Managed
    public long getLastSchedulingCycleRuntimeDelayMs()
    {
        // When coordinator restarts/deploy, the initial 0 value make sure the metric won't spike. Without it, the first metric published will have larger value
        // due to the delay from the initialization to the actual successful run of refreshAndStartQueries method
        return lastSchedulingCycleRunTimeMs.get() == 0L ? lastSchedulingCycleRunTimeMs.get() : currentTimeMillis() - lastSchedulingCycleRunTimeMs.get();
    }

    private int getQueriesQueuedOnInternal(InternalResourceGroup resourceGroup)
    {
        if (resourceGroup.subGroups().isEmpty()) {
            int queuedQueries = resourceGroup.getQueuedQueries();
            int runningQueries = resourceGroup.getRunningQueries();
            if (isResourceManagerEnabled) {
                ResourceGroupRuntimeInfo resourceGroupRuntimeInfo = resourceGroupRuntimeInfos.get().get(resourceGroup.getId());
                if (resourceGroupRuntimeInfo != null) {
                    queuedQueries += resourceGroupRuntimeInfo.getQueuedQueries();
                    runningQueries += resourceGroupRuntimeInfo.getRunningQueries();
                }
            }
            return Math.max(Math.min(queuedQueries, resourceGroup.getSoftConcurrencyLimit() - runningQueries), 0);
        }

        int queriesQueuedInternal = 0;
        for (InternalResourceGroup subGroup : resourceGroup.subGroups()) {
            queriesQueuedInternal += getQueriesQueuedOnInternal(subGroup);
        }

        return queriesQueuedInternal;
    }

    @Managed
    public int getTaskLimitExceeded()
    {
        return taskLimitExceeded.get() ? 1 : 0;
    }

    private int getTotalRunningTaskCount()
    {
        int taskCount = 0;
        for (RootInternalResourceGroup rootGroup : rootGroups) {
            synchronized (rootGroup) {
                taskCount += rootGroup.getRunningTaskCount();
            }
        }
        return taskCount;
    }

    @VisibleForTesting
    public void setTaskLimitExceeded(boolean exceeded)
    {
        taskLimitExceeded.set(exceeded);
        for (RootInternalResourceGroup group : rootGroups) {
            group.setTaskLimitExceeded(exceeded);
        }
    }

    @SuppressWarnings("unchecked")
    private static <C> ResourceGroupConfigurationManager<C> cast(ResourceGroupConfigurationManager<?> manager)
    {
        return (ResourceGroupConfigurationManager<C>) manager;
    }

    private static class InitializingConfigurationManager
            implements ResourceGroupConfigurationManager<Void>
    {
        @Override
        public void configure(ResourceGroup group, SelectionContext criteria)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }

        @Override
        public Optional<SelectionContext<Void>> match(SelectionCriteria criteria)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }
    }
}
