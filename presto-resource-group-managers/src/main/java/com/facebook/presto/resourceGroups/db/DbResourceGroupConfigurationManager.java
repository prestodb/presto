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
package com.facebook.presto.resourceGroups.db;

import com.facebook.presto.resourceGroups.AbstractResourceConfigurationManager;
import com.facebook.presto.resourceGroups.ManagerSpec;
import com.facebook.presto.resourceGroups.ResourceGroupIdTemplate;
import com.facebook.presto.resourceGroups.ResourceGroupSpec;
import com.facebook.presto.resourceGroups.SelectorSpec;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class DbResourceGroupConfigurationManager
        extends AbstractResourceConfigurationManager
{
    private final ResourceGroupsDao dao;
    private final ConcurrentMap<ResourceGroupId, ResourceGroup> groups = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private Map<ResourceGroupIdTemplate, ResourceGroupSpec> resourceGroupSpecs = new HashMap<>();
    private final ConcurrentMap<ResourceGroupIdTemplate, List<ResourceGroupId>> configuredGroups = new ConcurrentHashMap<>();
    private final AtomicReference<List<ResourceGroupSpec>> rootGroups = new AtomicReference<>(ImmutableList.of());
    private final AtomicReference<List<ResourceGroupSelector>> selectors = new AtomicReference<>();
    private final AtomicReference<Optional<Duration>> cpuQuotaPeriod = new AtomicReference<>(Optional.empty());
    private final ScheduledExecutorService configExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("DbResourceGroupConfigurationManager"));
    private final AtomicBoolean started = new AtomicBoolean();

    @Inject
    public DbResourceGroupConfigurationManager(ClusterMemoryPoolManager memoryPoolManager, ResourceGroupsDao dao)
    {
        super(memoryPoolManager);
        requireNonNull(memoryPoolManager, "memoryPoolManager is null");
        requireNonNull(dao, "daoProvider is null");
        this.dao = dao;
        this.dao.createResourceGroupsGlobalPropertiesTable();
        this.dao.createResourceGroupsTable();
        this.dao.createSelectorsTable();
        load();
    }

    @Override
    protected Optional<Duration> getCpuQuotaPeriodMillis()
    {
        return cpuQuotaPeriod.get();
    }

    @Override
    protected List<ResourceGroupSpec> getRootGroups()
    {
        return rootGroups.get();
    }

    @PreDestroy
    public void destroy()
    {
        configExecutor.shutdownNow();
    }

    @PostConstruct
    public void start()
    {
        if (started.compareAndSet(false, true)) {
            configExecutor.scheduleWithFixedDelay(this::load, 1, 1, TimeUnit.SECONDS);
        }
    }

    @Override
    public void configure(ResourceGroup group, SelectionContext context)
    {
        Map.Entry<ResourceGroupIdTemplate, ResourceGroupSpec> entry = getMatchingSpec(group, context);
        if (groups.putIfAbsent(group.getId(), group) == null) {
            // If a new spec replaces the spec returned from getMatchingSpec the group will be reconfigured on the next run of load().
            configuredGroups.computeIfAbsent(entry.getKey(), v -> new LinkedList<>()).add(group.getId());
        }
        synchronized (getRootGroup(group.getId())) {
            configureGroup(group, entry.getValue());
        }
    }

    @Override
    public List<ResourceGroupSelector> getSelectors()
    {
        return this.selectors.get();
    }

    private synchronized Optional<Duration> getCpuQuotaPeriodFromDb()
    {
        List<ResourceGroupGlobalProperties> globalProperties = dao.getResourceGroupGlobalProperties();
        checkState(globalProperties.size() <= 1, "There is more than one cpu_quota_period");
        return (!globalProperties.isEmpty()) ? globalProperties.get(0).getCpuQuotaPeriod() : Optional.empty();
    }

    private synchronized void load()
    {
        Map.Entry<ManagerSpec, Map<ResourceGroupIdTemplate, ResourceGroupSpec>> specsFromDb = buildSpecsFromDb();
        ManagerSpec managerSpec = specsFromDb.getKey();
        Map<ResourceGroupIdTemplate, ResourceGroupSpec> resourceGroupSpecs = specsFromDb.getValue();
        Set<ResourceGroupIdTemplate> changedSpecs = new HashSet<>();
        Set<ResourceGroupIdTemplate> deletedSpecs = Sets.difference(this.resourceGroupSpecs.keySet(), resourceGroupSpecs.keySet());

        for (Map.Entry<ResourceGroupIdTemplate, ResourceGroupSpec> entry : resourceGroupSpecs.entrySet()) {
            if (!entry.getValue().sameConfig(this.resourceGroupSpecs.get(entry.getKey()))) {
                changedSpecs.add(entry.getKey());
            }
        }

        this.resourceGroupSpecs = resourceGroupSpecs;
        this.cpuQuotaPeriod.set(managerSpec.getCpuQuotaPeriod());
        this.rootGroups.set(managerSpec.getRootGroups());
        this.selectors.set(buildSelectors(managerSpec));

        configureChangedGroups(changedSpecs);
        disableDeletedGroups(deletedSpecs);
    }

    // Populate temporary data structures to build resource group specs and selectors from db
    private synchronized void populateFromDbHelper(Map<Long, ResourceGroupSpecBuilder> recordMap,
                                                   Set<Long> rootGroupIds,
                                                   Map<Long, ResourceGroupIdTemplate> resourceGroupIdTemplateMap,
                                                   Map<Long, Set<Long>> subGroupIdsToBuild)
    {
        List<ResourceGroupSpecBuilder> records = dao.getResourceGroups();
        for (ResourceGroupSpecBuilder record : records) {
            recordMap.put(record.getId(), record);
            if (!record.getParentId().isPresent()) {
                rootGroupIds.add(record.getId());
                resourceGroupIdTemplateMap.put(record.getId(), new ResourceGroupIdTemplate(record.getNameTemplate().toString()));
            }
            else {
                subGroupIdsToBuild.computeIfAbsent(record.getParentId().get(), k -> new HashSet<>()).add(record.getId());
            }
        }
    }

    private synchronized Map.Entry<ManagerSpec, Map<ResourceGroupIdTemplate, ResourceGroupSpec>> buildSpecsFromDb()
    {
        // New resource group spec map
        Map<ResourceGroupIdTemplate, ResourceGroupSpec> resourceGroupSpecs = new HashMap<>();
        // Set of root group db ids
        Set<Long> rootGroupIds = new HashSet<>();
        // Map of id from db to resource group spec
        Map<Long, ResourceGroupSpec> resourceGroupSpecMap = new HashMap<>();
        // Map of id from db to resource group template id
        Map<Long, ResourceGroupIdTemplate> resourceGroupIdTemplateMap = new HashMap<>();
        // Map of id from db to resource group spec builder
        Map<Long, ResourceGroupSpecBuilder> recordMap = new HashMap<>();
        // Map of subgroup id's not yet built
        Map<Long, Set<Long>> subGroupIdsToBuild = new HashMap<>();
        populateFromDbHelper(recordMap, rootGroupIds, resourceGroupIdTemplateMap, subGroupIdsToBuild);
        // Build up resource group specs from leaf to root
        for (LinkedList<Long> queue = new LinkedList<>(rootGroupIds); !queue.isEmpty(); ) {
            Long id = queue.pollFirst();
            resourceGroupIdTemplateMap.computeIfAbsent(id, k -> {
                ResourceGroupSpecBuilder builder = recordMap.get(id);
                return ResourceGroupIdTemplate.forSubGroupNamed(
                        resourceGroupIdTemplateMap.get(builder.getParentId().get()),
                        builder.getNameTemplate().toString());
            });
            Set<Long> childrenToBuild = subGroupIdsToBuild.getOrDefault(id, ImmutableSet.of());
            // Add to resource group specs if no more child resource groups are left to build
            if (childrenToBuild.isEmpty()) {
                ResourceGroupSpecBuilder builder = recordMap.get(id);
                ResourceGroupSpec resourceGroupSpec = builder.build();
                resourceGroupSpecMap.put(id, resourceGroupSpec);
                // Add newly built spec to spec map
                resourceGroupSpecs.put(resourceGroupIdTemplateMap.get(id), resourceGroupSpec);
                // Add this resource group spec to parent subgroups and remove id from subgroup ids to build
                builder.getParentId().ifPresent(parentId -> {
                    recordMap.get(parentId).addSubGroup(resourceGroupSpec);
                    subGroupIdsToBuild.get(parentId).remove(id);
                });
            }
            else {
                // Add this group back to queue since it still has subgroups to build
                queue.addFirst(id);
                // Add this group's subgroups to the queue so that when this id is dequeued again childrenToBuild will be empty
                queue.addAll(0, childrenToBuild);
            }
        }

        // Specs are built from db records, validate and return manager spec
        List<ResourceGroupSpec> rootGroups = rootGroupIds.stream().map(resourceGroupSpecMap::get).collect(Collectors.toList());

        List<SelectorSpec> selectors = dao.getSelectors().stream().map(selectorRecord ->
                new SelectorSpec(selectorRecord.getUserRegex(), selectorRecord.getSourceRegex(),
                        resourceGroupIdTemplateMap.get(selectorRecord.getResourceGroupId()))
        ).collect(Collectors.toList());
        ManagerSpec managerSpec = new ManagerSpec(rootGroups, selectors, getCpuQuotaPeriodFromDb());
        validateRootGroups(managerSpec);
        return new AbstractMap.SimpleImmutableEntry<>(managerSpec, resourceGroupSpecs);
    }

    private synchronized void configureChangedGroups(Set<ResourceGroupIdTemplate> changedSpecs)
    {
        for (ResourceGroupIdTemplate resourceGroupIdTemplate : changedSpecs) {
            for (ResourceGroupId resourceGroupId : configuredGroups.getOrDefault(resourceGroupIdTemplate, ImmutableList.of())) {
                synchronized (getRootGroup(resourceGroupId)) {
                    configureGroup(groups.get(resourceGroupId), resourceGroupSpecs.get(resourceGroupIdTemplate));
                }
            }
        }
    }

    private synchronized void disableDeletedGroups(Set<ResourceGroupIdTemplate> deletedSpecs)
    {
        for (ResourceGroupIdTemplate resourceGroupIdTemplate : deletedSpecs) {
            for (ResourceGroupId resourceGroupId : configuredGroups.getOrDefault(resourceGroupIdTemplate, ImmutableList.of())) {
                disableGroup(groups.get(resourceGroupId));
            }
        }
    }

    private synchronized void disableGroup(ResourceGroup group)
    {
        // Disable groups that are removed from the db
        group.setMaxRunningQueries(0);
        group.setMaxQueuedQueries(0);
    }

    private ResourceGroup getRootGroup(ResourceGroupId groupId)
    {
        Optional<ResourceGroupId> parent = groupId.getParent();
        while (parent.isPresent()) {
            groupId = parent.get();
            parent = groupId.getParent();
        }
        // GroupId is guaranteed to be in groups: it is added before the first call to this method in configure()
        return groups.get(groupId);
    }
}
