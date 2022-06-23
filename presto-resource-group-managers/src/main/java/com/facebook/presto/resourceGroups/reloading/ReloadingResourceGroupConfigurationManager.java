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
package com.facebook.presto.resourceGroups.reloading;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.resourceGroups.AbstractResourceConfigurationManager;
import com.facebook.presto.resourceGroups.ManagerSpec;
import com.facebook.presto.resourceGroups.ResourceGroupIdTemplate;
import com.facebook.presto.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.resourceGroups.ResourceGroupSpec;
import com.facebook.presto.resourceGroups.VariableMap;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_UNAVAILABLE;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ReloadingResourceGroupConfigurationManager
        extends AbstractResourceConfigurationManager
{
    private static final Logger log = Logger.get(ReloadingResourceGroupConfigurationManager.class);
    private final ConcurrentMap<ResourceGroupId, ResourceGroup> groups = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private Map<ResourceGroupIdTemplate, ResourceGroupSpec> resourceGroupSpecs = new HashMap<>();
    private final ConcurrentMap<ResourceGroupIdTemplate, List<ResourceGroupId>> configuredGroups = new ConcurrentHashMap<>();
    private final AtomicReference<List<ResourceGroupSpec>> rootGroups = new AtomicReference<>(ImmutableList.of());
    private final AtomicReference<List<ResourceGroupSelector>> selectors = new AtomicReference<>();
    private final AtomicReference<Optional<Duration>> cpuQuotaPeriod = new AtomicReference<>(Optional.empty());
    private final ManagerSpecProvider managerSpecProvider;
    private final ScheduledExecutorService configExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("DbResourceGroupConfigurationManager"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicLong lastRefresh = new AtomicLong();
    private final Duration maxRefreshInterval;
    private final boolean exactMatchSelectorEnabled;

    private final CounterStat refreshFailures = new CounterStat();

    @Inject
    public ReloadingResourceGroupConfigurationManager(ClusterMemoryPoolManager memoryPoolManager, ReloadingResourceGroupConfig config, ManagerSpecProvider managerSpecProvider)
    {
        super(memoryPoolManager);
        requireNonNull(memoryPoolManager, "memoryPoolManager is null");
        this.maxRefreshInterval = config.getMaxRefreshInterval();
        this.exactMatchSelectorEnabled = config.getExactMatchSelectorEnabled();
        this.managerSpecProvider = requireNonNull(managerSpecProvider, "provider is null");
        load();
    }

    @Override
    protected Optional<Duration> getCpuQuotaPeriod()
    {
        return cpuQuotaPeriod.get();
    }

    @Override
    protected List<ResourceGroupSpec> getRootGroups()
    {
        checkMaxRefreshInterval();

        if (this.selectors.get().isEmpty()) {
            throw new PrestoException(CONFIGURATION_INVALID, "No root groups are configured");
        }

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
            configExecutor.scheduleWithFixedDelay(this::load, 10, 10, TimeUnit.SECONDS);
        }
    }

    @Override
    public void configure(ResourceGroup group, SelectionContext<VariableMap> criteria)
    {
        Map.Entry<ResourceGroupIdTemplate, ResourceGroupSpec> entry = getMatchingSpec(group, criteria);
        if (groups.putIfAbsent(group.getId(), group) == null) {
            // If a new spec replaces the spec returned from getMatchingSpec the group will be reconfigured on the next run of load().
            configuredGroups.computeIfAbsent(entry.getKey(), v -> new LinkedList<>()).add(group.getId());
        }
        synchronized (getRootGroup(group.getId())) {
            configureGroup(group, entry.getValue());
        }
    }

    @Override
    public Optional<SelectionContext<VariableMap>> match(SelectionCriteria criteria)
    {
        checkMaxRefreshInterval();

        if (selectors.get().isEmpty()) {
            throw new PrestoException(CONFIGURATION_INVALID, "No selectors are configured");
        }

        return selectors.get().stream()
                .map(s -> s.match(criteria))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    @VisibleForTesting
    public List<ResourceGroupSelector> getSelectors()
    {
        checkMaxRefreshInterval();

        if (selectors.get().isEmpty()) {
            throw new PrestoException(CONFIGURATION_INVALID, "No selectors are configured");
        }
        return selectors.get();
    }

    @VisibleForTesting
    public synchronized void load()
    {
        try {
            ManagerSpec managerSpec = managerSpecProvider.getManagerSpec();
            validateRootGroups(managerSpec);

            List<ResourceGroupSpec> rootGroups = managerSpec.getRootGroups();

            Map<ResourceGroupIdTemplate, ResourceGroupSpec> resourceGroupSpecs = new HashMap<>();
            buildResourceGroupSpecsMap(resourceGroupSpecs, rootGroups, Optional.empty());

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
            ImmutableList.Builder<ResourceGroupSelector> selectorsBuilder = ImmutableList.builder();
            if (exactMatchSelectorEnabled) {
                selectorsBuilder.addAll(managerSpecProvider.getExactMatchSelectors());
            }
            selectorsBuilder.addAll(buildSelectors(managerSpec));
            this.selectors.set(selectorsBuilder.build());

            configureChangedGroups(changedSpecs);
            disableDeletedGroups(deletedSpecs);

            if (lastRefresh.get() > 0) {
                for (ResourceGroupIdTemplate deleted : deletedSpecs) {
                    log.info("Resource group spec deleted %s", deleted);
                }
                for (ResourceGroupIdTemplate changed : changedSpecs) {
                    log.info("Resource group spec %s changed to %s", changed, resourceGroupSpecs.get(changed));
                }
            }
            else {
                log.info("Loaded %s selectors and %s resource groups from source", this.selectors.get().size(), this.resourceGroupSpecs.size());
            }

            lastRefresh.set(System.nanoTime());
        }
        catch (Throwable e) {
            refreshFailures.update(1);
            log.error(e, "Error loading configuration from source");
            if (lastRefresh.get() != 0) {
                log.debug("Last successful configuration loading was %s ago", succinctNanos(System.nanoTime() - lastRefresh.get()).toString());
            }
        }
    }

    private synchronized void buildResourceGroupSpecsMap(Map<ResourceGroupIdTemplate, ResourceGroupSpec> resourceGroupSpecs, List<ResourceGroupSpec> childResourceGroups, Optional<ResourceGroupIdTemplate> parentId)
    {
        for (ResourceGroupSpec resourceGroupSpec : childResourceGroups) {
            ResourceGroupIdTemplate childId;
            if (parentId.isPresent()) {
                childId = ResourceGroupIdTemplate.forSubGroupNamed(parentId.get(), resourceGroupSpec.getName().toString());
            }
            else {
                childId = new ResourceGroupIdTemplate(resourceGroupSpec.getName().toString());
            }

            if (!resourceGroupSpec.getSubGroups().isEmpty()) {
                buildResourceGroupSpecsMap(resourceGroupSpecs, resourceGroupSpec.getSubGroups(), Optional.of(childId));
            }
            resourceGroupSpecs.put(childId, resourceGroupSpec);
        }
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
        // Disable groups that are removed from the source
        group.setHardConcurrencyLimit(0);
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

    private void checkMaxRefreshInterval()
    {
        if (System.nanoTime() - lastRefresh.get() > maxRefreshInterval.toMillis() * MILLISECONDS.toNanos(1)) {
            String message = "Resource group configuration cannot be fetched from source.";
            if (lastRefresh.get() != 0) {
                message += format(" Current resource group configuration is loaded %s ago", succinctNanos(System.nanoTime() - lastRefresh.get()).toString());
            }

            throw new PrestoException(CONFIGURATION_UNAVAILABLE, message);
        }
    }

    @Managed
    @Nested
    public CounterStat getRefreshFailures()
    {
        return refreshFailures;
    }
}
