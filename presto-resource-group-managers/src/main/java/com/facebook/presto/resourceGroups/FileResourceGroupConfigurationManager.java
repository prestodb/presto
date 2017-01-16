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
package com.facebook.presto.resourceGroups;

import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.Sets.difference;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class FileResourceGroupConfigurationManager
        extends AbstractResourceConfigurationManager
{
    private static final Logger log = Logger.get(FileResourceGroupConfigurationManager.class);

    private final ConcurrentMap<ResourceGroupId, ResourceGroup> groups = new ConcurrentHashMap<>();
    private final AtomicReference<HashMap<ResourceGroupIdTemplate, ResourceGroupSpec>> currentGroupSpecs = new AtomicReference<>();
    private final Map<ResourceGroupIdTemplate, List<ResourceGroupId>> configuredGroups = new ConcurrentHashMap<>();
    private final AtomicReference<List<ResourceGroupSpec>> rootGroups = new AtomicReference<>();
    private final AtomicReference<List<ResourceGroupSelector>> selectors = new AtomicReference<>();
    private final AtomicReference<Optional<Duration>> cpuQuotaPeriodMillis = new AtomicReference<>();

    private final WatchService watchService;
    private final ExecutorService executor;
    private final Path configFile;
    private final JsonCodec<ManagerSpec> codec;

    @Inject
    public FileResourceGroupConfigurationManager(ClusterMemoryPoolManager memoryPoolManager, FileResourceGroupConfig config, JsonCodec<ManagerSpec> codec)
    {
        super(requireNonNull(memoryPoolManager, "memoryPoolManager is null"));
        requireNonNull(config, "config is null");
        this.codec = requireNonNull(codec, "codec is null");
        executor = newSingleThreadExecutor(daemonThreadsNamed("config-watcher"));
        configFile = Paths.get(config.getConfigFile());
        try {
            watchService =  FileSystems.getDefault().newWatchService();
            // cannot watch single files with watch service so
            // watch the parent directory and filter out the events that we don't care about
            configFile.getParent().register(watchService, ENTRY_MODIFY);
            configure();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void configure() throws IOException
    {
        ManagerSpec managerSpec = codec.fromJson(readAllBytes(configFile));
        validateRootGroups(managerSpec);
        List<ResourceGroupSpec> rootGroups = managerSpec.getRootGroups();
        HashMap<ResourceGroupIdTemplate, ResourceGroupSpec> newGroupSpecs = getSpecsWithId(rootGroups);
        Set<ResourceGroupIdTemplate> configuredSpecs = configuredGroups.keySet();
        Set<ResourceGroupIdTemplate> changedSpecs = new HashSet<>();
        for (ResourceGroupIdTemplate specId : configuredSpecs) {
            if (newGroupSpecs.containsKey(specId) &&
                    !newGroupSpecs.get(specId).sameConfig(currentGroupSpecs.get().get(specId))) {
                changedSpecs.add(specId);
            }
        }
        currentGroupSpecs.set(newGroupSpecs);
        this.cpuQuotaPeriodMillis.set(managerSpec.getCpuQuotaPeriod());
        this.rootGroups.set(rootGroups);
        this.selectors.set(buildSelectors(managerSpec));
        configureChangedGroups(changedSpecs);
        disableDeletedGroups(difference(configuredSpecs, newGroupSpecs.keySet()));
    }

    @Override
    protected Optional<Duration> getCpuQuotaPeriodMillis()
    {
        return cpuQuotaPeriodMillis.get();
    }

    @Override
    protected List<ResourceGroupSpec> getRootGroups()
    {
        return rootGroups.get();
    }

    @Override
    public void configure(ResourceGroup group, SelectionContext context)
    {
        Entry<ResourceGroupIdTemplate, ResourceGroupSpec> entry = getMatchingSpec(group, context);
        if (groups.putIfAbsent(group.getId(), group) == null) {
            configuredGroups.computeIfAbsent(entry.getKey(), value -> new LinkedList<>()).add(group.getId());
        }
        synchronized (getRootGroup(group.getId())) {
            configureGroup(group, entry.getValue());
        }
    }

    private HashMap<ResourceGroupIdTemplate, ResourceGroupSpec> getSpecsWithId(List<ResourceGroupSpec> rootGroups)
    {
        HashMap<ResourceGroupIdTemplate, ResourceGroupSpec> specsMap = new HashMap<>();
        for (ResourceGroupSpec group : rootGroups) {
            ResourceGroupIdTemplate rootId = new ResourceGroupIdTemplate(group.getName().toString());
            specsMap.put(rootId, group);
            getSpecsWithId(rootId, group.getSubGroups(), specsMap);
        }
        return specsMap;
    }

    private void getSpecsWithId(ResourceGroupIdTemplate parentId, List<ResourceGroupSpec> groups, HashMap<ResourceGroupIdTemplate, ResourceGroupSpec> specsMap)
    {
        for (ResourceGroupSpec group : groups) {
            ResourceGroupIdTemplate currentId = ResourceGroupIdTemplate.forSubGroupNamed(parentId, group.getName().toString());
            specsMap.put(currentId, group);
            getSpecsWithId(currentId, group.getSubGroups(), specsMap);
        }
    }

    private ResourceGroup getRootGroup(ResourceGroupId groupId)
    {
        Optional<ResourceGroupId> parent = groupId.getParent();
        while (parent.isPresent()) {
            groupId = parent.get();
            parent = groupId.getParent();
        }
        // groupId is guaranteed to be in groups: it is added before the first call to this method in configure()
        return groups.get(groupId);
    }

    private void configureChangedGroups(Set<ResourceGroupIdTemplate> changedSpecs)
    {
        for (ResourceGroupIdTemplate spec : changedSpecs) {
            for (ResourceGroupId resourceGroupId : configuredGroups.getOrDefault(spec, ImmutableList.of())) {
                synchronized (getRootGroup(resourceGroupId)) {
                    configureGroup(groups.get(resourceGroupId), currentGroupSpecs.get().get(spec));
                }
            }
        }
    }

    private void disableDeletedGroups(Set<ResourceGroupIdTemplate> deletedSpecs)
    {
        for (ResourceGroupIdTemplate spec : deletedSpecs) {
            for (ResourceGroupId resourceGroupId : configuredGroups.getOrDefault(spec, ImmutableList.of())) {
                ResourceGroup group = groups.get(resourceGroupId);
                disableGroup(group);
            }
        }
    }

    private synchronized void disableGroup(ResourceGroup group)
    {
        // Disable groups that are removed from the config file
        group.setMaxRunningQueries(0);
        group.setMaxQueuedQueries(0);
    }

    @Override
    public List<ResourceGroupSelector> getSelectors()
    {
        return selectors.get();
    }

    @PostConstruct
    public void start()
    {
        executor.execute(() -> {
            while (true) {
                WatchKey key = null;
                try {
                    key = watchService.take();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        Path changed = (Path) event.context();
                        if (changed.equals(configFile.getFileName())) {
                            log.info("Reloading %s", configFile);
                            configure();
                        }
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    Throwables.propagate(e);
                }
                catch (Throwable t) {
                    log.warn(t, "Exception while reloading changes");
                }
                finally {
                    if (key != null && !key.reset()) {
                        log.debug("Watch key is no longer valid");
                        break;
                    }
                }
            }
        });
    }

    @PreDestroy
    public void shutdown()
    {
        try {
            if (watchService != null) {
                watchService.close();
            }
        }
        catch (IOException ignored) {
        }
        executor.shutdown();
    }
}
