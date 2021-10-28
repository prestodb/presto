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

import com.facebook.presto.resourceGroups.ManagerSpec;
import com.facebook.presto.resourceGroups.ResourceGroupIdTemplate;
import com.facebook.presto.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.resourceGroups.ResourceGroupSpec;
import com.facebook.presto.resourceGroups.SelectorSpec;
import com.facebook.presto.resourceGroups.reloading.ManagerSpecProvider;
import com.facebook.presto.resourceGroups.reloading.ReloadingResourceGroupConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.units.Duration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class DbManagerSpecProvider
        implements ManagerSpecProvider
{
    private final ResourceGroupsDao resourceGroupsDao;
    private final String environment;

    @Inject
    public DbManagerSpecProvider(ResourceGroupsDao dao, @ForEnvironment String environment, ReloadingResourceGroupConfig config)
    {
        this.resourceGroupsDao = requireNonNull(dao, "dao is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.resourceGroupsDao.createResourceGroupsGlobalPropertiesTable();
        this.resourceGroupsDao.createResourceGroupsTable();
        this.resourceGroupsDao.createSelectorsTable();
        if (config.getExactMatchSelectorEnabled()) {
            this.resourceGroupsDao.createExactMatchSelectorsTable();
        }
    }

    @Override
    public synchronized ManagerSpec getManagerSpec()
    {
        Set<Long> rootGroupIds = new HashSet<>();
        Map<Long, ResourceGroupSpec> resourceGroupSpecMap = new HashMap<>();
        Map<Long, ResourceGroupIdTemplate> resourceGroupIdTemplateMap = new HashMap<>();
        Map<Long, ResourceGroupSpecBuilder> recordMap = new HashMap<>();
        Map<Long, Set<Long>> subGroupIdsToBuild = new HashMap<>();
        populateFromDbHelper(recordMap, rootGroupIds, resourceGroupIdTemplateMap, subGroupIdsToBuild);

        // Build up resource group specs from root to leaf
        for (LinkedList<Long> queue = new LinkedList<>(rootGroupIds); !queue.isEmpty(); ) {
            Long id = queue.pollFirst();
            resourceGroupIdTemplateMap.computeIfAbsent(id, k -> {
                ResourceGroupSpecBuilder builder = recordMap.get(k);
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
        List<ResourceGroupSpec> rootGroups = rootGroupIds.stream().map(resourceGroupSpecMap::get).collect(toList());

        List<SelectorSpec> selectors = resourceGroupsDao.getSelectors(environment)
                .stream()
                .map(selectorRecord ->
                        new SelectorSpec(
                                selectorRecord.getUserRegex(),
                                selectorRecord.getSourceRegex(),
                                selectorRecord.getQueryType(),
                                selectorRecord.getClientTags(),
                                selectorRecord.getSelectorResourceEstimate(),
                                resourceGroupIdTemplateMap.get(selectorRecord.getResourceGroupId()))
                ).collect(toList());
        ManagerSpec managerSpec = new ManagerSpec(rootGroups, selectors, getCpuQuotaPeriodFromDb());
        return managerSpec;
    }

    // Populate temporary data structures to build resource group specs and selectors from db
    private synchronized void populateFromDbHelper(Map<Long, ResourceGroupSpecBuilder> recordMap,
            Set<Long> rootGroupIds,
            Map<Long, ResourceGroupIdTemplate> resourceGroupIdTemplateMap,
            Map<Long, Set<Long>> subGroupIdsToBuild)
    {
        List<ResourceGroupSpecBuilder> records = resourceGroupsDao.getResourceGroups(environment);
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

    private synchronized Optional<Duration> getCpuQuotaPeriodFromDb()
    {
        List<ResourceGroupGlobalProperties> globalProperties = resourceGroupsDao.getResourceGroupGlobalProperties();
        checkState(globalProperties.size() <= 1, "There is more than one cpu_quota_period");
        return (!globalProperties.isEmpty()) ? globalProperties.get(0).getCpuQuotaPeriod() : Optional.empty();
    }

    @Override
    public List<ResourceGroupSelector> getExactMatchSelectors()
    {
        return ImmutableList.of(new DbSourceExactMatchSelector(environment, resourceGroupsDao));
    }
}
