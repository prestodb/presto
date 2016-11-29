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

import com.facebook.presto.resourceGroups.ResourceGroupNameTemplate;
import com.facebook.presto.resourceGroups.ResourceGroupSpec;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ResourceGroupSpecBuilder
{
    private final long id;
    private final ResourceGroupNameTemplate nameTemplate;
    private final String softMemoryLimit;
    private final int maxQueued;
    private final int maxRunning;
    private final Optional<String> schedulingPolicy;
    private final Optional<Integer> schedulingWeight;
    private final Optional<Boolean> jmxExport;
    private final Optional<Duration> softCpuLimit;
    private final Optional<Duration> hardCpuLimit;
    private final Optional<Long> parentId;
    private final ImmutableList.Builder<ResourceGroupSpec> subGroups = ImmutableList.builder();

    ResourceGroupSpecBuilder(
            long id,
            ResourceGroupNameTemplate nameTemplate,
            String softMemoryLimit,
            int maxQueued,
            int maxRunning,
            Optional<String> schedulingPolicy,
            Optional<Integer> schedulingWeight,
            Optional<Boolean> jmxExport,
            Optional<String> softCpuLimit,
            Optional<String> hardCpuLimit,
            Optional<Long> parentId
    )
    {
        this.id = id;
        this.nameTemplate = nameTemplate;
        this.softMemoryLimit = requireNonNull(softMemoryLimit, "softMemoryLimit is null");
        this.maxQueued = maxQueued;
        this.maxRunning = maxRunning;
        this.schedulingPolicy = requireNonNull(schedulingPolicy, "schedulingPolicy is null");
        this.schedulingWeight = schedulingWeight;
        this.jmxExport = requireNonNull(jmxExport, "jmxExport is null");
        this.softCpuLimit = requireNonNull(softCpuLimit, "softCpuLimit is null").map(Duration::valueOf);
        this.hardCpuLimit = requireNonNull(hardCpuLimit, "hardCpuLimit is null").map(Duration::valueOf);
        this.parentId = parentId;
    }

    public long getId()
    {
        return id;
    }

    public ResourceGroupNameTemplate getNameTemplate()
    {
        return nameTemplate;
    }

    public Optional<Duration> getSoftCpuLimit()
    {
        return softCpuLimit;
    }

    public Optional<Duration> getHardCpuLimit()
    {
        return hardCpuLimit;
    }

    public Optional<Long> getParentId()
    {
        return parentId;
    }

    public void addSubGroup(ResourceGroupSpec subGroup)
    {
        subGroups.add(subGroup);
    }

    public ResourceGroupSpec build()
    {
        return new ResourceGroupSpec(
                nameTemplate,
                softMemoryLimit,
                maxQueued,
                maxRunning,
                schedulingPolicy,
                schedulingWeight,
                Optional.of(subGroups.build()),
                jmxExport,
                softCpuLimit,
                hardCpuLimit

        );
    }

    public static class Mapper
            implements ResultSetMapper<ResourceGroupSpecBuilder>
    {
        @Override
        public ResourceGroupSpecBuilder map(int index, ResultSet resultSet, StatementContext context)
                throws SQLException
        {
            long id = resultSet.getLong("resource_group_id");
            ResourceGroupNameTemplate nameTemplate = new ResourceGroupNameTemplate(resultSet.getString("name"));
            String softMemoryLimit = resultSet.getString("soft_memory_limit");
            int maxQueued = resultSet.getInt("max_queued");
            int maxRunning = resultSet.getInt("max_running");
            Optional<String> schedulingPolicy = Optional.ofNullable(resultSet.getString("scheduling_policy"));
            Optional<Integer> schedulingWeight = Optional.of(resultSet.getInt("scheduling_weight"));
            if (resultSet.wasNull()) {
                schedulingWeight = Optional.empty();
            }
            Optional<Boolean> jmxExport = Optional.of(resultSet.getBoolean("jmx_export"));
            if (resultSet.wasNull()) {
                jmxExport = Optional.empty();
            }
            Optional<String> softCpuLimit = Optional.ofNullable(resultSet.getString("soft_cpu_limit"));
            Optional<String> hardCpuLimit = Optional.ofNullable(resultSet.getString("hard_cpu_limit"));
            Optional<Long> parentId = Optional.of(resultSet.getLong("parent"));
            if (resultSet.wasNull()) {
                parentId = Optional.empty();
            }
            return new ResourceGroupSpecBuilder(
                    id,
                    nameTemplate,
                    softMemoryLimit,
                    maxQueued,
                    maxRunning,
                    schedulingPolicy,
                    schedulingWeight,
                    jmxExport,
                    softCpuLimit,
                    hardCpuLimit,
                    parentId
            );
        }
    }
}
