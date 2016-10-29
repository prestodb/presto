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

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface H2ResourceGroupsDao
        extends ResourceGroupsDao
{
    @SqlUpdate("INSERT INTO resource_groups_global_properties\n" +
            "(name, value) VALUES (:name, :value)")
    void insertResourceGroupsGlobalProperties(
            @Bind("name") String name,
            @Bind("value") String value);

    @SqlUpdate("UPDATE resource_groups_global_properties SET name = :name")
    void updateResourceGroupsGlobalProperties(@Bind("name") String name);

    @SqlUpdate("INSERT INTO resource_groups\n" +
            "(resource_group_id, name, soft_memory_limit, max_queued, max_running, scheduling_policy, scheduling_weight, jmx_export, soft_cpu_limit, hard_cpu_limit, parent)\n" +
            "VALUES (:resource_group_id, :name, :soft_memory_limit, :max_queued, :max_running, :scheduling_policy, :scheduling_weight, :jmx_export, :soft_cpu_limit, :hard_cpu_limit, :parent)")
    void insertResourceGroup(
            @Bind("resource_group_id") long resourceGroupId,
            @Bind("name") String name,
            @Bind("soft_memory_limit") String softMemoryLimit,
            @Bind("max_queued") int maxQueued,
            @Bind("max_running") int maxRunning,
            @Bind("scheduling_policy") String schedulingPolicy,
            @Bind("scheduling_weight") Integer schedulingWeight,
            @Bind("jmx_export") Boolean jmxExport,
            @Bind("soft_cpu_limit") String softCpuLimit,
            @Bind("hard_cpu_limit") String hardCpuLimit,
            @Bind("parent") Long parent
    );

    @SqlUpdate("UPDATE resource_groups SET\n" +
            " resource_group_id = :resource_group_id\n" +
            ", name = :name\n" +
            ", soft_memory_limit = :soft_memory_limit\n" +
            ", max_queued = :max_queued\n" +
            ", max_running = :max_running\n" +
            ", scheduling_policy = :scheduling_policy\n" +
            ", scheduling_weight = :scheduling_weight\n" +
            ", jmx_export = :jmx_export\n" +
            ", soft_cpu_limit = :soft_cpu_limit\n" +
            ", hard_cpu_limit = :hard_cpu_limit\n" +
            ", parent = :parent\n" +
            "WHERE resource_group_id = :resource_group_id")
    void updateResourceGroup(
            @Bind("resource_group_id") long resourceGroupId,
            @Bind("name") String resourceGroup,
            @Bind("soft_memory_limit") String softMemoryLimit,
            @Bind("max_queued") int maxQueued,
            @Bind("max_running") int maxRunning,
            @Bind("scheduling_policy") String schedulingPolicy,
            @Bind("scheduling_weight") Integer schedulingWeight,
            @Bind("jmx_export") Boolean jmxExport,
            @Bind("soft_cpu_limit") String softCpuLimit,
            @Bind("hard_cpu_limit") String hardCpuLimit,
            @Bind("parent") Long parent);

    @SqlUpdate("DELETE FROM resource_groups WHERE resource_group_id = :resource_group_id")
    void deleteResourceGroup(@Bind("resource_group_id") long resourceGroupId);

    @SqlUpdate("INSERT INTO selectors\n" +
            "(resource_group_id, user_regex, source_regex)\n" +
            "VALUES (:resource_group_id, :user_regex, :source_regex)")
    void insertSelector(
            @Bind("resource_group_id") long resourceGroupId,
            @Bind("user_regex") String userRegex,
            @Bind("source_regex") String sourceRegex);

    @SqlUpdate("UPDATE selectors SET\n" +
            " resource_group_id = :resource_group_id\n" +
            ", user_regex = :user_regex\n" +
            ", source_regex = :source_regex\n" +
            "WHERE resource_group_id = :resource_group_id\n" +
            " AND ((user_regex IS NULL AND :old_user_regex IS NULL) OR user_regex = :old_user_regex)\n" +
            " AND ((source_regex IS NULL AND :old_source_regex IS NULL) OR source_regex = :old_source_regex)")
    void updateSelector(
            @Bind("resource_group_id") long resourceGroupId,
            @Bind("user_regex") String newUserRegex,
            @Bind("source_regex") String newSourceRegex,
            @Bind("old_user_regex") String oldUserRegex,
            @Bind("old_source_regex") String oldSourceRegex);

    @SqlUpdate("DELETE FROM selectors WHERE resource_group_id = :resource_group_id\n" +
            " AND ((user_regex IS NULL AND :user_regex IS NULL) OR user_regex = :user_regex)\n" +
            " AND ((source_regex IS NULL AND :source_regex IS NULL) OR source_regex = :source_regex)")
    void deleteSelector(
            @Bind("resource_group_id") long resourceGroupId,
            @Bind("user_regex") String userRegex,
            @Bind("source_regex") String sourceRegex);

    @SqlUpdate("DELETE FROM selectors WHERE resource_group_id = :resource_group_id")
    void deleteSelectors(@Bind("resource_group_id") long resourceGroup);
}
