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
package io.prestosql.plugin.resourcegroups.db;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

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
            "(resource_group_id, name, soft_memory_limit, max_queued, soft_concurrency_limit, hard_concurrency_limit, scheduling_policy, scheduling_weight, jmx_export, soft_cpu_limit, hard_cpu_limit, parent, environment)\n" +
            "VALUES (:resource_group_id, :name, :soft_memory_limit, :max_queued, :soft_concurrency_limit, :hard_concurrency_limit, :scheduling_policy, :scheduling_weight, :jmx_export, :soft_cpu_limit, :hard_cpu_limit, :parent, :environment)")
    void insertResourceGroup(
            @Bind("resource_group_id") long resourceGroupId,
            @Bind("name") String name,
            @Bind("soft_memory_limit") String softMemoryLimit,
            @Bind("max_queued") int maxQueued,
            @Bind("soft_concurrency_limit") Integer softConcurrencyLimit,
            @Bind("hard_concurrency_limit") int hardConcurrencyLimit,
            @Bind("scheduling_policy") String schedulingPolicy,
            @Bind("scheduling_weight") Integer schedulingWeight,
            @Bind("jmx_export") Boolean jmxExport,
            @Bind("soft_cpu_limit") String softCpuLimit,
            @Bind("hard_cpu_limit") String hardCpuLimit,
            @Bind("parent") Long parent,
            @Bind("environment") String environment);

    @SqlUpdate("UPDATE resource_groups SET\n" +
            " resource_group_id = :resource_group_id\n" +
            ", name = :name\n" +
            ", soft_memory_limit = :soft_memory_limit\n" +
            ", max_queued = :max_queued\n" +
            ", soft_concurrency_limit = :soft_concurrency_limit\n" +
            ", hard_concurrency_limit = :hard_concurrency_limit\n" +
            ", scheduling_policy = :scheduling_policy\n" +
            ", scheduling_weight = :scheduling_weight\n" +
            ", jmx_export = :jmx_export\n" +
            ", soft_cpu_limit = :soft_cpu_limit\n" +
            ", hard_cpu_limit = :hard_cpu_limit\n" +
            ", parent = :parent\n" +
            ", environment = :environment\n" +
            "WHERE resource_group_id = :resource_group_id")
    void updateResourceGroup(
            @Bind("resource_group_id") long resourceGroupId,
            @Bind("name") String resourceGroup,
            @Bind("soft_memory_limit") String softMemoryLimit,
            @Bind("max_queued") int maxQueued,
            @Bind("soft_concurrency_limit") Integer softConcurrencyLimit,
            @Bind("hard_concurrency_limit") int hardConcurrencyLimit,
            @Bind("scheduling_policy") String schedulingPolicy,
            @Bind("scheduling_weight") Integer schedulingWeight,
            @Bind("jmx_export") Boolean jmxExport,
            @Bind("soft_cpu_limit") String softCpuLimit,
            @Bind("hard_cpu_limit") String hardCpuLimit,
            @Bind("parent") Long parent,
            @Bind("environment") String environment);

    @SqlUpdate("DELETE FROM resource_groups WHERE resource_group_id = :resource_group_id")
    void deleteResourceGroup(@Bind("resource_group_id") long resourceGroupId);

    @SqlUpdate("INSERT INTO selectors\n" +
            "(resource_group_id, priority, user_regex, source_regex, query_type, client_tags, selector_resource_estimate)\n" +
            "VALUES (:resource_group_id, :priority, :user_regex, :source_regex, :query_type, :client_tags, :selector_resource_estimate)")
    void insertSelector(
            @Bind("resource_group_id") long resourceGroupId,
            @Bind("priority") long priority,
            @Bind("user_regex") String userRegex,
            @Bind("source_regex") String sourceRegex,
            @Bind("query_type") String queryType,
            @Bind("client_tags") String clientTags,
            @Bind("selector_resource_estimate") String selectorResourceEstimate);

    @SqlUpdate("UPDATE selectors SET\n" +
            " resource_group_id = :resource_group_id\n" +
            ", user_regex = :user_regex\n" +
            ", source_regex = :source_regex\n" +
            ", client_tags = :client_tags\n" +
            "WHERE resource_group_id = :resource_group_id\n" +
            " AND ((user_regex IS NULL AND :old_user_regex IS NULL) OR user_regex = :old_user_regex)\n" +
            " AND ((source_regex IS NULL AND :old_source_regex IS NULL) OR source_regex = :old_source_regex)\n" +
            " AND ((client_tags IS NULL AND :old_client_tags IS NULL) OR client_tags = :old_client_tags)")
    void updateSelector(
            @Bind("resource_group_id") long resourceGroupId,
            @Bind("user_regex") String newUserRegex,
            @Bind("source_regex") String newSourceRegex,
            @Bind("client_tags") String newClientTags,
            @Bind("old_user_regex") String oldUserRegex,
            @Bind("old_source_regex") String oldSourceRegex,
            @Bind("old_client_tags") String oldClientTags);

    @SqlUpdate("DELETE FROM selectors WHERE resource_group_id = :resource_group_id\n" +
            " AND ((user_regex IS NULL AND :user_regex IS NULL) OR user_regex = :user_regex)\n" +
            " AND ((source_regex IS NULL AND :source_regex IS NULL) OR source_regex = :source_regex)\n" +
            " AND ((client_tags IS NULL AND :client_tags IS NULL) OR client_tags = :client_tags)")
    void deleteSelector(
            @Bind("resource_group_id") long resourceGroupId,
            @Bind("user_regex") String userRegex,
            @Bind("source_regex") String sourceRegex,
            @Bind("client_tags") String clientTags);

    @SqlUpdate("DELETE FROM selectors WHERE resource_group_id = :resource_group_id")
    void deleteSelectors(@Bind("resource_group_id") long resourceGroup);

    @SqlUpdate("INSERT INTO exact_match_source_selectors (environment, source, query_type, update_time, resource_group_id)\n" +
            "VALUES (:environment, :source, :query_type, now(), :resourceGroupId)\n")
    void insertExactMatchSelector(
            @Bind("environment") String environment,
            @Bind("source") String source,
            @Bind("query_type") String queryType,
            @Bind("resourceGroupId") String resourceGroupId);

    @SqlUpdate("DROP TABLE selectors")
    void dropSelectorsTable();
}
