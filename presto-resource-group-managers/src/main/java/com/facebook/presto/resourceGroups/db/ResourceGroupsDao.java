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
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.List;

public interface ResourceGroupsDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS resource_groups_global_properties (\n" +
            "  name VARCHAR(128) NOT NULL PRIMARY KEY,\n" +
            "  value VARCHAR(512) NULL,\n" +
            "  CHECK (name in ('cpu_quota_period'))\n" +
            ")")
    void createResourceGroupsGlobalPropertiesTable();

    @SqlQuery("SELECT value FROM resource_groups_global_properties WHERE name = 'cpu_quota_period'")
    @Mapper(ResourceGroupGlobalProperties.Mapper.class)
    List<ResourceGroupGlobalProperties> getResourceGroupGlobalProperties();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS resource_groups (\n" +
            "  resource_group_id BIGINT NOT NULL AUTO_INCREMENT,\n" +
            "  name VARCHAR(250) NOT NULL,\n" +
            "  soft_memory_limit VARCHAR(128) NOT NULL,\n" +
            "  max_queued INT NOT NULL,\n" +
            "  soft_concurrency_limit INT NULL,\n" +
            "  hard_concurrency_limit INT NOT NULL,\n" +
            "  scheduling_policy VARCHAR(128) NULL,\n" +
            "  scheduling_weight INT NULL,\n" +
            "  jmx_export BOOLEAN NULL,\n" +
            "  soft_cpu_limit VARCHAR(128) NULL,\n" +
            "  hard_cpu_limit VARCHAR(128) NULL,\n" +
            "  queued_time_limit VARCHAR(128) NULL,\n" +
            "  running_time_limit VARCHAR(128) NULL,\n" +
            "  parent BIGINT NULL,\n" +
            "  environment VARCHAR(128) NULL,\n" +
            "  PRIMARY KEY (resource_group_id),\n" +
            "  FOREIGN KEY (parent) REFERENCES resource_groups (resource_group_id)\n" +
            ")")
    void createResourceGroupsTable();

    @SqlQuery("SELECT resource_group_id, name, soft_memory_limit, max_queued, soft_concurrency_limit, " +
            "  hard_concurrency_limit, scheduling_policy, scheduling_weight, jmx_export, soft_cpu_limit, " +
            "  hard_cpu_limit, queued_time_limit, running_time_limit, parent\n" +
            "FROM resource_groups\n" +
            "WHERE environment = :environment\n")
    @Mapper(ResourceGroupSpecBuilder.Mapper.class)
    List<ResourceGroupSpecBuilder> getResourceGroups(@Bind("environment") String environment);

    @SqlQuery("SELECT S.resource_group_id, S.priority, S.user_regex, S.source_regex, S.query_type, S.client_tags\n" +
            "FROM selectors S\n" +
            "JOIN resource_groups R ON (S.resource_group_id = R.resource_group_id)\n" +
            "WHERE R.environment = :environment\n" +
            "ORDER by priority DESC")
    @Mapper(SelectorRecord.Mapper.class)
    List<SelectorRecord> getSelectors(@Bind("environment") String environment);

    @SqlUpdate("CREATE TABLE IF NOT EXISTS selectors (\n" +
            "  resource_group_id BIGINT NOT NULL,\n" +
            "  priority BIGINT NOT NULL,\n" +
            "  user_regex VARCHAR(512),\n" +
            "  source_regex VARCHAR(512),\n" +
            "  query_type VARCHAR(512),\n" +
            "  client_tags VARCHAR(512),\n" +
            "  FOREIGN KEY (resource_group_id) REFERENCES resource_groups (resource_group_id)\n" +
            ")")
    void createSelectorsTable();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS exact_match_source_selectors(\n" +
            "  environment VARCHAR(128) NOT NULL,\n" +
            "  source VARCHAR(512) NOT NULL,\n" +
            "  query_type VARCHAR(512),\n" +
            "  update_time DATETIME NOT NULL,\n" +
            "  resource_group_id VARCHAR(256) NOT NULL,\n" +
            "  PRIMARY KEY (environment, source) \n" +
            ")")
    void createExactMatchSelectorsTable();

    @SqlQuery("SELECT resource_group_id\n" +
            "FROM exact_match_source_selectors\n" +
            "WHERE environment = :environment\n" +
            "  AND source = :source\n" +
            "  AND (query_type IS NULL OR query_type = :query_type)")
    String getExactMatchResourceGroup(
            @Bind("environment") String environment,
            @Bind("source") String source,
            @Bind("query_type") String queryType);
}
