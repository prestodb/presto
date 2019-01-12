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
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

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
    @UseRowMapper(ResourceGroupGlobalProperties.Mapper.class)
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
            "  parent BIGINT NULL,\n" +
            "  environment VARCHAR(128) NULL,\n" +
            "  PRIMARY KEY (resource_group_id),\n" +
            "  FOREIGN KEY (parent) REFERENCES resource_groups (resource_group_id)\n" +
            ")")
    void createResourceGroupsTable();

    @SqlQuery("SELECT resource_group_id, name, soft_memory_limit, max_queued, soft_concurrency_limit, " +
            "  hard_concurrency_limit, scheduling_policy, scheduling_weight, jmx_export, soft_cpu_limit, " +
            "  hard_cpu_limit, parent\n" +
            "FROM resource_groups\n" +
            "WHERE environment = :environment\n")
    @UseRowMapper(ResourceGroupSpecBuilder.Mapper.class)
    List<ResourceGroupSpecBuilder> getResourceGroups(@Bind("environment") String environment);

    @SqlQuery("SELECT S.resource_group_id, S.priority, S.user_regex, S.source_regex, S.query_type, S.client_tags, S.selector_resource_estimate\n" +
            "FROM selectors S\n" +
            "JOIN resource_groups R ON (S.resource_group_id = R.resource_group_id)\n" +
            "WHERE R.environment = :environment\n" +
            "ORDER by priority DESC")
    @UseRowMapper(SelectorRecord.Mapper.class)
    List<SelectorRecord> getSelectors(@Bind("environment") String environment);

    @SqlUpdate("CREATE TABLE IF NOT EXISTS selectors (\n" +
            "  resource_group_id BIGINT NOT NULL,\n" +
            "  priority BIGINT NOT NULL,\n" +
            "  user_regex VARCHAR(512),\n" +
            "  source_regex VARCHAR(512),\n" +
            "  query_type VARCHAR(512),\n" +
            "  client_tags VARCHAR(512),\n" +
            "  selector_resource_estimate VARCHAR(1024),\n" +
            "  FOREIGN KEY (resource_group_id) REFERENCES resource_groups (resource_group_id)\n" +
            ")")
    void createSelectorsTable();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS exact_match_source_selectors(\n" +
            "  environment VARCHAR(128),\n" +
            "  source VARCHAR(512) NOT NULL,\n" +
            "  query_type VARCHAR(512),\n" +
            "  update_time DATETIME NOT NULL,\n" +
            "  resource_group_id VARCHAR(256) NOT NULL,\n" +
            "  PRIMARY KEY (environment, source, query_type),\n" +
            "  UNIQUE (source, environment, query_type, resource_group_id)\n" +
            ")")
    void createExactMatchSelectorsTable();

    /**
     * Returns the most specific exact-match selector for a given environment, source and query type.
     * NULL values in the environment and query type fields signify wildcards.
     */
    @SqlQuery("SELECT resource_group_id\n" +
            "FROM exact_match_source_selectors\n" +
            "WHERE source = :source\n" +
            "  AND (environment = :environment OR environment IS NULL)\n" +
            "  AND (query_type = :query_type OR query_type IS NULL)\n" +
            "ORDER BY environment IS NULL, query_type IS NULL\n" +
            "LIMIT 1")
    String getExactMatchResourceGroup(
            @Bind("environment") String environment,
            @Bind("source") String source,
            @Bind("query_type") String queryType);
}
