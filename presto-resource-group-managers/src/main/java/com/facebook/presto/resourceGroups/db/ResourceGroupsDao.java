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
            "  max_running INT NOT NULL,\n" +
            "  scheduling_policy VARCHAR(128) NULL,\n" +
            "  scheduling_weight INT NULL,\n" +
            "  jmx_export BOOLEAN NULL,\n" +
            "  soft_cpu_limit VARCHAR(128) NULL,\n" +
            "  hard_cpu_limit VARCHAR(128) NULL,\n" +
            "  parent BIGINT NULL,\n" +
            "  PRIMARY KEY (resource_group_id),\n" +
            "  FOREIGN KEY (parent) REFERENCES resource_groups (resource_group_id)\n" +
            ")")
    void createResourceGroupsTable();

    @SqlQuery("SELECT resource_group_id, name, soft_memory_limit, max_queued, max_running," +
            "  scheduling_policy, scheduling_weight, jmx_export, soft_cpu_limit, hard_cpu_limit, parent\n" +
            "FROM resource_groups")
    @Mapper(ResourceGroupSpecBuilder.Mapper.class)
    List<ResourceGroupSpecBuilder> getResourceGroups();

    @SqlQuery("SELECT resource_group_id, user_regex, source_regex from selectors")
    @Mapper(SelectorRecord.Mapper.class)
    List<SelectorRecord> getSelectors();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS selectors (\n" +
            "  resource_group_id BIGINT NOT NULL,\n" +
            "  user_regex VARCHAR(512),\n" +
            "  source_regex VARCHAR(512),\n" +
            "  FOREIGN KEY (resource_group_id) REFERENCES resource_groups (resource_group_id)\n" +
            ")")
    void createSelectorsTable();
}
