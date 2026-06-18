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
package com.facebook.presto.benchmark.source;

import com.facebook.presto.benchmark.framework.BenchmarkQuery;
import com.facebook.presto.benchmark.framework.BenchmarkSuite;
import org.jdbi.v3.sqlobject.config.RegisterColumnMapper;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.Define;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;

@RegisterColumnMapper(StringToStringMapColumnMapper.class)
@RegisterColumnMapper(PhaseSpecificationsColumnMapper.class)
public interface BenchmarkSuiteDao
{
    @SqlUpdate("CREATE TABLE <table_name> (\n" +
            "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n" +
            "  `suite` varchar(256) NOT NULL,\n" +
            "  `query_set` varchar(256) NOT NULL,\n" +
            "  `phases` mediumtext NOT NULL,\n" +
            "  `session_properties` mediumtext DEFAULT NULL,\n" +
            "  `created_by` varchar(256) NOT NULL,\n" +
            "  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
            "  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  UNIQUE KEY suite (`suite`))")
    void createBenchmarkSuitesTable(
            @Define("table_name") String tableName);

    @SqlUpdate("CREATE TABLE <table_name> (\n" +
            "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n" +
            "  `query_set` varchar(256) NOT NULL,\n" +
            "  `name` varchar(256) NOT NULL,\n" +
            "  `catalog` varchar(256) NOT NULL,\n" +
            "  `schema` varchar(256) NOT NULL,\n" +
            "  `query` mediumtext NOT NULL,\n" +
            "  `session_properties` mediumtext DEFAULT NULL,\n" +
            "  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
            "  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
            "  PRIMARY KEY (`id`),\n" +
            "  UNIQUE KEY `query_set_name` (`query_set`,`name`))")
    void createBenchmarkQueriesTable(
            @Define("table_name") String tableName);

    @SqlQuery("SELECT\n" +
            "  `suite`,\n" +
            "  `query_set`,\n" +
            "  `phases`,\n" +
            "  `session_properties`\n" +
            "FROM\n" +
            "  <table_name>\n" +
            "WHERE\n" +
            "  `suite`  = :suite\n")
    @RegisterConstructorMapper(BenchmarkSuite.JdbiBuilder.class)
    BenchmarkSuite.JdbiBuilder getBenchmarkSuite(
            @Define("table_name") String tableName,
            @Bind("suite") String suite);

    @SqlQuery("SELECT\n" +
            "  `name`,\n" +
            "  `catalog`,\n" +
            "  `schema`,\n" +
            "  `query`,\n" +
            "  `session_properties`\n" +
            "FROM\n" +
            "  <table_name>\n" +
            "WHERE\n" +
            "  `query_set`  = :query_set\n")
    @RegisterConstructorMapper(BenchmarkQuery.class)
    List<BenchmarkQuery> getBenchmarkQueries(
            @Define("table_name") String tableName,
            @Bind("query_set") String querySet);
}
