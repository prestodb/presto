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
package com.facebook.presto.verifier.source;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.validation.constraints.NotNull;

import java.util.List;

public class MySqlSourceQueryConfig
{
    private String database;
    private String tableName = "verifier_queries";
    private List<String> suites = ImmutableList.of();
    private int maxQueriesPerSuite = 100_000;

    @NotNull
    public String getDatabase()
    {
        return database;
    }

    @Config("database")
    public MySqlSourceQueryConfig setDatabase(String database)
    {
        this.database = database;
        return this;
    }

    @NotNull
    public String getTableName()
    {
        return tableName;
    }

    @Config("table-name")
    public MySqlSourceQueryConfig setTableName(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    @NotNull
    public List<String> getSuites()
    {
        return suites;
    }

    @ConfigDescription("The suites of queries in the query database to run")
    @Config("suites")
    public MySqlSourceQueryConfig setSuites(String suites)
    {
        if (suites != null) {
            this.suites = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(suites);
        }
        return this;
    }

    public int getMaxQueriesPerSuite()
    {
        return maxQueriesPerSuite;
    }

    @ConfigDescription("The maximum number of queries to run for each suite")
    @Config("max-queries-per-suite")
    public MySqlSourceQueryConfig setMaxQueriesPerSuite(int maxQueriesPerSuite)
    {
        this.maxQueriesPerSuite = maxQueriesPerSuite;
        return this;
    }
}
