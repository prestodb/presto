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

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class BenchmarkSuiteConfig
{
    private String suite;
    private String suitesTableName = "benchmark_suites";
    private String queriesTableName = "benchmark_queries";

    @NotNull
    public String getSuite()
    {
        return suite;
    }

    @Config("suite")
    public BenchmarkSuiteConfig setSuite(String suite)
    {
        this.suite = suite;
        return this;
    }

    @NotNull
    public String getSuitesTableName()
    {
        return suitesTableName;
    }

    @Config("suites-table-name")
    public BenchmarkSuiteConfig setSuitesTableName(String suitesTableName)
    {
        this.suitesTableName = suitesTableName;
        return this;
    }

    @NotNull
    public String getQueriesTableName()
    {
        return queriesTableName;
    }

    @Config("queries-table-name")
    public BenchmarkSuiteConfig setQueriesTableName(String queriesTableName)
    {
        this.queriesTableName = queriesTableName;
        return this;
    }
}
