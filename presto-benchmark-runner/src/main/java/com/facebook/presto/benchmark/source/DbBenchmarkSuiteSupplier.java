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

import com.facebook.presto.benchmark.framework.BenchmarkSuite;
import com.facebook.presto.benchmark.framework.BenchmarkSuiteInfo;
import com.google.inject.Inject;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import static java.util.Objects.requireNonNull;

public class DbBenchmarkSuiteSupplier
        implements BenchmarkSuiteSupplier
{
    public static final String BENCHMARK_SUITE_SUPPLIER = "mysql";

    private final Jdbi jdbi;
    private final String suitesTableName;
    private final String queriesTableName;
    private final String suite;

    @Inject
    public DbBenchmarkSuiteSupplier(Jdbi jdbi, BenchmarkSuiteConfig config)
    {
        this.jdbi = requireNonNull(jdbi, "jdbi is null");
        this.suitesTableName = requireNonNull(config.getSuitesTableName(), "suites-table-name is null");
        this.queriesTableName = requireNonNull(config.getQueriesTableName(), "queries-table-name is null");
        this.suite = requireNonNull(config.getSuite(), "suite name is null");
    }

    @Override
    public BenchmarkSuite get()
    {
        BenchmarkSuite benchmarkSuite;
        try (Handle handle = jdbi.open()) {
            BenchmarkSuiteDao benchmarkDao = handle.attach(BenchmarkSuiteDao.class);
            BenchmarkSuiteInfo benchmarkSuiteInfo = benchmarkDao.getBenchmarkSuiteInfo(suitesTableName, suite);
            benchmarkSuite = new BenchmarkSuite(suite, benchmarkSuiteInfo, benchmarkDao.getBenchmarkQueries(queriesTableName, benchmarkSuiteInfo.getQuerySet()));
        }
        return benchmarkSuite;
    }
}
