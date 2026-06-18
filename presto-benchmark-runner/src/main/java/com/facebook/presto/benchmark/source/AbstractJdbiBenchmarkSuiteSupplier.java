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
import org.jdbi.v3.core.Jdbi;

import static java.util.Objects.requireNonNull;

public abstract class AbstractJdbiBenchmarkSuiteSupplier
        implements BenchmarkSuiteSupplier
{
    private final Jdbi jdbi;
    private final String suitesTableName;
    private final String queriesTableName;
    private final String suite;

    public AbstractJdbiBenchmarkSuiteSupplier(Jdbi jdbi, BenchmarkSuiteConfig config)
    {
        this.jdbi = requireNonNull(jdbi, "jdbi is null");
        this.suitesTableName = requireNonNull(config.getSuitesTableName(), "suitesTableName is null");
        this.queriesTableName = requireNonNull(config.getQueriesTableName(), "queriesTableName is null");
        this.suite = requireNonNull(config.getSuite(), "suite is null");
    }

    @Override
    public BenchmarkSuite get()
    {
        return jdbi.inTransaction(handle -> {
            BenchmarkSuiteDao dao = handle.attach(BenchmarkSuiteDao.class);
            BenchmarkSuite.JdbiBuilder suiteBuilder = dao.getBenchmarkSuite(suitesTableName, suite);
            return suiteBuilder.setQueries(dao.getBenchmarkQueries(queriesTableName, suiteBuilder.getQuerySet())).build();
        });
    }
}
