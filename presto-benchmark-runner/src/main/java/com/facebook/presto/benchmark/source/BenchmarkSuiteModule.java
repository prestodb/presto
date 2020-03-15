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

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.benchmark.framework.BenchmarkRunnerConfig;
import com.facebook.presto.benchmark.framework.MySqlBenchmarkSuiteConfig;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.sql.DriverManager;
import java.util.Set;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.presto.benchmark.source.DbBenchmarkSuiteSupplier.BENCHMARK_SUITE_SUPPLIER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.Scopes.SINGLETON;

public class BenchmarkSuiteModule
        extends AbstractConfigurationAwareModule
{
    private final Set<String> supportedBenchmarkSuiteSuppliers;

    public BenchmarkSuiteModule(Set<String> customBenchmarkSuiteSuppliers)
    {
        this.supportedBenchmarkSuiteSuppliers = ImmutableSet.<String>builder()
                .add(BENCHMARK_SUITE_SUPPLIER)
                .addAll(customBenchmarkSuiteSuppliers)
                .build();
    }

    @Override
    protected void setup(Binder binder)
    {
        String benchmarkSuiteSupplier = buildConfigObject(BenchmarkRunnerConfig.class).getBenchmarkSuiteSupplier();
        checkArgument(supportedBenchmarkSuiteSuppliers.contains(benchmarkSuiteSupplier), "Unsupported BenchmarkSuiteSupplier: %s", benchmarkSuiteSupplier);

        if (BENCHMARK_SUITE_SUPPLIER.equals(benchmarkSuiteSupplier)) {
            configBinder(binder).bindConfig(BenchmarkSuiteConfig.class);
            binder.bind(BenchmarkSuiteSupplier.class).to(DbBenchmarkSuiteSupplier.class).in(SINGLETON);
            String database = buildConfigObject(MySqlBenchmarkSuiteConfig.class).getDatabaseUrl();
            binder.bind(Jdbi.class).toInstance(Jdbi.create(() -> DriverManager.getConnection(database)).installPlugin(new SqlObjectPlugin()));
        }
    }
}
