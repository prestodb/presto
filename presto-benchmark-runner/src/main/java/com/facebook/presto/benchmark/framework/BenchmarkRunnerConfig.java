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
package com.facebook.presto.benchmark.framework;

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import static com.facebook.presto.benchmark.source.DbBenchmarkSuiteSupplier.BENCHMARK_SUITE_SUPPLIER;

public class BenchmarkRunnerConfig
{
    private String testId;
    private String benchmarkSuiteSupplier = BENCHMARK_SUITE_SUPPLIER;

    @NotNull
    public String getTestId()
    {
        return testId;
    }

    @Config("test-id")
    public BenchmarkRunnerConfig setTestId(String testId)
    {
        this.testId = testId;
        return this;
    }

    @NotNull
    public String getBenchmarkSuiteSupplier()
    {
        return benchmarkSuiteSupplier;
    }

    @Config("benchmark-suite-supplier")
    public BenchmarkRunnerConfig setBenchmarkSuiteSupplier(String benchmarkSuiteSupplier)
    {
        this.benchmarkSuiteSupplier = benchmarkSuiteSupplier;
        return this;
    }
}
