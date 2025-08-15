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
import com.facebook.airlift.configuration.ConfigDescription;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import javax.validation.constraints.NotNull;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.benchmark.source.MySqlBenchmarkSuiteSupplier.MYSQL_BENCHMARK_SUITE_SUPPLIER;
import static com.google.common.base.Preconditions.checkArgument;

public class BenchmarkRunnerConfig
{
    private String testId;
    private String benchmarkSuiteSupplier = MYSQL_BENCHMARK_SUITE_SUPPLIER;

    private Set<String> eventClients = ImmutableSet.of("json");
    private Optional<String> jsonEventLogFile = Optional.empty();

    private boolean continueOnFailure;

    private Optional<Integer> maxConcurrency = Optional.empty();

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

    @NotNull
    public Set<String> getEventClients()
    {
        return eventClients;
    }

    @ConfigDescription("The event client(s) to log the results to")
    @Config("event-clients")
    public BenchmarkRunnerConfig setEventClients(String eventClients)
    {
        this.eventClients = ImmutableSet.copyOf(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(eventClients));
        return this;
    }

    @NotNull
    public Optional<String> getJsonEventLogFile()
    {
        return jsonEventLogFile;
    }

    @ConfigDescription("The file to log json events. Used with event-clients=json. Print to standard output stream if not specified.")
    @Config("json.log-file")
    public BenchmarkRunnerConfig setJsonEventLogFile(String jsonEventLogFile)
    {
        this.jsonEventLogFile = Optional.ofNullable(jsonEventLogFile);
        return this;
    }

    public boolean isContinueOnFailure()
    {
        return continueOnFailure;
    }

    @Config("continue-on-failure")
    public BenchmarkRunnerConfig setContinueOnFailure(boolean continueOnFailure)
    {
        this.continueOnFailure = continueOnFailure;
        return this;
    }

    @NotNull
    public Optional<Integer> getMaxConcurrency()
    {
        return maxConcurrency;
    }

    @ConfigDescription("Default max concurrency for concurrent phases")
    @Config("max-concurrency")
    public BenchmarkRunnerConfig setMaxConcurrency(Integer maxConcurrency)
    {
        checkArgument(maxConcurrency == null || maxConcurrency > 0, "maxConcurrency must be positive");
        this.maxConcurrency = Optional.ofNullable(maxConcurrency);
        return this;
    }
}
