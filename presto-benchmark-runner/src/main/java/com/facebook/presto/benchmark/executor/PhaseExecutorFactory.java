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
package com.facebook.presto.benchmark.executor;

import com.facebook.airlift.event.client.EventClient;
import com.facebook.presto.benchmark.framework.BenchmarkQuery;
import com.facebook.presto.benchmark.framework.BenchmarkSuite;
import com.facebook.presto.benchmark.framework.ConcurrentExecutionPhase;
import com.facebook.presto.benchmark.framework.ExecutionStrategy;
import com.facebook.presto.benchmark.framework.PhaseSpecification;
import com.google.inject.Inject;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PhaseExecutorFactory
{
    private BenchmarkQueryExecutor benchmarkQueryExecutor;
    private Set<EventClient> eventClients;

    @Inject
    public PhaseExecutorFactory(BenchmarkQueryExecutor benchmarkQueryExecutor, Set<EventClient> eventClients)
    {
        this.benchmarkQueryExecutor = requireNonNull(benchmarkQueryExecutor, "benchmarkQueryExecutor is null");
        this.eventClients = requireNonNull(eventClients, "eventClients is null");
    }

    public PhaseExecutor get(PhaseSpecification phaseSpecification, BenchmarkSuite benchmarkSuite)
    {
        ExecutionStrategy executionStrategy = phaseSpecification.getExecutionStrategy();
        switch (executionStrategy) {
            case CONCURRENT:
                ConcurrentExecutionPhase phase = (ConcurrentExecutionPhase) phaseSpecification;
                List<BenchmarkQuery> queryList = phase.getQueries().stream()
                        .map(query -> benchmarkSuite.getQueryMap().get(query))
                        .collect(toImmutableList());

                return new ConcurrentPhaseExecutor(
                        phase.getName(),
                        benchmarkQueryExecutor,
                        queryList,
                        eventClients,
                        benchmarkSuite.getSuiteInfo().getSessionProperties(),
                        phase.getMaxConcurrency());

            default:
                throw new IllegalStateException(format("Unsupported execution strategy type: %s", executionStrategy));
        }
    }
}
