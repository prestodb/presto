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

import com.facebook.airlift.event.client.EventClient;
import com.facebook.presto.benchmark.event.BenchmarkPhaseEvent;
import com.facebook.presto.benchmark.event.BenchmarkSuiteEvent;
import com.facebook.presto.benchmark.source.BenchmarkSuiteSupplier;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import javax.annotation.PostConstruct;

import java.util.Set;

import static com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status.FAILED;
import static com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status.SUCCEEDED;
import static com.facebook.presto.benchmark.framework.ExecutionStrategy.CONCURRENT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BenchmarkRunner
{
    private final BenchmarkSuiteSupplier benchmarkSuiteSupplier;
    private final ConcurrentPhaseExecutor concurrentPhaseExecutor;
    private final Set<EventClient> eventClients;
    private final boolean continueOnFailure;

    @Inject
    public BenchmarkRunner(
            BenchmarkSuiteSupplier benchmarkSuiteSupplier,
            ConcurrentPhaseExecutor concurrentPhaseExecutor,
            Set<EventClient> eventClients,
            BenchmarkRunnerConfig config)
    {
        this.benchmarkSuiteSupplier = requireNonNull(benchmarkSuiteSupplier, "benchmarkSuiteSupplier is null");
        this.concurrentPhaseExecutor = requireNonNull(concurrentPhaseExecutor, "concurrentPhaseExecutor is null");
        this.eventClients = ImmutableSet.copyOf(requireNonNull(eventClients, "eventClients is null"));
        this.continueOnFailure = config.isContinueOnFailure();
    }

    @PostConstruct
    public void start()
    {
        BenchmarkSuiteEvent suiteEvent = runSuite(benchmarkSuiteSupplier.get());
        eventClients.forEach(client -> client.post(suiteEvent));
    }

    private BenchmarkSuiteEvent runSuite(BenchmarkSuite suite)
    {
        int successfulPhases = 0;

        for (PhaseSpecification phase : suite.getPhases()) {
            BenchmarkPhaseEvent phaseEvent = getPhaseExecutor(phase).runPhase(phase, suite);
            eventClients.forEach(client -> client.post(phaseEvent));

            if (phaseEvent.getEventStatus() == SUCCEEDED) {
                successfulPhases++;
            }
            else if (phaseEvent.getEventStatus() == FAILED && !continueOnFailure) {
                return BenchmarkSuiteEvent.failed(suite.getSuite());
            }
        }

        if (successfulPhases < suite.getPhases().size()) {
            return BenchmarkSuiteEvent.completedWithFailures(suite.getSuite());
        }
        else {
            return BenchmarkSuiteEvent.succeeded(suite.getSuite());
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends PhaseSpecification> PhaseExecutor<T> getPhaseExecutor(T phase)
    {
        if (phase.getExecutionStrategy() == CONCURRENT) {
            return (PhaseExecutor<T>) concurrentPhaseExecutor;
        }

        throw new IllegalArgumentException(format("Unsupported execution strategy: %s", phase.getExecutionStrategy()));
    }
}
