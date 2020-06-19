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
import com.facebook.presto.benchmark.executor.PhaseExecutor;
import com.facebook.presto.benchmark.executor.PhaseExecutorFactory;
import com.facebook.presto.benchmark.source.BenchmarkSuiteSupplier;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import javax.annotation.PostConstruct;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status.FAILED;
import static com.facebook.presto.benchmark.event.BenchmarkPhaseEvent.Status.SUCCEEDED;
import static java.util.Objects.requireNonNull;

public class BenchmarkRunner
{
    private final BenchmarkSuiteSupplier benchmarkSuiteSupplier;
    private final PhaseExecutorFactory phaseExecutorFactory;
    private final Set<EventClient> eventClients;
    private final boolean continueOnFailure;

    @Inject
    public BenchmarkRunner(
            BenchmarkSuiteSupplier benchmarkSuiteSupplier,
            PhaseExecutorFactory phaseExecutorFactory,
            Set<EventClient> eventClients,
            BenchmarkRunnerConfig benchmarkRunnerConfig)
    {
        this.benchmarkSuiteSupplier = requireNonNull(benchmarkSuiteSupplier, "benchmarkSuiteSupplier is null");
        this.phaseExecutorFactory = requireNonNull(phaseExecutorFactory, "phaseExecutorFactory is null");
        this.eventClients = ImmutableSet.copyOf(requireNonNull(eventClients, "eventClients is null"));
        this.continueOnFailure = requireNonNull(benchmarkRunnerConfig, "benchmarkRunnerConfig is null").isContinueOnFailure();
    }

    @PostConstruct
    public void start()
    {
        BenchmarkSuite suite = benchmarkSuiteSupplier.get();
        List<PhaseSpecification> phases = suite.getPhases();
        int successfulPhases = 0;

        for (PhaseSpecification phase : phases) {
            PhaseExecutor phaseExecutor = phaseExecutorFactory.get(phase, suite);
            BenchmarkPhaseEvent phaseEvent = phaseExecutor.run(continueOnFailure);
            if (phaseEvent.getEventStatus() == SUCCEEDED) {
                successfulPhases++;
            }
            else if (phaseEvent.getEventStatus() == FAILED && !continueOnFailure) {
                postEvent(BenchmarkSuiteEvent.failed(suite.getSuite()));
                break;
            }
        }

        if (successfulPhases < phases.size()) {
            postEvent(BenchmarkSuiteEvent.completedWithFailures(suite.getSuite()));
        }
        else {
            postEvent(BenchmarkSuiteEvent.succeeded(suite.getSuite()));
        }
    }

    private void postEvent(BenchmarkSuiteEvent event)
    {
        for (EventClient eventClient : eventClients) {
            eventClient.post(event);
        }
    }
}
