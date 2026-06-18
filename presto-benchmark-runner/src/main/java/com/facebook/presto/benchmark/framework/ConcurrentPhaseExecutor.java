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
import com.facebook.airlift.log.Logger;
import com.facebook.presto.benchmark.event.BenchmarkPhaseEvent;
import com.facebook.presto.benchmark.event.BenchmarkQueryEvent;
import com.facebook.presto.benchmark.prestoaction.PrestoActionFactory;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.google.inject.Inject;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.benchmark.event.BenchmarkQueryEvent.Status;
import static com.facebook.presto.benchmark.event.BenchmarkQueryEvent.Status.FAILED;
import static com.facebook.presto.benchmark.event.BenchmarkQueryEvent.Status.SUCCEEDED;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ConcurrentPhaseExecutor
        extends AbstractPhaseExecutor<ConcurrentExecutionPhase>
{
    private static final int DEFAULT_MAX_CONCURRENCY = 70;
    private static final Logger log = Logger.get(ConcurrentPhaseExecutor.class);

    private final boolean continueOnFailure;
    private final Optional<Integer> maxConcurrency;

    @Inject
    public ConcurrentPhaseExecutor(
            SqlParser sqlParser,
            ParsingOptions parsingOptions,
            PrestoActionFactory prestoActionFactory,
            Set<EventClient> eventClients,
            BenchmarkRunnerConfig config)
    {
        super(sqlParser, parsingOptions, prestoActionFactory, eventClients, config.getTestId());
        this.continueOnFailure = config.isContinueOnFailure();
        this.maxConcurrency = requireNonNull(config.getMaxConcurrency(), "maxConcurrency is null");
    }

    @Override
    public BenchmarkPhaseEvent runPhase(ConcurrentExecutionPhase phase, BenchmarkSuite suite)
    {
        int maxConcurrency = this.maxConcurrency.orElseGet(() -> phase.getMaxConcurrency().orElse(DEFAULT_MAX_CONCURRENCY));
        log.info("Starting concurrent phase '%s' with max concurrency %s", phase.getName(), maxConcurrency);

        ExecutorService executor = newFixedThreadPool(maxConcurrency);

        try {
            CompletionService<BenchmarkQueryEvent> completionService = new ExecutorCompletionService<>(executor);

            for (String queryName : phase.getQueries()) {
                BenchmarkQuery benchmarkQuery = overrideSessionProperties(suite.getQueries().get(queryName), suite.getSessionProperties());
                completionService.submit(() -> runQuery(benchmarkQuery));
            }

            return reportProgressUntilFinished(phase, completionService);
        }
        finally {
            executor.shutdownNow();
        }
    }

    private BenchmarkPhaseEvent reportProgressUntilFinished(
            ConcurrentExecutionPhase phase,
            CompletionService<BenchmarkQueryEvent> completionService)
    {
        String phaseName = phase.getName();
        int completed = 0;
        double lastProgress = 0;
        int queriesSubmitted = phase.getQueries().size();
        Map<Status, Integer> statusCount = new EnumMap<>(Status.class);

        while (completed < queriesSubmitted) {
            try {
                BenchmarkQueryEvent event = completionService.take().get();
                postEvent(event);
                completed++;
                statusCount.compute(event.getEventStatus(), (status, count) -> count == null ? 1 : count + 1);
                if (event.getEventStatus() == FAILED && !continueOnFailure) {
                    return BenchmarkPhaseEvent.failed(phaseName, event.getErrorMessage());
                }

                double progress = ((double) completed) / queriesSubmitted * 100;
                if (progress - lastProgress > 0.5 || completed == queriesSubmitted) {
                    log.info("Progress: %s succeeded, %s failed, %s submitted, %.2f%% done",
                            statusCount.getOrDefault(SUCCEEDED, 0),
                            statusCount.getOrDefault(FAILED, 0),
                            queriesSubmitted,
                            progress);
                    lastProgress = progress;
                }
            }
            catch (InterruptedException e) {
                currentThread().interrupt();
                if (!continueOnFailure) {
                    return BenchmarkPhaseEvent.failed(phaseName, e.toString());
                }
            }
            catch (ExecutionException e) {
                if (!continueOnFailure) {
                    return BenchmarkPhaseEvent.failed(phaseName, e.toString());
                }
            }
        }
        if (statusCount.getOrDefault(FAILED, 0) > 0) {
            return BenchmarkPhaseEvent.completedWithFailures(phaseName, format("%s out of %s submitted queries failed", statusCount.get(FAILED), queriesSubmitted));
        }

        return BenchmarkPhaseEvent.succeeded(phaseName);
    }
}
