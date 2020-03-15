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
import com.facebook.airlift.log.Logger;
import com.facebook.presto.benchmark.event.BenchmarkPhaseEvent;
import com.facebook.presto.benchmark.event.BenchmarkQueryEvent;
import com.facebook.presto.benchmark.framework.BenchmarkQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
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
        implements PhaseExecutor
{
    private static final Logger log = Logger.get(ConcurrentPhaseExecutor.class);

    private final String phaseName;
    private final QueryExecutor queryExecutor;
    private final List<BenchmarkQuery> queries;
    private final Set<EventClient> eventClients;
    private final Map<String, String> sessionProperties;
    private final int maxConcurrency;

    private ExecutorService executor;
    private CompletionService<BenchmarkQueryEvent> completionService;

    public ConcurrentPhaseExecutor(
            String phaseName,
            QueryExecutor queryExecutor,
            List<BenchmarkQuery> queries,
            Set<EventClient> eventClients,
            Map<String, String> sessionProperties,
            int maxConcurrency)
    {
        this.phaseName = requireNonNull(phaseName, "phaseName is null");
        this.queryExecutor = requireNonNull(queryExecutor, "benchmarkQueryExecutor is null");
        this.queries = ImmutableList.copyOf(requireNonNull(queries, "queries is null"));
        this.eventClients = requireNonNull(eventClients, "eventClients is null");
        this.sessionProperties = ImmutableMap.copyOf(requireNonNull(sessionProperties, "sessionProperties is null"));
        this.maxConcurrency = maxConcurrency;
    }

    @Override
    public BenchmarkPhaseEvent run(boolean continueOnFailure)
    {
        this.executor = newFixedThreadPool(maxConcurrency);
        this.completionService = new ExecutorCompletionService<>(executor);

        for (BenchmarkQuery query : queries) {
            completionService.submit(() -> queryExecutor.run(query, sessionProperties));
        }

        return reportProgressUntilFinished(queries.size(), continueOnFailure);
    }

    private BenchmarkPhaseEvent reportProgressUntilFinished(int queriesSubmitted, boolean continueOnFailure)
    {
        int completed = 0;
        double lastProgress = 0;
        Map<Status, Integer> statusCount = new EnumMap<>(Status.class);

        while (completed < queriesSubmitted) {
            try {
                BenchmarkQueryEvent event = completionService.take().get();
                completed++;
                statusCount.compute(event.getEventStatus(), (status, count) -> count == null ? 1 : count + 1);
                if (event.getEventStatus() == FAILED && !continueOnFailure) {
                    executor.shutdownNow();
                    return postEvent(BenchmarkPhaseEvent.failed(phaseName, event.getErrorMessage()));
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
                    executor.shutdownNow();
                    return postEvent(BenchmarkPhaseEvent.failed(phaseName, e.toString()));
                }
            }
            catch (ExecutionException e) {
                executor.shutdownNow();
                if (!continueOnFailure) {
                    executor.shutdownNow();
                    return postEvent(BenchmarkPhaseEvent.failed(phaseName, e.toString()));
                }
            }
        }
        if (statusCount.getOrDefault(FAILED, 0) > 0) {
            return postEvent(BenchmarkPhaseEvent.completedWithFailures(phaseName, format("%s out of %s submitted queries failed", statusCount.get(FAILED), queriesSubmitted)));
        }

        return postEvent(BenchmarkPhaseEvent.succeeded(phaseName));
    }

    private BenchmarkPhaseEvent postEvent(BenchmarkPhaseEvent event)
    {
        for (EventClient eventClient : eventClients) {
            eventClient.post(event);
        }
        return event;
    }
}
