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
package com.facebook.presto.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.SetThreadName;

import java.util.List;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

public class QueuedExecution
{
    private final QueryExecution queryExecution;
    private final List<QueryQueue> nextQueues;
    private final ListenableFuture<?> listenableFuture;
    private final Executor executor;
    private final SqlQueryManagerStats stats;

    public static QueuedExecution createQueuedExecution(QueryExecution queryExecution, List<QueryQueue> nextQueues, Executor executor, SqlQueryManagerStats stats)
    {
        SettableFuture<?> settableFuture = SettableFuture.create();
        SqlQueryManager.addCompletionCallback(queryExecution, () -> settableFuture.set(null));
        return new QueuedExecution(queryExecution, nextQueues, executor, stats, settableFuture);
    }

    private QueuedExecution(QueryExecution queryExecution, List<QueryQueue> nextQueues, Executor executor, SqlQueryManagerStats stats, ListenableFuture<?> listenableFuture)
    {
        this.queryExecution = requireNonNull(queryExecution, "queryExecution is null");
        this.nextQueues = ImmutableList.copyOf(requireNonNull(nextQueues, "nextQueues is null"));
        this.executor = requireNonNull(executor, "executor is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.listenableFuture = requireNonNull(listenableFuture, "listenableFuture is null");
    }

    public ListenableFuture<?> getCompletionFuture()
    {
        return listenableFuture;
    }

    public void start()
    {
        // Only execute if the query is not already completed (e.g. cancelled)
        if (listenableFuture.isDone()) {
            return;
        }
        if (nextQueues.isEmpty()) {
            executor.execute(() -> {
                try (SetThreadName ignored = new SetThreadName("Query-%s", queryExecution.getQueryId())) {
                    stats.queryStarted();
                    listenableFuture.addListener(stats::queryStopped, MoreExecutors.directExecutor());

                    queryExecution.start();
                }
            });
        }
        else {
            nextQueues.get(0).enqueue(new QueuedExecution(queryExecution, nextQueues.subList(1, nextQueues.size()), executor, stats, listenableFuture));
        }
    }
}
