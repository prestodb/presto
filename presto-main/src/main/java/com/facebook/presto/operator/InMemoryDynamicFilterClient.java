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
package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.server.DynamicFilterService;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class InMemoryDynamicFilterClient
        implements DynamicFilterClient
{
    private final DynamicFilterService service;
    private final TaskId taskId;
    private final String source;
    private final int driverId;
    private final int expectedDrviersCount;

    public InMemoryDynamicFilterClient(DynamicFilterService service, TaskId taskId, String source, int driverId, int expectedDrviersCount)
    {
        this.service = requireNonNull(service, "dynamicFilterService is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.source = requireNonNull(source, "source is null");
        this.driverId = driverId;
        this.expectedDrviersCount = expectedDrviersCount;
    }

    @Override
    public ListenableFuture<DynamicFilterSummary> getSummary()
    {
        return service.getSummary(taskId.getQueryId().getId(), source);
    }

    @Override
    public ListenableFuture<DynamicFilterSummary> getSummary(String queryId, String source)
    {
        return service.getSummary(queryId, source);
    }

    @Override
    public ListenableFuture<?> storeSummary(DynamicFilterSummary summary)
    {
        service.storeOrMergeSummary(taskId.getQueryId().getId(), source, taskId.getStageExecutionId().getId(), taskId.getId(), driverId, summary, expectedDrviersCount);
        return immediateFuture(null);
    }
}
