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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.execution.SqlStageExecution;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
 * Tracks the completion status of table-finish nodes that write temporary tables for CTE materialization.
 * CTEMaterializationTracker manages a map of materialized CTEs and their associated materialization futures.
 * When a stage includes a CTE table finish, it marks the corresponding CTE as materialized and completes
 * the associated future.
 * This signals the scheduler that some dependency has been resolved, prompting it to resume/continue scheduling.
 */
public class CTEMaterializationTracker
{
    private final Map<String, SettableFuture<Void>> materializationFutures = new ConcurrentHashMap<>();

    public ListenableFuture<Void> getFutureForCTE(String cteName)
    {
        return Futures.nonCancellationPropagating(
                materializationFutures.compute(cteName, (key, existingFuture) -> {
                    if (existingFuture == null) {
                        // Create a new SettableFuture and store it internally
                        return SettableFuture.create();
                    }
                    Preconditions.checkArgument(!existingFuture.isCancelled(),
                            String.format("Error: Existing future was found cancelled in CTEMaterializationTracker for cte", cteName));
                    return existingFuture;
                }));
    }

    public void markCTEAsMaterialized(String cteName)
    {
        materializationFutures.compute(cteName, (key, existingFuture) -> {
            if (existingFuture == null) {
                SettableFuture<Void> completedFuture = SettableFuture.create();
                completedFuture.set(null);
                return completedFuture;
            }
            Preconditions.checkArgument(!existingFuture.isCancelled(),
                    String.format("Error: Existing future was found cancelled in CTEMaterializationTracker for cte", cteName));
            existingFuture.set(null); // Notify all listeners
            return existingFuture;
        });
    }

    public List<ListenableFuture<?>> waitForCteMaterialization(SqlStageExecution stage)
    {
        if (stage.requiresMaterializedCTE()) {
            ImmutableList.Builder<ListenableFuture<?>> blockedFutures = new ImmutableList.Builder<>();
            boolean blocked = false;
            List<String> requiredCTEIds = stage.getRequiredCTEList();
            for (String cteId : requiredCTEIds) {
                ListenableFuture<Void> cteFuture = getFutureForCTE(cteId);
                if (!cteFuture.isDone()) {
                    // Add CTE materialization future to the blocked list
                    blockedFutures.add(cteFuture);
                    blocked = true;
                }
            }
            if (blocked) {
                return blockedFutures.build();
            }
        }
        return ImmutableList.of();
    }
}
