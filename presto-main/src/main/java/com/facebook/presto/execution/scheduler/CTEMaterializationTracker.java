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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.HashMap;
import java.util.Map;

/*
 * Tracks the completion status of table-finish nodes that write temporary tables for CTE materialization.
 * CTEMaterializationTracker manages a map of materialized CTEs and their associated materialization futures.
 * When a stage includes a CTE table finish, it marks the corresponding CTE as materialized and completes
 * the associated future. This signals the scheduler that some dependency has been resolved, prompting it
 * to resume scheduling. The scheduler can call the clearAllFutures() method once it starts scheduling again
 */
public class CTEMaterializationTracker
{
    private final Map<String, SettableFuture<Void>> materializationFutures = new HashMap<>();

    private final Map<String, Boolean> materializedCtes = new HashMap<>();

    public synchronized ListenableFuture<Void> getFutureForCTE(String cteName)
    {
        if (hasBeenMaterialized(cteName)) {
            SettableFuture<Void> completedFuture = SettableFuture.create();
            completedFuture.set(null);
            return completedFuture;
        }

        return materializationFutures.compute(cteName, (key, existingFuture) -> {
            if (existingFuture == null) {
                return SettableFuture.create();
            }
            return existingFuture;
        });
    }

    public synchronized void clearAllFutures()
    {
        materializationFutures.clear();
    }

    public synchronized void markCTEAsMaterialized(String cteName)
    {
        if (materializedCtes.putIfAbsent(cteName, true) == null) {
            SettableFuture<Void> future = materializationFutures.get(cteName);
            if (future != null) {
                future.set(null); // Notify all listeners
            }
        }
    }

    private boolean hasBeenMaterialized(String cteName)
    {
        return materializedCtes.containsKey(cteName);
    }
}
