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

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
 *    Tracks whether tablefinish nodes writing temporary tables for CTE Materialization are complete
 */
@ThreadSafe
public class CTEMaterializationTracker
{
    private final Map<String, SettableFuture<Void>> materializationFutures = new ConcurrentHashMap<>();

    private final Map<String, Boolean> materializedCtes = new ConcurrentHashMap<>();

    public ListenableFuture<Void> getFutureForCTE(String cteName)
    {
        return materializationFutures.compute(cteName, (key, existingFuture) -> {
            if (existingFuture == null || existingFuture.isCancelled()) {
                return SettableFuture.create();
            }
            return existingFuture;
        });
    }

    public void markCTEAsMaterialized(String cteName)
    {
        if (materializedCtes.putIfAbsent(cteName, true) == null) {
            SettableFuture<Void> future = materializationFutures.get(cteName);
            if (future != null && !future.isCancelled()) {
                future.set(null); // Notify all listeners
            }
        }
    }

    public synchronized boolean hasBeenMaterialized(String cteName)
    {
        return materializedCtes.containsKey(cteName);
    }
}
