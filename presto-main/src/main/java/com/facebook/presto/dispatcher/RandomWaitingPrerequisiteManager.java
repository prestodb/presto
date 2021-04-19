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
package com.facebook.presto.dispatcher;

import com.facebook.presto.spi.dispatcher.PrerequisiteManager;
import com.facebook.presto.spi.dispatcher.QueryPrerequisiteContext;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RandomWaitingPrerequisiteManager
        implements PrerequisiteManager
{
    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    @Override
    public CompletableFuture<?> waitForPrerequisites(QueryPrerequisiteContext context)
    {
        CompletableFuture<?> future = new CompletableFuture<>();
        scheduledExecutor.schedule(() -> future.completeExceptionally(new Exception("foo")), 10, TimeUnit.SECONDS);
        return future;
    }
}
