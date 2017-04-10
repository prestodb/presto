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
package com.facebook.presto.hive.util;

import io.airlift.log.Logger;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

public final class ResumableTasks
{
    private static final Logger log = Logger.get(ResumableTasks.class);

    private ResumableTasks()
    {
    }

    public static void submit(Executor executor, ResumableTask task)
    {
        AtomicReference<Runnable> runnableReference = new AtomicReference<>();
        Runnable runnable = () -> {
            ResumableTask.TaskStatus status = safeProcessTask(task);
            if (!status.isFinished()) {
                status.getContinuationFuture().thenRun(() -> executor.execute(runnableReference.get()));
            }
        };
        runnableReference.set(runnable);
        executor.execute(runnable);
    }

    private static ResumableTask.TaskStatus safeProcessTask(ResumableTask task)
    {
        try {
            return task.process();
        }
        catch (Throwable t) {
            log.warn(t, "ResumableTask completed exceptionally");
            return ResumableTask.TaskStatus.finished();
        }
    }
}
