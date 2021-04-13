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

import com.facebook.airlift.log.Logger;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.Executor;

public final class ResumableTasks
{
    private static final Logger log = Logger.get(ResumableTasks.class);

    private ResumableTasks() {}

    public static ListenableFuture<Void> submit(Executor executor, ResumableTask task)
    {
        SettableFuture<Void> completionFuture = SettableFuture.create();
        executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                ResumableTask.TaskStatus status;
                try {
                    status = task.process();
                }
                catch (Throwable t) {
                    log.error(t, "ResumableTask completed exceptionally");
                    completionFuture.setException(t);
                    return;
                }

                if (status.isFinished()) {
                    completionFuture.set(null);
                    return;
                }

                // if task is not complete, schedule it it to run again when the future finishes
                status.getContinuationFuture().addListener(this, executor);
            }
        });
        return completionFuture;
    }
}
