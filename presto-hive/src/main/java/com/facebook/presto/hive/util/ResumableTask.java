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

import java.util.concurrent.CompletableFuture;

public interface ResumableTask
{
    /**
     * Process the task either fully, or in part.
     *
     * @return a finished status if the task is complete, otherwise includes a continuation future to indicate
     * when it should be continued to be processed.
     */
    TaskStatus process();

    class TaskStatus
    {
        private final boolean finished;
        private final CompletableFuture<?> continuationFuture;

        private TaskStatus(boolean finished, CompletableFuture<?> continuationFuture)
        {
            this.finished = finished;
            this.continuationFuture = continuationFuture;
        }

        public static TaskStatus finished()
        {
            return new TaskStatus(true, CompletableFuture.completedFuture(null));
        }

        public static TaskStatus continueOn(CompletableFuture<?> continuationFuture)
        {
            return new TaskStatus(false, continuationFuture);
        }

        public boolean isFinished()
        {
            return finished;
        }

        public CompletableFuture<?> getContinuationFuture()
        {
            return continuationFuture;
        }
    }
}
