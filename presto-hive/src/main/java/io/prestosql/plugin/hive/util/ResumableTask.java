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
package io.prestosql.plugin.hive.util;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

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
        private final ListenableFuture<?> continuationFuture;

        private TaskStatus(boolean finished, ListenableFuture<?> continuationFuture)
        {
            this.finished = finished;
            this.continuationFuture = continuationFuture;
        }

        public static TaskStatus finished()
        {
            return new TaskStatus(true, Futures.immediateFuture(null));
        }

        public static TaskStatus continueOn(ListenableFuture<?> continuationFuture)
        {
            return new TaskStatus(false, continuationFuture);
        }

        public boolean isFinished()
        {
            return finished;
        }

        public ListenableFuture<?> getContinuationFuture()
        {
            return continuationFuture;
        }
    }
}
