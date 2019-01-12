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
package io.prestosql.execution.scheduler.group;

import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.scheduler.SourceScheduler;

public interface LifespanScheduler
{
    // Thread Safety:
    // * Invocation of onLifespanFinished can be parallel and in any thread.
    //   There may be multiple invocations in flight at the same time,
    //   and may overlap with any other methods.
    // * Invocation of schedule happens sequentially in a single thread.
    // * This object is safely published after invoking scheduleInitial.

    void scheduleInitial(SourceScheduler scheduler);

    void onLifespanFinished(Iterable<Lifespan> newlyCompletedDriverGroups);

    SettableFuture schedule(SourceScheduler scheduler);
}
