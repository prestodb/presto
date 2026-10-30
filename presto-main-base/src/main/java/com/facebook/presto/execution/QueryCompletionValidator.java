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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.PlanFragment;

/**
 * Coordinator server modules register this hook through Guice multibinding.
 * Implementations may be shared across queries and invoked concurrently on
 * query notification threads. They must be thread safe and avoid blocking I/O.
 */
public interface QueryCompletionValidator
{
    boolean isEnabled(Session session);

    /**
     * Called for each finished or canceled stage using its existing final task
     * snapshots. Canceled stages can contain only work completed before
     * cancellation; stage and task states do not guarantee full-input coverage.
     * Throw a PrestoException with an appropriate error code to reject the
     * query before transaction commit. Normal Presto error classification applies,
     * including codes inherited from causes; unclassified failures are internal errors.
     */
    void validate(Session session, PlanFragment fragment, StageExecutionInfo stageInfo);
}
