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

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

public enum QueryState
{
    /**
     * Query has been accepted and is awaiting execution.
     */
    QUEUED(false),
    /**
     * Query is being planned.
     */
    PLANNING(false),
    /**
     * Query execution is being started.
     */
    STARTING(false),
    /**
     * Query has at least one running task.
     */
    RUNNING(false),
    /**
     * Query is finishing (e.g. commit for autocommit queries)
     */
    FINISHING(false),
    /**
     * Query has finished executing and all output has been consumed.
     */
    FINISHED(true),
    /**
     * Query execution failed.
     */
    FAILED(true);

    public static final Set<QueryState> TERMINAL_QUERY_STATES = Stream.of(QueryState.values()).filter(QueryState::isDone).collect(toImmutableSet());

    private final boolean doneState;

    QueryState(boolean doneState)
    {
        this.doneState = doneState;
    }

    /**
     * Is this a terminal state.
     */
    public boolean isDone()
    {
        return doneState;
    }
}
