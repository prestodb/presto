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
package com.facebook.presto.verifier;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryResult
{
    public enum State
    {
        INVALID, FAILED, SUCCESS, TOO_MANY_ROWS, TIMEOUT
    }

    private final State state;
    private final Exception exception;
    private final Duration duration;
    private final List<List<Object>> results;

    public QueryResult(State state, Exception exception, Duration duration, List<List<Object>> results)
    {
        this.state = checkNotNull(state, "state is null");
        this.exception = exception;
        this.duration = duration;
        this.results = (results != null) ? ImmutableList.copyOf(results) : null;
    }

    public State getState()
    {
        return state;
    }

    public Exception getException()
    {
        return exception;
    }

    public Duration getDuration()
    {
        return duration;
    }

    public List<List<Object>> getResults()
    {
        return results;
    }
}
