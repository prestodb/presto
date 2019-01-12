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
package io.prestosql.verifier;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class QueryResult
{
    public enum State
    {
        INVALID, FAILED, SUCCESS, TOO_MANY_ROWS, TIMEOUT, FAILED_TO_SETUP, FAILED_TO_TEARDOWN
    }

    private final State state;
    private final Exception exception;
    private final Duration wallTime;
    private final Duration cpuTime;
    private final String queryId;
    private final List<List<Object>> results;

    public QueryResult(State state, Exception exception, Duration wallTime, Duration cpuTime, String queryId, List<List<Object>> results)
    {
        this.state = requireNonNull(state, "state is null");
        this.exception = exception;
        this.wallTime = wallTime;
        this.cpuTime = cpuTime;
        this.queryId = queryId;
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

    public Duration getWallTime()
    {
        return wallTime;
    }

    public Duration getCpuTime()
    {
        return cpuTime;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public List<List<Object>> getResults()
    {
        return results;
    }

    public void addSuppressed(Throwable throwable)
    {
        checkState(exception != null, "exception is null");
        checkArgument(throwable != null, "throwable is null");
        exception.addSuppressed(throwable);
    }
}
