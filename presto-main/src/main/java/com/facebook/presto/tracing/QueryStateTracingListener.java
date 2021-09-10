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
package com.facebook.presto.tracing;

import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.spi.tracing.Tracer;

import static java.util.Objects.requireNonNull;

public class QueryStateTracingListener
        implements StateMachine.StateChangeListener<QueryState>
{
    private QueryState oldState;
    private final Tracer tracer;

    public QueryStateTracingListener(Tracer tracer)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    @Override
    public void stateChanged(QueryState newState)
    {
        if (oldState == null) {
            // Initial state condition
            tracer.startBlock(newState.toString(), "");
            oldState = newState;
            return;
        }
        tracer.endBlock(oldState.toString(), "");
        if (newState.isDone()) {
            // Last state
            tracer.addPoint("Query done with state " + newState.toString());
            return;
        }
        else {
            tracer.startBlock(newState.toString(), "");
        }
        oldState = newState;
    }
}
