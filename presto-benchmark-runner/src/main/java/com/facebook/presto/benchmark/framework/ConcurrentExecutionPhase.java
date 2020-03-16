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
package com.facebook.presto.benchmark.framework;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ConcurrentExecutionPhase
        extends PhaseSpecification
{
    private final ExecutionStrategy executionStrategy;
    private final List<String> queries;
    private int maxConcurrency = 50;

    @JsonCreator
    public ConcurrentExecutionPhase(String name, ExecutionStrategy executionStrategy, List<String> queries, int maxConcurrency)
    {
        super(name);
        this.executionStrategy = requireNonNull(executionStrategy, "executionStrategy is null");
        this.queries = requireNonNull(ImmutableList.copyOf(queries), "queries is null");
        this.maxConcurrency = maxConcurrency;
    }

    @JsonProperty
    @Override
    public ExecutionStrategy getExecutionStrategy()
    {
        return executionStrategy;
    }

    @JsonProperty
    public List<String> getQueries()
    {
        return queries;
    }

    @JsonProperty
    public int getMaxConcurrency()
    {
        return maxConcurrency;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ConcurrentExecutionPhase o = (ConcurrentExecutionPhase) obj;
        return Objects.equals(getExecutionStrategy(), o.getExecutionStrategy()) &&
                Objects.equals(getName(), o.getName()) &&
                Objects.equals(queries, o.queries);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getExecutionStrategy(), getName(), queries);
    }
}
