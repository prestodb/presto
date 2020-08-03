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
import java.util.Optional;

import static com.facebook.presto.benchmark.framework.ExecutionStrategy.CONCURRENT;
import static java.util.Objects.requireNonNull;

public class ConcurrentExecutionPhase
        extends PhaseSpecification
{
    private final List<String> queries;
    private Optional<Integer> maxConcurrency;

    @JsonCreator
    public ConcurrentExecutionPhase(String name, List<String> queries, Optional<Integer> maxConcurrency)
    {
        super(name);
        this.queries = ImmutableList.copyOf(queries);
        this.maxConcurrency = requireNonNull(maxConcurrency, "maxConcurrency is null");
    }

    @Override
    public ExecutionStrategy getExecutionStrategy()
    {
        return CONCURRENT;
    }

    @JsonProperty
    public List<String> getQueries()
    {
        return queries;
    }

    @JsonProperty
    public Optional<Integer> getMaxConcurrency()
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
        return Objects.equals(getName(), o.getName()) &&
                Objects.equals(queries, o.queries) &&
                Objects.equals(maxConcurrency, o.maxConcurrency);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getName(), queries, maxConcurrency);
    }
}
