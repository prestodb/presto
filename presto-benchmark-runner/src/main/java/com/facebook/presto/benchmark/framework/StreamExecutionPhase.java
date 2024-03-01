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

import static com.facebook.presto.benchmark.framework.ExecutionStrategy.STREAM;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class StreamExecutionPhase
        extends PhaseSpecification
{
    private final List<List<String>> streams;

    @JsonCreator
    public StreamExecutionPhase(String name, List<List<String>> streams)
    {
        super(name);
        this.streams = streams.stream()
                .map(ImmutableList::copyOf)
                .collect(toImmutableList());
    }

    @Override
    public ExecutionStrategy getExecutionStrategy()
    {
        return STREAM;
    }

    @JsonProperty
    public List<List<String>> getStreams()
    {
        return streams;
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
        StreamExecutionPhase o = (StreamExecutionPhase) obj;
        return Objects.equals(getName(), o.getName()) &&
                Objects.equals(streams, o.streams);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getName(), streams);
    }
}
