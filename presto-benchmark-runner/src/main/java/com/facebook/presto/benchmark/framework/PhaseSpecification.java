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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = StreamExecutionPhase.class, name = "stream"),
        @JsonSubTypes.Type(value = ConcurrentExecutionPhase.class, name = "concurrent")})
public abstract class PhaseSpecification
{
    private final String name;

    public abstract ExecutionStrategy getExecutionStrategy();

    public PhaseSpecification(String name)
    {
        this.name = requireNonNull(name, "name is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }
}
