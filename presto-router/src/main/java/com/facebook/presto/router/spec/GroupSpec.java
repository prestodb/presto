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
package com.facebook.presto.router.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class GroupSpec
{
    private final String name;
    private final List<URI> members;
    private final List<Integer> weights;

    @JsonCreator
    public GroupSpec(
            @JsonProperty("name") String name,
            @JsonProperty("members") List<URI> members,
            @JsonProperty("weights") List<Integer> weights)
    {
        this.name = requireNonNull(name, "name is null");
        this.members = ImmutableList.copyOf(requireNonNull(members, "members is null"));
        if (weights != null) {
            this.weights = weights;
        }
        else {
            this.weights = new ArrayList<>(nCopies(members.size(), 1));
        }
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<URI> getMembers()
    {
        return members;
    }

    @JsonProperty
    public List<Integer> getWeights()
    {
        return weights;
    }
}
