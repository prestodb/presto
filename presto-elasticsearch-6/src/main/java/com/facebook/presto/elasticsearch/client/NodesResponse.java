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
package com.facebook.presto.elasticsearch.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class NodesResponse
{
    private final Map<String, Node> nodes;

    @JsonCreator
    public NodesResponse(@JsonProperty("nodes") Map<String, Node> nodes)
    {
        requireNonNull(nodes, "nodes is null");

        this.nodes = ImmutableMap.copyOf(nodes);
    }

    public Map<String, Node> getNodes()
    {
        return nodes;
    }

    public static class Node
    {
        private final Set<String> roles;
        private final Optional<Http> http;

        @JsonCreator
        public Node(
                @JsonProperty("roles") Set<String> roles,
                @JsonProperty("http") Optional<Http> http)
        {
            this.roles = ImmutableSet.copyOf(roles);
            this.http = requireNonNull(http, "http is null");
        }

        public Set<String> getRoles()
        {
            return roles;
        }

        public Optional<String> getAddress()
        {
            return http.map(Http::getAddress);
        }
    }

    public static class Http
    {
        private final String address;

        @JsonCreator
        public Http(@JsonProperty("publish_address") String address)
        {
            this.address = address;
        }

        public String getAddress()
        {
            return address;
        }

        @Override
        public String toString()
        {
            return address;
        }
    }
}
