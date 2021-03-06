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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class JsonRenderer
        implements Renderer<String>
{
    private static final JsonCodec<JsonRenderedNode> CODEC = JsonCodec.jsonCodec(JsonRenderedNode.class);
    private static final JsonCodec<Map<PlanFragmentId, JsonPlanFragment>> PLAN_MAP_CODEC = JsonCodec.mapJsonCodec(PlanFragmentId.class, JsonPlanFragment.class);

    @Override
    public String render(PlanRepresentation plan)
    {
        return CODEC.toJson(renderJson(plan, plan.getRoot()));
    }

    public String render(Map<PlanFragmentId, JsonPlanFragment> fragmentJsonMap)
    {
        return PLAN_MAP_CODEC.toJson(fragmentJsonMap);
    }

    private JsonRenderedNode renderJson(PlanRepresentation plan, NodeRepresentation node)
    {
        List<JsonRenderedNode> children = node.getChildren().stream()
                .map(plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(n -> renderJson(plan, n))
                .collect(toImmutableList());

        return new JsonRenderedNode(
                node.getId().toString(),
                node.getName(),
                node.getIdentifier(),
                node.getDetails(),
                children,
                node.getRemoteSources().stream()
                        .map(PlanFragmentId::toString)
                        .collect(toImmutableList()));
    }

    public static class JsonRenderedNode
    {
        private final String id;
        private final String name;
        private final String identifier;
        private final String details;
        private final List<JsonRenderedNode> children;
        private final List<String> remoteSources;

        @JsonCreator
        public JsonRenderedNode(String id, String name, String identifier, String details, List<JsonRenderedNode> children, List<String> remoteSources)
        {
            this.id = requireNonNull(id, "id is null");
            this.name = requireNonNull(name, "name is null");
            this.identifier = requireNonNull(identifier, "identifier is null");
            this.details = requireNonNull(details, "details is null");
            this.children = requireNonNull(children, "children is null");
            this.remoteSources = requireNonNull(remoteSources, "id is null");
        }

        @JsonProperty
        public String getId()
        {
            return id;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public String getIdentifier()
        {
            return identifier;
        }

        @JsonProperty
        public String getDetails()
        {
            return details;
        }

        @JsonProperty
        public List<JsonRenderedNode> getChildren()
        {
            return children;
        }

        @JsonProperty
        public List<String> getRemoteSources()
        {
            return remoteSources;
        }
    }

    public static class JsonPlanFragment
    {
        @JsonRawValue
        private final String plan;

        @JsonCreator
        public JsonPlanFragment(String plan)
        {
            this.plan = plan;
        }

        @JsonProperty
        public String getPlan()
        {
            return this.plan;
        }
    }
}
