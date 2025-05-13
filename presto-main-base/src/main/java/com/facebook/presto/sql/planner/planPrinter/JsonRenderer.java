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
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.Serialization;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class JsonRenderer
        implements Renderer<String>
{
    private final JsonCodec<Map<PlanFragmentId, JsonPlanFragment>> planMapCodec;
    private final JsonCodec<JsonRenderedNode> codec;
    private final JsonCodec<Map<PlanFragmentId, JsonPlan>> deserializationCodec;

    public JsonRenderer(FunctionAndTypeManager functionAndTypeManager)
    {
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionSerializer()));
        provider.setKeyDeserializers(ImmutableMap.of(VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionDeserializer(functionAndTypeManager)));

        JsonCodecFactory codecFactory = new JsonCodecFactory(provider, true);
        this.codec = codecFactory.jsonCodec(JsonRenderedNode.class);
        this.planMapCodec = codecFactory.mapJsonCodec(PlanFragmentId.class, JsonPlanFragment.class);
        this.deserializationCodec = codecFactory.mapJsonCodec(PlanFragmentId.class, JsonPlan.class);
    }

    public Map<PlanFragmentId, JsonPlan> deserialize(String serialized)
    {
        return deserializationCodec.fromJson(serialized);
    }

    @Override
    public String render(PlanRepresentation plan)
    {
        return codec.toJson(renderJson(plan, plan.getRoot()));
    }

    public String render(Map<PlanFragmentId, JsonPlanFragment> fragmentJsonMap)
    {
        return planMapCodec.toJson(fragmentJsonMap);
    }

    @VisibleForTesting
    public JsonRenderedNode renderJson(PlanRepresentation plan, NodeRepresentation node)
    {
        List<JsonRenderedNode> children = node.getChildren().stream()
                .map(plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(n -> renderJson(plan, n))
                .collect(toImmutableList());

        return new JsonRenderedNode(
                node.getSourceLocation(),
                node.getId().toString(),
                node.getName(),
                node.getIdentifier(),
                node.getDetails(),
                children,
                node.getRemoteSources().stream()
                        .map(PlanFragmentId::toString)
                        .collect(toImmutableList()),
                node.getEstimatedStats(),
                node.getStats());
    }

    public static class JsonRenderedNode
    {
        private final Optional<SourceLocation> sourceLocation;
        private final String id;
        private final String name;
        private final String identifier;
        private final String details;
        private final List<JsonRenderedNode> children;
        private final List<String> remoteSources;
        private final List<PlanNodeStatsEstimate> estimates;
        private final Optional<PlanNodeStats> stats;

        @JsonCreator
        public JsonRenderedNode(Optional<SourceLocation> sourceLocation, String id, String name, String identifier, String details, List<JsonRenderedNode> children, List<String> remoteSources, List<PlanNodeStatsEstimate> estimates, Optional<PlanNodeStats> stats)
        {
            this.sourceLocation = sourceLocation;
            this.id = requireNonNull(id, "id is null");
            this.name = requireNonNull(name, "name is null");
            this.identifier = requireNonNull(identifier, "identifier is null");
            this.details = requireNonNull(details, "details is null");
            this.children = requireNonNull(children, "children is null");
            this.remoteSources = requireNonNull(remoteSources, "id is null");
            this.estimates = requireNonNull(estimates, "estimate is null");
            this.stats = requireNonNull(stats, "stats is null");
        }

        @JsonProperty
        public Optional<SourceLocation> getSourceLocation()
        {
            return sourceLocation;
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

        @JsonProperty
        public List<PlanNodeStatsEstimate> getEstimates()
        {
            return estimates;
        }

        @JsonProperty
        public Optional<PlanNodeStats> getStats()
        {
            return stats;
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
            JsonRenderedNode other = (JsonRenderedNode) obj;
            return Objects.equals(this.name, other.name) &&
                    Objects.equals(this.id, other.id) &&
                    Objects.equals(this.sourceLocation, other.sourceLocation) &&
                    Objects.equals(this.identifier, other.identifier) &&
                    Objects.equals(this.details, other.details) &&
                    Objects.equals(this.children, other.children) &&
                    Objects.equals(this.estimates, other.estimates) &&
                    Objects.equals(this.remoteSources, other.remoteSources) &&
                    Objects.equals(this.stats, other.stats);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, id, sourceLocation, identifier, details, children, estimates, remoteSources, stats);
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

    /**
     * Used for deserializing rendered JSON plan
     */
    public static class JsonPlan
    {
        @JsonProperty
        private final JsonRenderedNode plan;

        @JsonCreator
        public JsonPlan(@JsonProperty("plan") JsonRenderedNode plan)
        {
            this.plan = plan;
        }

        @JsonProperty
        public JsonRenderedNode getPlan()
        {
            return plan;
        }
    }
}
