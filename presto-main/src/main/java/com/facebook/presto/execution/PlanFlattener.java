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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class PlanFlattener
{
    private final QueryExplainer explainer;
    private final FlatteningVisitor visitor;

    @Inject
    public PlanFlattener(QueryExplainer explainer, ObjectMapper objectMapper)
    {
        this.explainer = requireNonNull(explainer, "explainer is null");
        this.visitor = new FlatteningVisitor(requireNonNull(objectMapper, "objectMapper is null"));
    }

    public FlattenedPlan flatten(SubPlan rootStage, Session session)
    {
        return new FlattenedPlan(rootStage.getAllFragments().stream()
                .map(fragment -> FlattenedPlanFragment.fromPlanFragment(fragment, explainer, session, visitor))
                .collect(Collectors.toList()));
    }

    public static class FlattenedPlan
    {
        private final List<FlattenedPlanFragment> fragments;

        private FlattenedPlan(List<FlattenedPlanFragment> fragments)
        {
            this.fragments = ImmutableList.copyOf(requireNonNull(fragments, "fragments is null"));
        }

        @JsonProperty
        public List<FlattenedPlanFragment> getFragments()
        {
            return fragments;
        }
    }

    public static class FlattenedPlanFragment
    {
        private final String textPlan;
        private final List<FlattenedNode> nodes;
        private final PlanFragment fragment;

        public static FlattenedPlanFragment fromPlanFragment(PlanFragment fragment, QueryExplainer explainer, Session session, FlatteningVisitor visitor)
        {
            ImmutableList.Builder<FlattenedNode> nodes = ImmutableList.builder();
            fragment.getRoot().accept(visitor, nodes);
            return new FlattenedPlanFragment(explainer.getPlan(fragment, session), fragment, nodes.build());
        }

        private FlattenedPlanFragment(String textPlan, PlanFragment fragment, List<FlattenedNode> nodes)
        {
            this.textPlan = requireNonNull(textPlan, "textPlan is null");
            this.fragment = requireNonNull(fragment, "fragment is null");
            this.nodes = ImmutableList.copyOf(requireNonNull(nodes, "nodes is null"));
        }

        @JsonProperty
        public PlanFragmentId getId()
        {
            return fragment.getId();
        }

        @JsonProperty
        public String getTextPlan()
        {
            return textPlan;
        }

        @JsonProperty
        public PlanNode getTree()
        {
            return fragment.getRoot();
        }

        @JsonProperty
        public List<FlattenedNode> getNodes()
        {
            return nodes;
        }

        @JsonProperty
        public Map<Symbol, Type> getSymbols()
        {
            return fragment.getSymbols();
        }

        @JsonProperty
        public PartitioningHandle getPartitioning()
        {
            return fragment.getPartitioning();
        }

        @JsonProperty
        public List<PlanNodeId> getPartitionedSources()
        {
            return fragment.getPartitionedSources();
        }

        @JsonProperty
        public List<Type> getTypes()
        {
            return fragment.getTypes();
        }

        @JsonProperty
        public Set<PlanNode> getPartitionedSourceNodes()
        {
            return fragment.getPartitionedSourceNodes();
        }

        @JsonProperty
        public List<RemoteSourceNode> getRemoteSourceNodes()
        {
            return fragment.getRemoteSourceNodes();
        }

        @JsonProperty
        public PartitioningScheme getPartitioningScheme()
        {
            return fragment.getPartitioningScheme();
        }
    }

    public static class FlattenedNode
    {
        private final String node;

        public FlattenedNode(String node)
        {
            this.node = requireNonNull(node, "node is null");
        }

        @JsonValue
        @JsonRawValue
        public String getNode()
        {
            return node;
        }
    }

    private static class FlatteningVisitor
            extends SimplePlanVisitor<ImmutableList.Builder<FlattenedNode>>
    {
        private final ObjectMapper flatteningMapper;

        public FlatteningVisitor(ObjectMapper originalMapper)
        {
            this.flatteningMapper = originalMapper.copy();

            PlanNodeFlatteningSerializer flattener = new PlanNodeFlatteningSerializer(originalMapper.getSerializerProviderInstance());
            flatteningMapper.registerModule(new SimpleModule().addSerializer(PlanNode.class, flattener));
        }

        @Override
        protected Void visitPlan(PlanNode node, ImmutableList.Builder<FlattenedNode> context)
        {
            try {
                context.add(new FlattenedNode(flatteningMapper.writeValueAsString(node)));
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException("Unable to flatten plan node", e);
            }
            return super.visitPlan(node, context);
        }
    }

    /**
     * The children of the given node should be serialized with only the properties of the PlanNode supertype.
     * This is accomplished using different techniques to serialize the root (i.e. the node it's passed) and
     * that node's children. For the root, it uses the subtype serializer (e.g. OutputNode), for children it uses
     * the supertype serializer (i.e. the serializer for PlanNode).
     */
    private static class PlanNodeFlatteningSerializer
            extends JsonSerializer<PlanNode>
    {
        private final SerializerProvider provider;
        private final JsonSerializer<Object> superTypeSerializer;

        private boolean rootLevel = true;

        public PlanNodeFlatteningSerializer(SerializerProvider provider)
        {
            this.provider = provider;
            try {
                this.superTypeSerializer = provider.findTypedValueSerializer(PlanNode.class, false, null);
            }
            catch (JsonMappingException e) {
                throw new RuntimeException("Unable to serialize plan node", e);
            }
        }

        @Override
        public void serialize(PlanNode value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void serializeWithType(PlanNode value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer)
                throws IOException
        {
            if (rootLevel) {
                rootLevel = false;
                try {
                    JsonSerializer<Object> subTypeSerializer = provider.findTypedValueSerializer(value.getClass(), true, null);
                    subTypeSerializer.serializeWithType(value, gen, serializers, typeSer);
                }
                finally {
                    rootLevel = true;
                }
                return;
            }

            superTypeSerializer.serializeWithType(value, gen, serializers, typeSer);
        }
    }
}
