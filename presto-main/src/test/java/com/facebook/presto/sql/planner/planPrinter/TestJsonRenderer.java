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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.TestingHandle;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.AbstractMockMetadata.dummyMetadata;
import static com.facebook.presto.testing.TestingEnvironment.FUNCTION_AND_TYPE_MANAGER;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;

public class TestJsonRenderer
{
    private static final PlanBuilder PLAN_BUILDER = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), dummyMetadata());
    private static final VariableReferenceExpression COLUMN_VARIABLE = new VariableReferenceExpression(Optional.empty(), "column", VARCHAR);
    private static final ColumnHandle COLUMN_HANDLE = new TestingMetadata.TestingColumnHandle("column");
    private static final PlanVariableAllocator VARIABLE_ALLOCATOR = new PlanVariableAllocator();
    private static final JsonRenderer JSON_RENDERER = new JsonRenderer(FUNCTION_AND_TYPE_MANAGER);
    private static final TableHandle TABLE_HANDLE_WITH_LAYOUT = new TableHandle(
            new ConnectorId("testConnector"),
            new TestingMetadata.TestingTableHandle(),
            TestingTransactionHandle.create(),
            Optional.of(TestingHandle.INSTANCE));
    private static final JsonCodec<JsonRenderer.JsonRenderedNode> PLAN_CODEC;

    static {
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        provider.setKeyDeserializers(ImmutableMap.of(VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionDeserializer(FUNCTION_AND_TYPE_MANAGER)));
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider, true);
        PLAN_CODEC = codecFactory.jsonCodec(JsonRenderer.JsonRenderedNode.class);
    }

    @Test
    public void testRoundtrip()
    {
        Map<PlanRepresentation, JsonRenderer.JsonRenderedNode> mapper = buildSimpleNodePlan();
        verify(mapper);
        mapper = buildMultiNodePlan();
        verify(mapper);
    }

    private PlanRepresentation getPlanRepresentation(PlanNode root)
    {
        return new PlanRepresentation(
                root,
                VARIABLE_ALLOCATOR.getTypes(),
                Optional.empty(),
                Optional.empty());
    }

    private NodeRepresentation getNodeRepresentation(PlanNode root, List<PlanNodeId> planNodeIds)
    {
        return new NodeRepresentation(
                Optional.empty(),
                root.getId(),
                root.getClass().getName(),
                root.getClass().getSimpleName(),
                "",
                root.getOutputVariables(),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                planNodeIds,
                ImmutableList.of());
    }

    private Map<PlanRepresentation, JsonRenderer.JsonRenderedNode> buildPlanResult(PlanNode root)
    {
        ImmutableMap.Builder<PlanRepresentation, JsonRenderer.JsonRenderedNode> result = ImmutableMap.builder();
        List<PlanNodeId> childrenIds = root.getSources().stream().map(PlanNode::getId).collect(toImmutableList());
        PlanRepresentation key = getPlanRepresentation(root);
        NodeRepresentation nodeRepresentation = getNodeRepresentation(root, childrenIds);
        key.addNode(nodeRepresentation);
        // deserialize string to json
        JsonRenderer.JsonRenderedNode value = JSON_RENDERER.renderJson(key, nodeRepresentation);
        // put json as value for verify
        result.put(key, value);
        return result.build();
    }

    private Map<PlanRepresentation, JsonRenderer.JsonRenderedNode> buildSimpleNodePlan()
    {
        TableScanNode scanNode = PLAN_BUILDER.tableScan(
                TABLE_HANDLE_WITH_LAYOUT,
                ImmutableList.of(COLUMN_VARIABLE),
                ImmutableMap.of(COLUMN_VARIABLE, COLUMN_HANDLE),
                TupleDomain.all(),
                TupleDomain.all());

        List<PlanNodeId> childrenIds = scanNode.getSources().stream().map(PlanNode::getId).collect(toImmutableList());
        PlanRepresentation key = getPlanRepresentation(scanNode);
        NodeRepresentation nodeRepresentation = getNodeRepresentation(scanNode, childrenIds);
        key.addNode(nodeRepresentation);

        return buildPlanResult(scanNode);
    }

    private Map<PlanRepresentation, JsonRenderer.JsonRenderedNode> buildMultiNodePlan()
    {
        ImmutableMap.Builder<PlanRepresentation, JsonRenderer.JsonRenderedNode> result = ImmutableMap.builder();
        ImmutableMap<VariableReferenceExpression, RowExpression> map = ImmutableMap.of(
                COLUMN_VARIABLE,
                COLUMN_VARIABLE);
        TableScanNode scan = PLAN_BUILDER.tableScan(
                TABLE_HANDLE_WITH_LAYOUT,
                ImmutableList.of(COLUMN_VARIABLE),
                ImmutableMap.of(COLUMN_VARIABLE, COLUMN_HANDLE));
        LimitNode limit = PLAN_BUILDER.limit(10, scan);
        ProjectNode root = PLAN_BUILDER.project(new Assignments(map), limit);

        return buildPlanResult(root);
    }

    private void verify(Map<PlanRepresentation, JsonRenderer.JsonRenderedNode> mapper)
    {
        for (PlanRepresentation key : mapper.keySet()) {
            // serialize json to string
            String originKey = JSON_RENDERER.render(key);
            JsonRenderer.JsonRenderedNode result = PLAN_CODEC.fromJson(originKey);
            JsonRenderer.JsonRenderedNode originValue = mapper.get(key);
            assertEquals(result, originValue);
        }
    }
}
