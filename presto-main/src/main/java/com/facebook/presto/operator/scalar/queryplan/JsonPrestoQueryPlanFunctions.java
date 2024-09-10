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
package com.facebook.presto.operator.scalar.queryplan;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.planner.planPrinter.JsonRenderer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.planPrinter.JsonRenderer.JsonRenderedNode;
import static com.facebook.presto.testing.TestingEnvironment.FUNCTION_AND_TYPE_MANAGER;
import static io.airlift.slice.Slices.utf8Slice;

public final class JsonPrestoQueryPlanFunctions
{
    private JsonPrestoQueryPlanFunctions() {}

    @Description("Get the plan ids of given plan node")
    @ScalarFunction("json_presto_query_plan_ids")
    @SqlType("ARRAY<VARCHAR>")
    @SqlNullable
    public static Block jsonPlanIds(@TypeParameter("ARRAY<VARCHAR>") ArrayType arrayType,
            @SqlType(StandardTypes.JSON) Slice jsonPlan)
    {
        List<JsonRenderedNode> planFragments = parseJsonPlanFragmentsAsList(jsonPlan);

        if (planFragments == null) {
            return null;
        }

        Map<String, List<String>> planMap = extractPlanIds(planFragments);
        return constructSqlArray(arrayType, planMap.keySet().stream().collect(Collectors.toList()));
    }

    @Description("Get the plan ids of given plan node")
    @ScalarFunction("json_presto_query_plan_node_children")
    @SqlType("ARRAY<VARCHAR>")
    @SqlNullable
    public static Block jsonPlanNodeChildren(@TypeParameter("ARRAY<VARCHAR>") ArrayType arrayType,
            @SqlType(StandardTypes.JSON) Slice jsonPlan,
            @SqlType(StandardTypes.VARCHAR) Slice planId)
    {
        List<JsonRenderedNode> planFragments = parseJsonPlanFragmentsAsList(jsonPlan);

        if (planFragments == null) {
            return null;
        }

        Map<String, List<String>> planMap = extractPlanIds(planFragments);
        List<String> planChildren = planMap.get(planId.toStringUtf8());
        if (planChildren == null) {
            return null;
        }
        return constructSqlArray(arrayType, planChildren);
    }

    @Description("Scrub runtime information such as plan estimates and variable names from the query plan, leaving the structure of the plan intact")
    @ScalarFunction("json_presto_query_plan_scrub")
    @SqlType(StandardTypes.JSON)
    @SqlNullable
    public static Slice jsonScrubPlan(@SqlType(StandardTypes.JSON) Slice jsonPlan)
    {
        Map<PlanFragmentId, Map<String, JsonRenderedNode>> jsonRenderedNodeMap = parseJsonPlanFragments(jsonPlan);

        if (jsonRenderedNodeMap == null) {
            return null;
        }

        jsonRenderedNodeMap.forEach((key, planMap) ->
        {
            planMap.put("plan", scrubJsonPlan(planMap.get("plan")));
        });

        JsonCodec<Map<PlanFragmentId, Map<String, JsonRenderer.JsonRenderedNode>>> planMapCodec = constructJsonPlanMapCodec();
        return utf8Slice(planMapCodec.toJson(jsonRenderedNodeMap));
    }

    private static Map<String, List<String>> extractPlanIds(List<JsonRenderedNode> jsonPlanFragments)
    {
        Map<String, List<String>> planMap = new HashMap<>();
        jsonPlanFragments.stream().forEach(jsonPlanFragment -> addChildrenPlanIds(jsonPlanFragment, planMap));
        return planMap;
    }

    private static List<JsonRenderedNode> parseJsonPlanFragmentsAsList(Slice jsonPlan)
    {
        Map<PlanFragmentId, Map<String, JsonRenderedNode>> jsonRenderedNodeMap = parseJsonPlanFragments(jsonPlan);

        if (jsonRenderedNodeMap == null) {
            return null;
        }
        List<JsonRenderedNode> planFragments = new ArrayList<>();
        jsonRenderedNodeMap.values().forEach(map -> planFragments.addAll(map.values()));
        return planFragments;
    }

    private static JsonCodec<Map<PlanFragmentId, Map<String, JsonRenderer.JsonRenderedNode>>> constructJsonPlanMapCodec()
    {
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionSerializer()));
        provider.setKeyDeserializers(ImmutableMap.of(VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionDeserializer(FUNCTION_AND_TYPE_MANAGER)));
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider, true);

        JsonCodec<Map<PlanFragmentId, Map<String, JsonRenderer.JsonRenderedNode>>> planMapCodec = codecFactory.mapJsonCodec(PlanFragmentId.class, mapJsonCodec(String.class, JsonRenderer.JsonRenderedNode.class));
        return planMapCodec;
    }

    private static Map<PlanFragmentId, Map<String, JsonRenderer.JsonRenderedNode>> parseJsonPlanFragments(Slice jsonPlan)
    {
        JsonCodec<Map<PlanFragmentId, Map<String, JsonRenderer.JsonRenderedNode>>> planMapCodec = constructJsonPlanMapCodec();

        try {
            Map<PlanFragmentId, Map<String, JsonRenderer.JsonRenderedNode>> fragmentMap = planMapCodec.fromJson(jsonPlan.getBytes());
            return fragmentMap;
        }
        catch (Exception ex) {
            return null;
        }
    }

    private static JsonCodec<JsonRenderedNode> constructJsonCodec()
    {
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionSerializer()));
        provider.setKeyDeserializers(ImmutableMap.of(VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionDeserializer(FUNCTION_AND_TYPE_MANAGER)));
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider, true);
        JsonCodec<JsonRenderedNode> planCodec = codecFactory.jsonCodec(JsonRenderedNode.class);
        return planCodec;
    }

    private static Block constructSqlArray(ArrayType arrayType, List<String> planIds)
    {
        Type valueType = new ArrayType(VARCHAR);

        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, planIds.size());
        BlockBuilder singleMapBlockBuilder = blockBuilder.beginBlockEntry();
        for (String planId : planIds) {
            valueType.writeSlice(singleMapBlockBuilder, utf8Slice(planId));
        }
        blockBuilder.closeEntry();
        return (Block) arrayType.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
    }

    private static void addChildrenPlanIds(JsonRenderedNode jsonRenderedNode, Map<String, List<String>> childrenMap)
    {
        String planId = jsonRenderedNode.getId();
        List<String> children = jsonRenderedNode.getChildren().stream().map(x -> x.getId()).collect(Collectors.toList());
        childrenMap.put(planId, children);
        jsonRenderedNode.getChildren().stream().forEach(x -> addChildrenPlanIds(x, childrenMap));
    }

    private static JsonRenderedNode scrubJsonPlan(JsonRenderedNode node)
    {
        if (node == null) {
            return null;
        }

        String newName = scrubName(node.getName());
        String newIdentifier = scrubIdentifier(node.getIdentifier());
        List<JsonRenderedNode> newChildren = node.getChildren().stream().map(x -> scrubJsonPlan(x)).collect(Collectors.toList());
        String newDetails = scrubDetails(node.getDetails());

        return new JsonRenderedNode(node.getSourceLocation(), "PLANID", newName, newIdentifier, newDetails, newChildren, node.getRemoteSources(), ImmutableList.of(), Optional.empty());
    }

    private static String scrubName(String name)
    {
        if (name.startsWith("Aggregate(PARTIAL)")) {
            return "Aggregate(PARTIAL)";
        }

        if (name.startsWith("Aggregate(FINAL)")) {
            return "Aggregate(FINAL)";
        }

        if (name.startsWith("Aggregate")) {
            return "Aggregate";
        }

        return name;
    }

    private static String scrubIdentifier(String identifier)
    {
        if (identifier.startsWith("[table")) {
            String pattern = "tableName=(\\w)";

            Pattern r = Pattern.compile(pattern);

            Matcher m = r.matcher(identifier);

            if (m.find()) {
                return "tableName=" + m.group(1);
            }
        }
        return "IDENTIFIER";
    }

    private static String scrubDetails(String details)
    {
        return "DETAILS";
    }
}
