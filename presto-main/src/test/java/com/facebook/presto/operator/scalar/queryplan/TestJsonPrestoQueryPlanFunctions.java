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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class TestJsonPrestoQueryPlanFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testJsonPlanIds()
    {
        assertFunction("json_presto_query_plan_ids(null)", new ArrayType(VARCHAR), null);

        assertFunction("json_presto_query_plan_ids(json '" +
                        TestJsonPrestoQueryPlanFunctionUtils.joinPlan.replaceAll("'", "''") + "')",
                new ArrayType(VARCHAR), ImmutableList.of("253", "313", "314", "8", "251", "284", "230", "252"));
    }

    @Test
    public void testJsonPlanNodeChildren()
    {
        assertFunction("json_presto_query_plan_node_children(null, null)", new ArrayType(VARCHAR), null);
        assertFunction("json_presto_query_plan_node_children(null, '1')", new ArrayType(VARCHAR), null);

        assertFunction("json_presto_query_plan_node_children(json '" + TestJsonPrestoQueryPlanFunctionUtils.joinPlan.replaceAll("'", "''") + "', '314')",
                new ArrayType(VARCHAR), ImmutableList.of());

        assertFunction("json_presto_query_plan_node_children(json '" + TestJsonPrestoQueryPlanFunctionUtils.joinPlan.replaceAll("'", "''") + "', '230')",
                new ArrayType(VARCHAR), ImmutableList.of("251", "284"));

        assertFunction("json_presto_query_plan_node_children(json '" + TestJsonPrestoQueryPlanFunctionUtils.joinPlan.replaceAll("'", "''") + "', 'nonkey')",
                new ArrayType(VARCHAR), null);
    }

    @Test
    public void testJsonScrubPlan()
    {
        assertFunction("json_presto_query_plan_scrub(json '" + TestJsonPrestoQueryPlanFunctionUtils.joinPlan.replaceAll("'", "''") + "')", JSON,
                TestJsonPrestoQueryPlanFunctionUtils.scrubbedJoinPlan);
    }

    @Test
    public void testJsonFunctionsInvalidPlans()
    {
        // failures to parse json string result in null
        String invalidJsonPlan = "{   \"id\" : \"9\",   \"name\" : \"Output\"}";

        assertFunction("json_presto_query_plan_ids(json '" + invalidJsonPlan.replaceAll("'", "''") + "')",
                new ArrayType(VARCHAR), null);

        assertFunction("json_presto_query_plan_node_children(json '" + invalidJsonPlan.replaceAll("'", "''") + "', 'nonkey')",
                new ArrayType(VARCHAR), null);
    }
}
