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
package com.facebook.presto.sql.planner.plan;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class TestAssignments
{
    private final Assignments assignments = assignment(new VariableReferenceExpression(Optional.empty(), "test", BIGINT), TRUE_CONSTANT);

    @Test(expectedExceptions = {UnsupportedOperationException.class})
    public void testOutputsImmutable()
    {
        List<VariableReferenceExpression> outputs = assignments.getOutputs();
        // should throw as it is an unmodifiableList
        outputs.add(new VariableReferenceExpression(Optional.empty(), "test", BIGINT));
    }

    @Test
    public void testOutputsMemoized()
    {
        assertSame(assignments.getOutputs(), assignments.getOutputs());
    }

    @Test
    public void testJsonCodecHandlesLongMapKeys()
    {
        // Guards against a regression where the JSON codec used to deserialize
        // PlanFragment -> ProjectNode -> Assignments failed when an
        // auto-generated variable name in the Assignments map exceeded the
        // default JSON-property-name length cap. Assignments map keys carry
        // VariableReferenceExpression names; for complex projection chains over
        // deeply nested struct schemas the planner can synthesize names well
        // over 50 KB, so the codec must tolerate them.
        String longName = Strings.repeat("a", 60_000);
        JsonCodec<Map<String, String>> codec = JsonCodec.mapJsonCodec(String.class, String.class);
        String json = codec.toJson(ImmutableMap.of(longName, "value"));
        Map<String, String> roundTripped = codec.fromJson(json);
        assertEquals(roundTripped.get(longName), "value");
    }
}
