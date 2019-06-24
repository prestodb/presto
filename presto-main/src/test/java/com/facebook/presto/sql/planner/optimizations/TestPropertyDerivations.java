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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConstantProperty;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.castToRowExpression;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.singleStreamPartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.builder;
import static com.facebook.presto.sql.planner.optimizations.PropertyDerivations.derivePropertiesRecursively;
import static org.testng.Assert.assertEquals;

public class TestPropertyDerivations
{
    private static final Metadata META_DATA = MetadataManager.createTestMetadataManager();
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testDeriveProject()
    {
        PlanBuilder p = new PlanBuilder(new PlanNodeIdAllocator(), META_DATA);
        PlanNode projection = p.project(
                Assignments.builder()
                        .put(p.variable("a"), castToRowExpression("b")) // a will not be inlined because it appears before b
                        .put(p.variable("b"), castToRowExpression("1"))
                        .put(p.variable("c"), castToRowExpression("b")) // c will be inlined as 1
                        .put(p.variable("d"), castToRowExpression("d"))
                        .build(),
                p.values(p.variable("c"), p.variable("d")));
        ActualProperties derivedProperties = derivePropertiesRecursively(projection, META_DATA, TEST_SESSION, p.getTypes(), SQL_PARSER);
        assertEquals(derivedProperties, builder()
                .global(singleStreamPartition())
                .local(ImmutableList.of(constant("c"), constant("b")))
                .constants(new ImmutableMap.Builder<VariableReferenceExpression, ConstantExpression>()
                        .put(variable("c"), new ConstantExpression(1L, BIGINT))
                        .put(variable("b"), new ConstantExpression(1L, BIGINT))
                        .build())
                .build());

        PlanNode projection2 = p.project(
                Assignments.builder()
                        .put(p.variable("a1"), castToRowExpression("a"))
                        .put(p.variable("b1"), castToRowExpression("b"))
                        .put(p.variable("c1"), castToRowExpression("c"))
                        .put(p.variable("d1"), castToRowExpression("d"))
                        .build(),
                projection);
        ActualProperties derivedProperties2 = derivePropertiesRecursively(projection2, META_DATA, TEST_SESSION, p.getTypes(), SQL_PARSER);
        assertEquals(derivedProperties2, builder()
                .global(singleStreamPartition())
                .local(ImmutableList.of(constant("c1"), constant("b1")))
                .constants(new ImmutableMap.Builder<VariableReferenceExpression, ConstantExpression>()
                        .put(variable("c1"), new ConstantExpression(1L, BIGINT))
                        .put(variable("b1"), new ConstantExpression(1L, BIGINT))
                        .build())
                .build());
    }

    private static ConstantProperty<VariableReferenceExpression> constant(String column)
    {
        return new ConstantProperty<>(variable(column));
    }

    private static VariableReferenceExpression variable(String name)
    {
        return new VariableReferenceExpression(name, BIGINT);
    }
}
