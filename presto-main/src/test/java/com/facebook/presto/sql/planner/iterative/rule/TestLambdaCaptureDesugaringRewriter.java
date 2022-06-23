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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.tree.BindExpression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter.rewrite;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestLambdaCaptureDesugaringRewriter
{
    @Test
    public void testRewriteBasicLambda()
    {
        final List<VariableReferenceExpression> variables = ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "a", BIGINT), new VariableReferenceExpression(Optional.empty(), "x", BIGINT));
        final PlanVariableAllocator allocator = new PlanVariableAllocator(variables);

        assertEquals(rewrite(expression("x -> a + x"), allocator),
                new BindExpression(
                        ImmutableList.of(expression("a")),
                        new LambdaExpression(
                                Stream.of("a_0", "x")
                                        .map(Identifier::new)
                                        .map(LambdaArgumentDeclaration::new)
                                        .collect(toList()),
                                expression("a_0 + x"))));
    }
}
