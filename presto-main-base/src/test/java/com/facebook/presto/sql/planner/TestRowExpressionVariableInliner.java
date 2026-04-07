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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class TestRowExpressionVariableInliner
{
    private static class TestFunctionHandle
            implements FunctionHandle
    {
        @Override
        public CatalogSchemaName getCatalogSchemaName()
        {
            return QualifiedObjectName.valueOf(new CatalogSchemaName("a", "b"), "c").getCatalogSchemaName();
        }

        @Override
        public String getName()
        {
            return "a.b.c";
        }

        @Override
        public FunctionKind getKind()
        {
            return SCALAR;
        }

        @Override
        public List<TypeSignature> getArgumentTypes()
        {
            return emptyList();
        }
    }

    private static final FunctionHandle TEST_FUNCTION = new TestFunctionHandle();

    @Test
    public void testInlineVariable()
    {
        assertEquals(RowExpressionVariableInliner.inlineVariables(
                ImmutableMap.of(
                        variable("a"),
                        variable("b")),
                variable("a")),
                variable("b"));
    }

    @Test
    public void testIdentityMappingPreservesReference()
    {
        // When a variable maps to itself, the inliner should preserve the original expression reference
        VariableReferenceExpression a = variable("a");
        RowExpression result = RowExpressionVariableInliner.inlineVariables(v -> v, a);
        assertSame(result, a, "Identity mapping should return the exact same object");
    }

    @Test
    public void testIdentityMappingInComplexExpression()
    {
        // In a complex expression where variables map to themselves, the original tree should be preserved
        VariableReferenceExpression a = variable("a");
        VariableReferenceExpression b = variable("b");
        CallExpression expression = new CallExpression("add", TEST_FUNCTION, BIGINT, ImmutableList.of(a, b));
        RowExpression result = RowExpressionVariableInliner.inlineVariables(v -> v, expression);
        assertSame(result, expression, "Identity mapping on complex expression should preserve the original tree");
    }

    @Test
    public void testPartialIdentityMapping()
    {
        // When some variables are mapped and others are identity, the mapped ones should change
        VariableReferenceExpression a = variable("a");
        VariableReferenceExpression b = variable("b");
        VariableReferenceExpression c = variable("c");
        CallExpression expression = new CallExpression("add", TEST_FUNCTION, BIGINT, ImmutableList.of(a, b));
        RowExpression result = RowExpressionVariableInliner.inlineVariables(
                ImmutableMap.of(a, c, b, b),
                expression);
        // a -> c, b -> b (identity), so expression should change
        assertEquals(result, new CallExpression("add", TEST_FUNCTION, BIGINT, ImmutableList.of(c, b)));
    }

    @Test
    public void testInlineLambda()
    {
        assertEquals(RowExpressionVariableInliner.inlineVariables(
                ImmutableMap.of(
                        variable("a"),
                        variable("b"),
                        variable("lambda_argument"),
                        variable("c")),
                new CallExpression("apply", TEST_FUNCTION, BIGINT, ImmutableList.of(
                        variable("a"),
                        new LambdaDefinitionExpression(
                                Optional.empty(),
                                ImmutableList.of(BIGINT),
                                ImmutableList.of("lambda_argument"),
                                new CallExpression("add", TEST_FUNCTION, BIGINT, ImmutableList.of(variable("lambda_argument"), variable("a"))))))),
                new CallExpression("apply", TEST_FUNCTION, BIGINT, ImmutableList.of(
                        variable("b"),
                        new LambdaDefinitionExpression(
                                Optional.empty(),
                                ImmutableList.of(BIGINT),
                                ImmutableList.of("lambda_argument"),
                                new CallExpression("add", TEST_FUNCTION, BIGINT, ImmutableList.of(variable("lambda_argument"), variable("b")))))));
    }

    private VariableReferenceExpression variable(String name)
    {
        return new VariableReferenceExpression(Optional.empty(), name, BIGINT);
    }
}
