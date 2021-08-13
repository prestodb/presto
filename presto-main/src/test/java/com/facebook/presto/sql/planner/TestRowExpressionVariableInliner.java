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
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

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
                                ImmutableList.of(BIGINT),
                                ImmutableList.of("lambda_argument"),
                                new CallExpression("add", TEST_FUNCTION, BIGINT, ImmutableList.of(variable("lambda_argument"), variable("a"))))))),
                new CallExpression("apply", TEST_FUNCTION, BIGINT, ImmutableList.of(
                        variable("b"),
                        new LambdaDefinitionExpression(
                                ImmutableList.of(BIGINT),
                                ImmutableList.of("lambda_argument"),
                                new CallExpression("add", TEST_FUNCTION, BIGINT, ImmutableList.of(variable("lambda_argument"), variable("b")))))));
    }

    private Symbol symbol(String name)
    {
        return new Symbol(name);
    }

    private VariableReferenceExpression variable(String name)
    {
        return new VariableReferenceExpression(name, BIGINT);
    }
}
