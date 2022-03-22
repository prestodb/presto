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
package com.facebook.presto.sql.relational;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.FunctionType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.BIND;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.facebook.presto.sql.relational.Expressions.subExpressions;
import static com.facebook.presto.sql.relational.Expressions.uniqueSubExpressions;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static org.testng.Assert.assertEquals;

public class TestSubExpressions
{
    private static final FunctionAndTypeManager FUNCTION_MANAGER = createTestMetadataManager().getFunctionAndTypeManager();

    @Test
    void testExtract()
    {
        RowExpression a = variable("a", BIGINT);
        RowExpression b = constant(1L, BIGINT);
        RowExpression c = call(ADD, a, b);
        RowExpression d = new LambdaDefinitionExpression(ImmutableList.of(BIGINT), ImmutableList.of("a"), c);
        RowExpression e = constant(1L, BIGINT);
        RowExpression f = specialForm(BIND, new FunctionType(ImmutableList.of(BIGINT), BIGINT), e, d);
        assertEquals(subExpressions(a), ImmutableList.of(a));
        assertEquals(subExpressions(b), ImmutableList.of(b));
        assertEquals(subExpressions(c), ImmutableList.of(c, a, b));
        assertEquals(subExpressions(d), ImmutableList.of(d, c, a, b));
        assertEquals(subExpressions(f), ImmutableList.of(f, e, d, c, a, b));
        assertEqualsIgnoreOrder(uniqueSubExpressions(f), ImmutableSet.of(a, b, c, d, f));
    }

    private RowExpression call(OperatorType operator, RowExpression left, RowExpression right)
    {
        FunctionHandle functionHandle = FUNCTION_MANAGER.resolveOperator(operator, fromTypes(left.getType(), right.getType()));
        return Expressions.call(operator.getOperator(), functionHandle, left.getType(), left, right);
    }
}
