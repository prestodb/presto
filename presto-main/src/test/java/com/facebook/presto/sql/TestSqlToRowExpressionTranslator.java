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
package com.facebook.presto.sql;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.FunctionType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestSqlToRowExpressionTranslator
{
    private final TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator();
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    @Test(timeOut = 10_000)
    public void testPossibleExponentialOptimizationTime()
    {
        Expression expression = new LongLiteral("1");
        ImmutableMap.Builder<NodeRef<Expression>, Type> types = ImmutableMap.builder();
        types.put(NodeRef.of(expression), BIGINT);
        for (int i = 0; i < 100; i++) {
            expression = new CoalesceExpression(expression, new LongLiteral("2"));
            types.put(NodeRef.of(expression), BIGINT);
        }
        translator.translateAndOptimize(expression, types.build());
    }

    @Test
    public void testOptimizeDecimalLiteral()
    {
        // Short decimal
        assertEquals(translator.translateAndOptimize(expression("CAST(NULL AS DECIMAL(7,2))")), constant(null, createDecimalType(7, 2)));
        assertEquals(translator.translateAndOptimize(expression("DECIMAL '42'")), constant(42L, createDecimalType(2, 0)));
        assertEquals(translator.translateAndOptimize(expression("CAST(42 AS DECIMAL(7,2))")), constant(4200L, createDecimalType(7, 2)));
        assertEquals(translator.translateAndOptimize(translator.simplifyExpression(expression("CAST(42 AS DECIMAL(7,2))"))), constant(4200L, createDecimalType(7, 2)));

        // Long decimal
        assertEquals(translator.translateAndOptimize(expression("CAST(NULL AS DECIMAL(35,2))")), constant(null, createDecimalType(35, 2)));
        assertEquals(
                translator.translateAndOptimize(expression("DECIMAL '123456789012345678901234567890'")),
                constant(encodeScaledValue(new BigDecimal("123456789012345678901234567890")), createDecimalType(30, 0)));
        assertEquals(
                translator.translateAndOptimize(expression("CAST(DECIMAL '123456789012345678901234567890' AS DECIMAL(35,2))")),
                constant(encodeScaledValue(new BigDecimal("123456789012345678901234567890.00")), createDecimalType(35, 2)));
        assertEquals(
                translator.translateAndOptimize(translator.simplifyExpression(expression("CAST(DECIMAL '123456789012345678901234567890' AS DECIMAL(35,2))"))),
                constant(encodeScaledValue(new BigDecimal("123456789012345678901234567890.00")), createDecimalType(35, 2)));
    }

    @Test
    public void testExpressionNeedsDesugar()
    {
        TypeProvider types = TypeProvider.copyOf(ImmutableMap.of("a", VARCHAR));

        assertEquals(translator.translate(expression("current_user"), types),
                constant(Slices.utf8Slice(TEST_SESSION.getUser()), VARCHAR));

        assertEquals(translator.translate(expression("Row(1, 'a')[1]"), types),
                dereference(row(constant(1L, INTEGER), stringConstant("a")), 0));

        assertEquals(translator.translate(expression("try(cast(a as bigint))"), types),
                call(METADATA.getFunctionManager(), "$internal$try", BIGINT,
                        bind(ImmutableList.of(variable("a", VARCHAR)),
                                lambda(ImmutableList.of(variable("a_0", VARCHAR)), cast(variable("a_0", VARCHAR), BIGINT)))));

        assertEquals(translator.translate(expression("try(cast('a' as bigint))"), types),
                call(METADATA.getFunctionManager(), "$internal$try", BIGINT,
                        lambda(ImmutableList.of(), cast(stringConstant("a"), BIGINT))));
    }

    private static RowExpression bind(List<VariableReferenceExpression> bindArguments, LambdaDefinitionExpression lambda)
    {
        return new SpecialFormExpression(SpecialFormExpression.Form.BIND, new FunctionType(ImmutableList.of(), lambda.getBody().getType()),
                new ImmutableList.Builder().addAll(bindArguments).add(lambda).build());
    }

    private static LambdaDefinitionExpression lambda(List<VariableReferenceExpression> arguments, RowExpression input)
    {
        return new LambdaDefinitionExpression(arguments.stream().map(VariableReferenceExpression::getType).collect(toImmutableList()),
                arguments.stream().map(VariableReferenceExpression::getName).collect(toImmutableList()),
                input);
    }

    private static RowExpression cast(RowExpression input, Type type)
    {
        return call(CAST.name(), METADATA.getFunctionManager().lookupCast(CAST, input.getType().getTypeSignature(), type.getTypeSignature()), type, input);
    }

    private static RowExpression dereference(RowExpression input, int index)
    {
        return specialForm(SpecialFormExpression.Form.DEREFERENCE, input.getType().getTypeParameters().get(index), input, constant((long) index, INTEGER));
    }

    private static RowExpression row(RowExpression... fields)
    {
        return specialForm(SpecialFormExpression.Form.ROW_CONSTRUCTOR, RowType.anonymous(Arrays.stream(fields).map(RowExpression::getType).collect(toImmutableList())), fields);
    }

    private static RowExpression stringConstant(String value)
    {
        return constant(Slices.utf8Slice(value), VarcharType.createVarcharType(value.length()));
    }
}
