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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.expressions.translator.FunctionTranslator;
import com.facebook.presto.expressions.translator.RowExpressionTranslator;
import com.facebook.presto.expressions.translator.RowExpressionTreeTranslator;
import com.facebook.presto.expressions.translator.TranslatedExpression;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.expressions.translator.FunctionTranslator.buildFunctionTranslator;
import static com.facebook.presto.expressions.translator.RowExpressionTreeTranslator.translateWith;
import static com.facebook.presto.expressions.translator.TranslatedExpression.untranslated;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRowExpressionTranslator
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    private final FunctionAndTypeManager functionAndTypeManager;
    private final TestingRowExpressionTranslator sqlToRowExpressionTranslator;

    public TestRowExpressionTranslator()
    {
        this.functionAndTypeManager = METADATA.getFunctionAndTypeManager();
        this.sqlToRowExpressionTranslator = new TestingRowExpressionTranslator(METADATA);
    }

    @Test
    public void testEndToEndFunctionTranslation()
    {
        String untranslated = "LN(bitwise_and(1, col1))";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("col1", BIGINT));
        CallExpression callExpression = (CallExpression) sqlToRowExpressionTranslator.translate(expression(untranslated), typeProvider);

        TranslatedExpression translatedExpression = translateWith(
                callExpression,
                new TestFunctionTranslator(functionAndTypeManager, buildFunctionTranslator(ImmutableSet.of(TestFunctions.class))),
                emptyMap());
        assertTrue(translatedExpression.getTranslated().isPresent());
        assertEquals(translatedExpression.getTranslated().get(), "LNof(1 BITWISE_AND col1)");
    }

    @Test
    public void testEndToEndSpecialFormTranslation()
    {
        String untranslated = "col1 AND col2";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("col1", BOOLEAN, "col2", BOOLEAN));

        RowExpression specialForm = sqlToRowExpressionTranslator.translate(expression(untranslated), typeProvider);

        TranslatedExpression translatedExpression = translateWith(
                specialForm,
                new TestFunctionTranslator(functionAndTypeManager, buildFunctionTranslator(ImmutableSet.of(TestFunctions.class))),
                emptyMap());
        assertTrue(translatedExpression.getTranslated().isPresent());
        assertEquals(translatedExpression.getTranslated().get(), "col1 TEST_AND col2");
    }

    @Test
    public void testMissingFunctionTranslator()
    {
        String untranslated = "ABS(col1)";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("col1", DOUBLE));

        RowExpression specialForm = sqlToRowExpressionTranslator.translate(expression(untranslated), typeProvider);

        TranslatedExpression translatedExpression = translateWith(
                specialForm,
                new TestFunctionTranslator(functionAndTypeManager, buildFunctionTranslator(ImmutableSet.of(TestFunctions.class))),
                emptyMap());
        assertFalse(translatedExpression.getTranslated().isPresent());
    }

    @Test
    public void testIncorrectFunctionSignatureInDefinition()
    {
        String untranslated = "CEIL(col1)";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("col1", DOUBLE));

        RowExpression specialForm = sqlToRowExpressionTranslator.translate(expression(untranslated), typeProvider);

        TranslatedExpression translatedExpression = translateWith(
                specialForm,
                new TestFunctionTranslator(functionAndTypeManager, buildFunctionTranslator(ImmutableSet.of(TestFunctions.class))),
                emptyMap());
        assertFalse(translatedExpression.getTranslated().isPresent());
    }

    @Test
    public void testHiddenFunctionNot()
    {
        String untranslated = "NOT true";
        RowExpression specialForm = sqlToRowExpressionTranslator.translate(expression(untranslated), TypeProvider.empty());

        TranslatedExpression translatedExpression = translateWith(
                specialForm,
                new TestFunctionTranslator(functionAndTypeManager, buildFunctionTranslator(ImmutableSet.of(TestFunctions.class))),
                emptyMap());
        assertTrue(translatedExpression.getTranslated().isPresent());
        assertEquals(translatedExpression.getTranslated().get(), "NOT_2 true");
    }

    @Test
    public void testBasicOperator()
    {
        String untranslated = "col1 + col2";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("col1", BIGINT, "col2", BIGINT));
        RowExpression specialForm = sqlToRowExpressionTranslator.translate(expression(untranslated), typeProvider);

        TranslatedExpression translatedExpression = translateWith(
                specialForm,
                new TestFunctionTranslator(functionAndTypeManager, buildFunctionTranslator(ImmutableSet.of(TestFunctions.class))),
                emptyMap());
        assertTrue(translatedExpression.getTranslated().isPresent());
        assertEquals(translatedExpression.getTranslated().get(), "col1 -|- col2");
    }

    @Test
    public void testLessThanOperator()
    {
        String untranslated = "col1 < col2";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("col1", BIGINT, "col2", BIGINT));
        RowExpression specialForm = sqlToRowExpressionTranslator.translate(expression(untranslated), typeProvider);

        TranslatedExpression translatedExpression = translateWith(
                specialForm,
                new TestFunctionTranslator(functionAndTypeManager, buildFunctionTranslator(ImmutableSet.of(TestFunctions.class))),
                emptyMap());
        assertTrue(translatedExpression.getTranslated().isPresent());
        assertEquals(translatedExpression.getTranslated().get(), "col1 LT col2");
    }

    @Test
    public void testUntranslatableSpecialForm()
    {
        String untranslated = "col1 OR col2";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("col1", BOOLEAN, "col2", BOOLEAN));
        RowExpression specialForm = sqlToRowExpressionTranslator.translate(expression(untranslated), typeProvider);

        TranslatedExpression translatedExpression = translateWith(
                specialForm,
                new TestFunctionTranslator(functionAndTypeManager, buildFunctionTranslator(ImmutableSet.of(TestFunctions.class))),
                emptyMap());
        assertFalse(translatedExpression.getTranslated().isPresent());
    }

    private class TestFunctionTranslator
            extends RowExpressionTranslator<String, Map<VariableReferenceExpression, ColumnHandle>>
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final FunctionTranslator<String> functionTranslator;

        TestFunctionTranslator(FunctionAndTypeManager functionAndTypeManager, FunctionTranslator<String> functionTranslator)
        {
            this.functionTranslator = requireNonNull(functionTranslator);
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager);
        }

        @Override
        public TranslatedExpression<String> translateConstant(ConstantExpression literal, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<String, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
        {
            return new TranslatedExpression<>(Optional.of(literal.toString()), literal, emptyList());
        }

        @Override
        public TranslatedExpression<String> translateCall(CallExpression callExpression, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<String, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
        {
            List<TranslatedExpression<String>> translatedExpressions = callExpression.getArguments().stream()
                    .map(expression -> rowExpressionTreeTranslator.rewrite(expression, context))
                    .collect(Collectors.toList());
            FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(callExpression.getFunctionHandle());
            try {
                return functionTranslator.translate(functionMetadata, callExpression, translatedExpressions);
            }
            catch (Throwable t) {
                return untranslated(callExpression, translatedExpressions);
            }
        }

        @Override
        public TranslatedExpression<String> translateSpecialForm(SpecialFormExpression specialFormExpression, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<String, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
        {
            if (!specialFormExpression.getForm().equals(SpecialFormExpression.Form.AND)) {
                return untranslated(specialFormExpression);
            }

            List<TranslatedExpression<String>> translatedExpressions = specialFormExpression.getArguments().stream()
                    .map(expression -> rowExpressionTreeTranslator.rewrite(expression, context))
                    .collect(Collectors.toList());

            assertTrue(translatedExpressions.get(0).getTranslated().isPresent());
            assertTrue(translatedExpressions.get(1).getTranslated().isPresent());
            return new TranslatedExpression<>(
                    Optional.of(translatedExpressions.get(0).getTranslated().get() + " TEST_AND " + translatedExpressions.get(1).getTranslated().get()),
                    specialFormExpression,
                    translatedExpressions);
        }

        @Override
        public TranslatedExpression<String> translateVariable(VariableReferenceExpression variable, Map<VariableReferenceExpression, ColumnHandle> context, RowExpressionTreeTranslator<String, Map<VariableReferenceExpression, ColumnHandle>> rowExpressionTreeTranslator)
        {
            return new TranslatedExpression<>(Optional.of(variable.getName()), variable, emptyList());
        }
    }

    public static class TestFunctions
    {
        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static String bitwiseAnd(@SqlType(StandardTypes.BIGINT) String left, @SqlType(StandardTypes.BIGINT) String right)
        {
            return left + " BITWISE_AND " + right;
        }

        @ScalarFunction("ln")
        @SqlType(StandardTypes.DOUBLE)
        public static String ln(@SqlType(StandardTypes.DOUBLE) String sql)
        {
            return "LNof(" + sql + ")";
        }

        @ScalarFunction("ceil")
        @SqlType(StandardTypes.DOUBLE)
        public static String ceil(@SqlType(StandardTypes.BOOLEAN) String sql)
        {
            return "CEILof(" + sql + ")";
        }

        @ScalarFunction("not")
        @SqlType(StandardTypes.BOOLEAN)
        public static String not(@SqlType(StandardTypes.BOOLEAN) String sql)
        {
            return "NOT_2 " + sql;
        }

        @ScalarOperator(OperatorType.ADD)
        @SqlType(StandardTypes.BIGINT)
        public static String plus(@SqlType(StandardTypes.BIGINT) String left, @SqlType(StandardTypes.BIGINT) String right)
        {
            return left + " -|- " + right;
        }

        @ScalarOperator(OperatorType.LESS_THAN)
        @SqlType(StandardTypes.BOOLEAN)
        public static String lessThan(@SqlType(StandardTypes.BIGINT) String left, @SqlType(StandardTypes.BIGINT) String right)
        {
            return left + " LT " + right;
        }
    }
}
