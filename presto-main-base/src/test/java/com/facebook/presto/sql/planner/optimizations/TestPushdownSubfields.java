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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;

public class TestPushdownSubfields
{
    private static final Metadata METADATA = createTestMetadataManager();
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(METADATA);
    private static final ExpressionOptimizer EXPRESSION_OPTIMIZER = new RowExpressionOptimizer(METADATA);
    private static final FunctionResolution FUNCTION_RESOLUTION = new FunctionResolution(METADATA.getFunctionAndTypeManager().getFunctionAndTypeResolver());

    @Test
    public void testMapSubscriptExtraction()
    {
        assertSubfields(
                "x[1]",
                ImmutableMap.of("x", mapType(BIGINT, BIGINT)),
                false,
                ImmutableList.of(new Subfield("x[1]")));
        assertNoSubfields(
                "x[CAST(0.99E0 AS DOUBLE)]",
                ImmutableMap.of("x", mapType(DOUBLE, BIGINT)),
                false);
        assertNoSubfields(
                "x[CAST(0.99E0 AS REAL)]",
                ImmutableMap.of("x", mapType(REAL, BIGINT)),
                false);
    }

    @Test
    public void testMapFunctionExtraction()
    {
        assertSubfields(
                "map_subset(x, array[1, 2])",
                ImmutableMap.of("x", mapType(BIGINT, BIGINT)),
                true,
                ImmutableList.of(new Subfield("x[1]"), new Subfield("x[2]")));
        assertSubfields(
                "map_filter(x, (k, v) -> k = 2)",
                ImmutableMap.of("x", mapType(BIGINT, BIGINT)),
                true,
                ImmutableList.of(new Subfield("x[2]")));
        assertNoSubfields(
                "map_subset(x, array[CAST(0.99E0 AS DOUBLE), CAST(1.01E0 AS DOUBLE)])",
                ImmutableMap.of("x", mapType(DOUBLE, BIGINT)),
                true);
        assertNoSubfields(
                "map_filter(x, (k, v) -> k = CAST(0.99E0 AS DOUBLE))",
                ImmutableMap.of("x", mapType(DOUBLE, BIGINT)),
                true);
    }

    private static void assertSubfields(String sql, Map<String, Type> types, boolean pushdownMapFunctionsEnabled, ImmutableList<Subfield> expected)
    {
        assertEquals(extractSubfields(sql, types, pushdownMapFunctionsEnabled), Optional.of(expected));
    }

    private static void assertNoSubfields(String sql, Map<String, Type> types, boolean pushdownMapFunctionsEnabled)
    {
        assertEquals(extractSubfields(sql, types, pushdownMapFunctionsEnabled), Optional.empty());
    }

    private static Optional<ImmutableList<Subfield>> extractSubfields(String sql, Map<String, Type> types, boolean pushdownMapFunctionsEnabled)
    {
        RowExpression expression = EXPRESSION_OPTIMIZER.optimize(TRANSLATOR.translate(sql, types), OPTIMIZED, TEST_SESSION.toConnectorSession());
        return PushdownSubfields.toSubfield(
                        expression,
                        FUNCTION_RESOLUTION,
                        EXPRESSION_OPTIMIZER,
                        TEST_SESSION.toConnectorSession(),
                        METADATA.getFunctionAndTypeManager(),
                        pushdownMapFunctionsEnabled)
                .map(ImmutableList::copyOf);
    }

    private static Type mapType(Type keyType, Type valueType)
    {
        return METADATA.getFunctionAndTypeManager().getType(parseTypeSignature(format("map(%s,%s)", keyType.getDisplayName(), valueType.getDisplayName())));
    }
}
