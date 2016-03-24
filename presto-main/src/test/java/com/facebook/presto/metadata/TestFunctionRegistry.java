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
package com.facebook.presto.metadata;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.operator.scalar.CustomFunctions;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.metadata.FunctionRegistry.getMagicLiteralFunctionSignature;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.metadata.FunctionRegistry.unmangleOperator;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.resolveTypes;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.collect.Lists.transform;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFunctionRegistry
{
    @Test
    public void testIdentityCast()
    {
        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry registry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), true);
        Signature exactOperator = registry.getCoercion(HYPER_LOG_LOG, HYPER_LOG_LOG);
        assertEquals(exactOperator.getName(), mangleOperatorName(OperatorType.CAST.name()));
        assertEquals(transform(exactOperator.getArgumentTypes(), Functions.toStringFunction()), ImmutableList.of(StandardTypes.HYPER_LOG_LOG));
        assertEquals(exactOperator.getReturnType().getBase(), StandardTypes.HYPER_LOG_LOG);
    }

    @Test
    public void testExactMatchBeforeCoercion()
    {
        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry registry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), true);
        boolean foundOperator = false;
        for (SqlFunction function : registry.listOperators()) {
            OperatorType operatorType = unmangleOperator(function.getSignature().getName());
            if (operatorType == OperatorType.CAST) {
                continue;
            }
            if (!function.getSignature().getTypeVariableConstraints().isEmpty()) {
                continue;
            }
            if (function.getSignature().getArgumentTypes().stream().anyMatch(TypeSignature::isCalculated)) {
                continue;
            }
            Signature exactOperator = registry.resolveOperator(operatorType, resolveTypes(function.getSignature().getArgumentTypes(), typeManager));
            assertEquals(exactOperator, function.getSignature());
            foundOperator = true;
        }
        assertTrue(foundOperator);
    }

    @Test
    public void testMagicLiteralFunction()
    {
        Signature signature = getMagicLiteralFunctionSignature(TIMESTAMP_WITH_TIME_ZONE);
        assertEquals(signature.getName(), "$literal$timestamp with time zone");
        assertEquals(signature.getArgumentTypes(), ImmutableList.of(parseTypeSignature(StandardTypes.BIGINT)));
        assertEquals(signature.getReturnType().getBase(), StandardTypes.TIMESTAMP_WITH_TIME_ZONE);

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry registry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), true);
        Signature function = registry.resolveFunction(QualifiedName.of(signature.getName()), signature.getArgumentTypes(), false);
        assertEquals(function.getArgumentTypes(), ImmutableList.of(parseTypeSignature(StandardTypes.BIGINT)));
        assertEquals(signature.getReturnType().getBase(), StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QFunction already registered: custom_add(bigint,bigint):bigint\\E")
    public void testDuplicateFunctions()
    {
        List<SqlFunction> functions = new FunctionListBuilder(new TypeRegistry())
                .scalar(CustomFunctions.class)
                .getFunctions()
                .stream()
                .filter(input -> input.getSignature().getName().equals("custom_add"))
                .collect(toImmutableList());

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry registry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), true);
        registry.addFunctions(functions);
        registry.addFunctions(functions);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "'sum' is both an aggregation and a scalar function")
    public void testConflictingScalarAggregation()
            throws Exception
    {
        List<SqlFunction> functions = new FunctionListBuilder(new TypeRegistry())
                .scalar(ScalarSum.class)
                .getFunctions();

        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry registry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), true);
        registry.addFunctions(functions);
    }

    @Test
    public void testListingHiddenFunctions()
            throws Exception
    {
        TypeRegistry typeManager = new TypeRegistry();
        FunctionRegistry registry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), true);
        List<SqlFunction> functions = registry.list();
        List<String> names = transform(functions, input -> input.getSignature().getName());

        assertTrue(names.contains("length"), "Expected function names " + names + " to contain 'length'");
        assertTrue(names.contains("stddev"), "Expected function names " + names + " to contain 'stddev'");
        assertTrue(names.contains("rank"), "Expected function names " + names + " to contain 'rank'");
        assertFalse(names.contains("like"), "Expected function names " + names + " not to contain 'like'");
    }

    public static final class ScalarSum
    {
        private ScalarSum() {}

        @ScalarFunction
        @SqlType(StandardTypes.BIGINT)
        public static long sum(@SqlType(StandardTypes.BIGINT) long a, @SqlType(StandardTypes.BIGINT) long b)
        {
            return a + b;
        }
    }
}
