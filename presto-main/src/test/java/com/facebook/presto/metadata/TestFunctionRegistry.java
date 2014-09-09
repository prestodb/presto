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

import com.facebook.presto.operator.scalar.CustomFunctions;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.HyperLogLogType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.metadata.FunctionInfo.nameGetter;
import static com.facebook.presto.metadata.FunctionRegistry.getMagicLiteralFunctionSignature;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.collect.Lists.transform;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFunctionRegistry
{
    @Test
    public void testIdentityCast()
    {
        FunctionRegistry registry = new FunctionRegistry(new TypeRegistry(), true);
        FunctionInfo exactOperator = registry.getExactOperator(OperatorType.CAST, ImmutableList.of(HYPER_LOG_LOG), HYPER_LOG_LOG);
        assertEquals(exactOperator.getSignature().getName(), OperatorType.CAST.name());
        assertEquals(exactOperator.getArgumentTypes(), ImmutableList.of(HyperLogLogType.NAME));
        assertEquals(exactOperator.getReturnType(), HyperLogLogType.NAME);
    }

    @Test
    public void testMagicLiteralFunction()
    {
        Signature signature = getMagicLiteralFunctionSignature(TIMESTAMP_WITH_TIME_ZONE);
        assertEquals(signature.getName(), "$literal$timestamp with time zone");
        assertEquals(signature.getArgumentTypes(), ImmutableList.of(BigintType.NAME));
        assertEquals(signature.getReturnType(), TimestampWithTimeZoneType.NAME);

        FunctionRegistry registry = new FunctionRegistry(new TypeRegistry(), true);
        FunctionInfo function = registry.resolveFunction(QualifiedName.of(signature.getName()), signature.getArgumentTypes(), false);
        assertEquals(function.getArgumentTypes(), ImmutableList.of(BigintType.NAME));
        assertEquals(signature.getReturnType(), TimestampWithTimeZoneType.NAME);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QFunction already registered: custom_add(bigint,bigint):bigint\\E")
    public void testDuplicateFunctions()
    {
        List<FunctionInfo> functions = new FunctionRegistry.FunctionListBuilder(new TypeRegistry())
                .scalar(CustomFunctions.class)
                .getFunctions();

        functions = FluentIterable.from(functions).filter(new Predicate<FunctionInfo>()
        {
            @Override
            public boolean apply(FunctionInfo input)
            {
                return input.getName().toString().equals("custom_add");
            }
        }).toList();

        FunctionRegistry registry = new FunctionRegistry(new TypeRegistry(), true);
        registry.addFunctions(functions, ImmutableMultimap.<OperatorType, FunctionInfo>of());
        registry.addFunctions(functions, ImmutableMultimap.<OperatorType, FunctionInfo>of());
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "'sum' is both an aggregation and a scalar function")
    public void testConflictingScalarAggregation()
            throws Exception
    {
        List<FunctionInfo> functions = new FunctionRegistry.FunctionListBuilder(new TypeRegistry())
                .scalar(ScalarSum.class)
                .getFunctions();

        FunctionRegistry registry = new FunctionRegistry(new TypeRegistry(), true);
        registry.addFunctions(functions, ImmutableMultimap.<OperatorType, FunctionInfo>of());
    }

    @Test
    public void testListingHiddenFunctions()
            throws Exception
    {
        FunctionRegistry registry = new FunctionRegistry(new TypeRegistry(), true);
        List<FunctionInfo> functions = registry.list();
        List<String> names = transform(transform(functions, nameGetter()), toStringFunction());

        assertTrue(names.contains("length"), "Expected function names " + names + " to contain 'length'");
        assertTrue(names.contains("stddev"), "Expected function names " + names + " to contain 'stddev'");
        assertTrue(names.contains("rank"), "Expected function names " + names + " to contain 'rank'");
        assertFalse(names.contains("at_time_zone"), "Expected function names " + names + " not to contain 'at_time_zone'");
    }

    public static final class ScalarSum
    {
        private ScalarSum() {}

        @ScalarFunction
        @SqlType(BigintType.NAME)
        public static long sum(@SqlType(BigintType.NAME) long a, @SqlType(BigintType.NAME) long b)
        {
            return a + b;
        }
    }
}
