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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutor;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.functionNamespace.testing.InMemoryFunctionNamespaceManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.function.FunctionImplementationType.THRIFT;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.SQL;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFunctionResolution
{
    private static final RoutineCharacteristics.Language JAVA = new RoutineCharacteristics.Language("java");

    private FunctionResolution functionResolution;
    private FunctionAndTypeManager functionAndTypeManager;

    @BeforeClass
    public void setup()
    {
        functionAndTypeManager = createTestFunctionAndTypeManager();
        functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Test
    public void testStandardFunctionResolution()
    {
        StandardFunctionResolution standardFunctionResolution = functionResolution;

        // not
        assertTrue(standardFunctionResolution.isNotFunction(standardFunctionResolution.notFunction()));

        // negate
        assertTrue(standardFunctionResolution.isNegateFunction(standardFunctionResolution.negateFunction(DOUBLE)));
        assertFalse(standardFunctionResolution.isNotFunction(standardFunctionResolution.negateFunction(DOUBLE)));

        // arithmetic
        assertTrue(standardFunctionResolution.isArithmeticFunction(standardFunctionResolution.arithmeticFunction(ADD, DOUBLE, DOUBLE)));
        assertFalse(standardFunctionResolution.isComparisonFunction(standardFunctionResolution.arithmeticFunction(ADD, DOUBLE, DOUBLE)));

        // comparison
        assertTrue(standardFunctionResolution.isComparisonFunction(standardFunctionResolution.comparisonFunction(GREATER_THAN, DOUBLE, DOUBLE)));
        assertFalse(standardFunctionResolution.isArithmeticFunction(standardFunctionResolution.comparisonFunction(GREATER_THAN, DOUBLE, DOUBLE)));

        // between
        assertTrue(standardFunctionResolution.isBetweenFunction(standardFunctionResolution.betweenFunction(DOUBLE, DOUBLE, DOUBLE)));
        assertFalse(standardFunctionResolution.isNotFunction(standardFunctionResolution.betweenFunction(DOUBLE, DOUBLE, DOUBLE)));

        // subscript
        assertTrue(standardFunctionResolution.isSubscriptFunction(standardFunctionResolution.subscriptFunction(new ArrayType(DOUBLE), BIGINT)));
        assertFalse(standardFunctionResolution.isBetweenFunction(standardFunctionResolution.subscriptFunction(new ArrayType(DOUBLE), BIGINT)));

        // BuiltInFunction
        assertEquals(standardFunctionResolution.notFunction(), standardFunctionResolution.lookupBuiltInFunction("not", ImmutableList.of(BOOLEAN)));
        assertEquals(standardFunctionResolution.countFunction(), standardFunctionResolution.lookupBuiltInFunction("count", ImmutableList.of()));

        // full qualified name
        assertEquals(standardFunctionResolution.notFunction(), standardFunctionResolution.lookupFunction("presto", "default", "not", ImmutableList.of(BOOLEAN)));
        assertEquals(standardFunctionResolution.countFunction(), standardFunctionResolution.lookupFunction("presto", "default", "count", ImmutableList.of()));

        // lookup cast
        assertTrue(standardFunctionResolution.isCastFunction(standardFunctionResolution.lookupCast("CAST", BIGINT, VARCHAR)));
    }

    @Test
    public void testLookupFunctionWithNonDefaultSchemaAndCatalog()
    {
        StandardFunctionResolution standardFunctionResolution = functionResolution;

        functionAndTypeManager.addFunctionNamespace(
                "custom_catalog",
                new InMemoryFunctionNamespaceManager(
                        "custom_catalog",
                        new SqlFunctionExecutors(
                                ImmutableMap.of(
                                        SQL, FunctionImplementationType.SQL,
                                        JAVA, THRIFT),
                                new NoopSqlFunctionExecutor()),
                        new SqlInvokedFunctionNamespaceManagerConfig().setSupportedFunctionLanguages("sql,java")));

        QualifiedObjectName customNotFunction = QualifiedObjectName.valueOf("custom_catalog", "custom_schema", "custom_not");
        SqlInvokedFunction notFunc = new SqlInvokedFunction(
                customNotFunction,
                ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.BOOLEAN))),
                parseTypeSignature(StandardTypes.BOOLEAN),
                "custom_not(x)",
                RoutineCharacteristics.builder().setLanguage(JAVA).setDeterminism(DETERMINISTIC).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
                "",
                notVersioned());

        QualifiedObjectName customCountFunction = QualifiedObjectName.valueOf("custom_catalog", "custom_schema", "custom_count");
        SqlInvokedFunction countFunc = new SqlInvokedFunction(
                customCountFunction,
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BIGINT),
                "custom_count()",
                RoutineCharacteristics.builder().setLanguage(JAVA).setNullCallClause(RETURNS_NULL_ON_NULL_INPUT).build(),
                "",
                notVersioned());

        functionAndTypeManager.createFunction(notFunc, true);
        functionAndTypeManager.createFunction(countFunc, true);

        assertEquals(notFunc.getFunctionId().getFunctionName(),
                functionAndTypeManager.getFunctionMetadata(standardFunctionResolution.lookupFunction("custom_catalog", "custom_schema", "custom_not", ImmutableList.of(BOOLEAN))).getName());
        assertEquals(countFunc.getFunctionId().getFunctionName(),
                functionAndTypeManager.getFunctionMetadata(standardFunctionResolution.lookupFunction("custom_catalog", "custom_schema", "custom_count", ImmutableList.of())).getName());
    }
}
