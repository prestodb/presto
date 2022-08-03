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
package com.facebook.presto.spark;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.functions.HiveFunctionNamespacePlugin;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.plugin.memory.MemoryConnectorFactory;
import com.facebook.presto.plugin.memory.MemoryPlugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestAggregations;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.Key;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner;
import static com.google.common.math.DoubleMath.fuzzyEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPrestoSparkAggregations
        extends AbstractTestAggregations
{
    private static final String FUNCTION_PREFIX = "hive.default.";
    private static final String TABLE_NAME = "orders";
    private static final Type INTEGER_ARRAY = new ArrayType(INTEGER);
    private static final Type VARCHAR_ARRAY = new ArrayType(VARCHAR);

    @Override
    protected QueryRunner createQueryRunner()
    {
        PrestoSparkQueryRunner server =  createHivePrestoSparkQueryRunner();
         server.installPlugin(new MemoryPlugin());
         // server.installPlugin(new HiveFunctionNamespacePlugin());
         // server.createCatalog("hive", "hive", ImmutableMap.of());
         server.createCatalog("memory", "memory", ImmutableMap.of());
        FunctionAndTypeManager functionAndTypeManager = server.getInstance(Key.get(FunctionAndTypeManager.class));
        functionAndTypeManager.loadFunctionNamespaceManager(
                "hive-functions",
                "hive",
                Collections.emptyMap());
        return server;
    }

    @Test
    public void test()
    {
        // check(selectF("FB_TRUNC", "'2014-01-01'", "'MM'"), VARCHAR, "2014-01-01");
        check(selectF("FB_ARG_MAX", "1", "2", "3"), INTEGER, 2);
        check(selectF("FB_IS_EMPTY", "'foo'"), BOOLEAN, false);
        check(selectF("FB_ARRAY_SET_INTERSECT", "ARRAY [3,2,1]", "ARRAY [2,5,6]", "ARRAY[2]"), INTEGER_ARRAY, asList(2));
    }

    private static String selectF(String function, String... args)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ").append(FUNCTION_PREFIX).append(function);
        builder.append("(");
        if (args != null) {
            builder.append(String.join(", ", args));
        }
        builder.append(")"); //.append(" FROM ").append(TABLE_NAME);
        return builder.toString();
    }

    public void check(String query, Type expectedType, Object expectedValue)
    {
        MaterializedResult result = getQueryRunner().execute(query);
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), expectedType);
        Object actual = result.getMaterializedRows().get(0).getField(0);

        if (expectedType.equals(DOUBLE) || expectedType.equals(RealType.REAL)) {
            if (expectedValue == null) {
                assertNaN(actual);
            }
            else {
                assertTrue(fuzzyEquals(((Number) actual).doubleValue(), ((Number) expectedValue).doubleValue(), 0.000001));
            }
        }
        else {
            assertEquals(actual, expectedValue);
        }
    }

    private static void assertNaN(Object o)
    {
        if (o instanceof Double) {
            assertEquals(((Double) o).doubleValue(), Double.NaN);
        }
        else if (o instanceof Float) {
            assertEquals(((Float) o).floatValue(), Float.NaN);
        }
        else {
            fail("Unexpected " + o);
        }
    }
}
