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
package com.facebook.presto.hive.functions;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.math.BigDecimal;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.math.DoubleMath.fuzzyEquals;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@SuppressWarnings("UnknownLanguage")
public class TestHiveScalarFunctions
        extends AbstractTestHiveFunctions
{
    private static final String FUNCTION_PREFIX = "hive.default.";
    private static final String TABLE_NAME = "memory.default.function_testing";
    private static final Type INTEGER_ARRAY = new ArrayType(INTEGER);
    private static final Type VARCHAR_ARRAY = new ArrayType(VARCHAR);

    @Override
    protected Optional<File> getInitScript()
    {
        return Optional.of(new File("src/test/sql/function-testing.sql"));
    }

    @Test
    public void genericFunction()
    {
        check(select("isnull", "null"), BOOLEAN, true);
        check(select("isnull", "1"), BOOLEAN, false);
        check(selectF("isnull", "c_varchar_null"), BOOLEAN, true);
        check(select("isnotnull", "1"), BOOLEAN, true);
        check(select("isnotnull", "null"), BOOLEAN, false);
        check(selectF("isnotnull", "c_varchar_null"), BOOLEAN, false);
        check(select("nvl", "null", "'2'"), VARCHAR, "2");
        check(selectF("nvl", "c_varchar_null", "'2'"), VARCHAR, "2");

        // Primitive numbers
        check(selectF("abs", "c_bigint"), BIGINT, 1L);
        check(selectF("abs", "c_integer"), INTEGER, 1);
        check(selectF("abs", "c_smallint"), INTEGER, 1);
        check(selectF("abs", "c_tinyint"), INTEGER, 1);
        check(selectF("abs", "c_decimal_52"), createDecimalType(5, 2), BigDecimal.valueOf(12345, 2));
        check(selectF("abs", "c_real"), DOUBLE, 123.45f);
        check(selectF("abs", "c_double"), DOUBLE, 123.45);

        // Primitive string
        check(selectF("upper", "c_varchar"), VARCHAR, "VARCHAR");
        check(selectF("upper", "c_varchar_10"), VARCHAR, "VARCHAR10");
        check(selectF("upper", "c_char_10"), VARCHAR, "CHAR10");
    }

    @Test
    public void genericArrayFunction()
    {
        check(selectF("coalesce", "c_varchar_null", "c_varchar_a", "c_varchar_10"), VARCHAR, "a");
        check(select("coalesce", "null", "10"), INTEGER, 10);

        check(select("size", "null"), INTEGER, -1);
        check(select("size", "array []"), INTEGER, 0);
        check(select("size", "array [1, 2]"), INTEGER, 2);
        check(select("size", "array [array [1, 2], null][1]"), INTEGER, 2);
        check(select("size", "array [array [1, 2], null][2]"), INTEGER, -1);

        check(select("array", "1", "2"), INTEGER_ARRAY, asList(1, 2));
        check(select("array", "'1'", "'2'"), VARCHAR_ARRAY, asList("1", "2"));
        check(selectF("array", "c_varchar", "c_varchar_10"), VARCHAR_ARRAY, asList("varchar", "varchar10"));

        check(selectF("array_contains", "c_array_integer", "2"), BOOLEAN, true);
        check(selectF("array_contains", "c_array_integer", "4"), BOOLEAN, false);
        check(selectF("array_contains", "c_array_varchar", "cast('a' as VARCHAR)"), BOOLEAN, true);
        check(selectF("array_contains", "c_array_varchar", "c_varchar_a"), BOOLEAN, true);
        check(selectF("array_contains", "c_array_varchar", "c_varchar_z"), BOOLEAN, false);

        check(selectF("sort_array", "ARRAY ['b', 'd', 'c', 'a']"), VARCHAR_ARRAY, asList("a", "b", "c", "d"));
        check(selectF("sort_array", "ARRAY [2, 4, 3, 1]"), INTEGER_ARRAY, asList(1, 2, 3, 4));

        check(select("split", "'oneAtwoBthree'", "'[ABC]'"), VARCHAR_ARRAY, asList("one", "two", "three"));

        check(select("concat", "'aa'", "'bb'"), VARCHAR, "aabb");
        check(select("concat_ws", "'.'", "'www'", "ARRAY ['facebook', 'com']"), VARCHAR, "www.facebook.com");
        check(select("elt", "1", "'face'", "'book'"), VARCHAR, "face");
        check(select("index", "ARRAY ['face', 'book']", "1"), VARCHAR, "book");
        check(selectF("index", "c_array_varchar", "1"), VARCHAR, "b");
    }

    @Test
    public void genericDateTimeFunction()
    {
        check(selectF("add_months", "c_date", "1"), VARCHAR, "2020-05-28");
        check(selectF("add_months", "c_timestamp", "1"), VARCHAR, "2020-05-28");
        check(select("add_months", "'2009-08-31'", "1"), VARCHAR, "2009-09-30");
        check(select("add_months", "'2009-08-31 12:01:05'", "1"), VARCHAR, "2009-09-30");
        check(select("datediff", "'2009-07-30'", "'2009-07-31'"), INTEGER, -1);
        check(select("datediff", "'2009-07-30 12:01:05'", "'2009-07-31 12:01:05'"), INTEGER, -1);
    }

    @Test
    public void genericMapFunction()
    {
        check(selectF("map_values", "c_map_varchar_integer"), INTEGER_ARRAY, asList(1, 2, 3));
        check(selectF("map_values", "c_map_varchar_varchar"), VARCHAR_ARRAY, asList("1", "2", "3"));

        check(selectF("index", "c_map_string_string", "cast('a' as varchar)"), VARCHAR, "1");
        check(selectF("index", "c_map_varchar_integer", "'a'"), INTEGER, 1);
        check(selectF("index", "c_map_varchar_varchar", "'a'"), VARCHAR, "1");

        check(select("str_to_map", "'a:1,b:2'"), typeOf("map(varchar,varchar)"),
                ImmutableMap.of("a", "1", "b", "2"));
        check(select("str_to_map", "'a=1;b=2'", "';'", "'='"), typeOf("map(varchar,varchar)"),
                ImmutableMap.of("a", "1", "b", "2"));

        check(select("struct", "'a'", "1"), typeOf("row(col1 varchar,col2 integer)"), asList("a", 1));
    }

    public void check(@Language("SQL") String query, Type expectedType, Object expectedValue)
    {
        MaterializedResult result = client.execute(query).getResult();
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

    private Type typeOf(String signature)
    {
        return typeManager.getType(parseTypeSignature(signature));
    }

    private static void assertNaN(Object o)
    {
        if (o instanceof Double) {
            assertEquals((Double) o, Double.NaN);
        }
        else if (o instanceof Float) {
            assertEquals((Float) o, Float.NaN);
        }
        else {
            fail("Unexpected " + o);
        }
    }

    private static String select(String function, String... args)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ").append(FUNCTION_PREFIX).append(function);
        builder.append("(");
        if (args != null) {
            builder.append(String.join(", ", args));
        }
        builder.append(")");
        return builder.toString();
    }

    private static String selectF(String function, String... args)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ").append(FUNCTION_PREFIX).append(function);
        builder.append("(");
        if (args != null) {
            builder.append(String.join(", ", args));
        }
        builder.append(")").append(" FROM ").append(TABLE_NAME);
        return builder.toString();
    }
}
