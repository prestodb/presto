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
package com.facebook.presto.sql.query;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestUnwrapCastInComparison
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testTinyint()
    {
        for (Number from : Arrays.asList(null, Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE)) {
            String fromType = "TINYINT";
            for (String operator : Arrays.asList("=", "<>", ">=", ">", "<=", "<", "IS DISTINCT FROM")) {
                for (Number to : Arrays.asList(null, Byte.MIN_VALUE - 1, Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE, Byte.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "SMALLINT", to);
                }

                for (Number to : Arrays.asList(null, Byte.MIN_VALUE - 1, Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE, Byte.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "INTEGER", to);
                }

                for (Number to : Arrays.asList(null, Byte.MIN_VALUE - 1, Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE, Byte.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "BIGINT", to);
                }

                for (Number to : Arrays.asList(null, Byte.MIN_VALUE - 1, Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE, Byte.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "REAL", to);
                }

                for (Number to : Arrays.asList(null, Byte.MIN_VALUE - 1, Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE, Byte.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "DOUBLE", to);
                }
            }
        }
    }

    @Test
    public void testSmallint()
    {
        for (Number from : Arrays.asList(null, Short.MIN_VALUE, 0, 1, Short.MAX_VALUE)) {
            String fromType = "SMALLINT";
            for (String operator : Arrays.asList("=", "<>", ">=", ">", "<=", "<", "IS DISTINCT FROM")) {
                for (Number to : Arrays.asList(null, Short.MIN_VALUE - 1, Short.MIN_VALUE, 0, 1, Short.MAX_VALUE, Short.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "INTEGER", to);
                }

                for (Number to : Arrays.asList(null, Short.MIN_VALUE - 1, Short.MIN_VALUE, 0, 1, Short.MAX_VALUE, Short.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "BIGINT", to);
                }

                for (Number to : Arrays.asList(null, Short.MIN_VALUE - 1, Short.MIN_VALUE, 0, 1, Short.MAX_VALUE, Short.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "REAL", to);
                }

                for (Number to : Arrays.asList(null, Short.MIN_VALUE - 1, Short.MIN_VALUE, 0, 1, Short.MAX_VALUE, Short.MAX_VALUE + 1)) {
                    validate(operator, fromType, from, "DOUBLE", to);
                }
            }
        }
    }

    @Test
    public void testInteger()
    {
        for (Number from : Arrays.asList(null, Integer.MIN_VALUE, 0, 1, Integer.MAX_VALUE)) {
            String fromType = "INTEGER";
            for (String operator : Arrays.asList("=", "<>", ">=", ">", "<=", "<", "IS DISTINCT FROM")) {
                for (Number to : Arrays.asList(null, Integer.MIN_VALUE - 1L, Integer.MIN_VALUE, 0, 1, Integer.MAX_VALUE, Integer.MAX_VALUE + 1L)) {
                    validate(operator, fromType, from, "BIGINT", to);
                }

                for (Number to : Arrays.asList(null, Integer.MIN_VALUE - 1L, Integer.MIN_VALUE, 0, 0.1, 0.9, 1, Integer.MAX_VALUE, Integer.MAX_VALUE + 1L)) {
                    validate(operator, fromType, from, "DOUBLE", to);
                }
            }
        }
    }

    @Test
    public void testReal()
    {
        String fromType = "REAL";
        String toType = "DOUBLE";

        for (String from : toLiteral(fromType, Arrays.asList(null, Float.NEGATIVE_INFINITY, -Float.MAX_VALUE, 0, 0.1, 0.9, 1, Float.MAX_VALUE, Float.POSITIVE_INFINITY, Float.NaN))) {
            for (String operator : Arrays.asList("=", "<>", ">=", ">", "<=", "<", "IS DISTINCT FROM")) {
                for (String to : toLiteral(toType, Arrays.asList(null, Double.NEGATIVE_INFINITY, Math.nextDown((double) -Float.MIN_VALUE), (double) -Float.MIN_VALUE, 0, 0.1, 0.9, 1, (double) Float.MAX_VALUE, Math.nextUp((double) Float.MAX_VALUE), Double.POSITIVE_INFINITY, Double.NaN))) {
                    validate(operator, fromType, from, toType, to);
                }
            }
        }
    }

    @Test
    public void testDecimal()
    {
        // decimal(15) -> double
        List<String> values = ImmutableList.of("-999999999999999", "999999999999999");
        for (String from : values) {
            for (String operator : Arrays.asList("=", "<>", ">=", ">", "<=", "<", "IS DISTINCT FROM")) {
                for (String to : values) {
                    validate(operator, "DECIMAL(15, 0)", from, "DOUBLE", Double.valueOf(to));
                }
            }
        }

        // decimal(16) -> double
        values = ImmutableList.of("-9999999999999999", "9999999999999999");
        for (String from : values) {
            for (String operator : Arrays.asList("=", "<>", ">=", ">", "<=", "<", "IS DISTINCT FROM")) {
                for (String to : values) {
                    validate(operator, "DECIMAL(16, 0)", from, "DOUBLE", Double.valueOf(to));
                }
            }
        }

        // decimal(7) -> real
        values = ImmutableList.of("-999999", "999999");
        for (String from : values) {
            for (String operator : Arrays.asList("=", "<>", ">=", ">", "<=", "<", "IS DISTINCT FROM")) {
                for (String to : values) {
                    validate(operator, "DECIMAL(7, 0)", from, "REAL", Double.valueOf(to));
                }
            }
        }

        // decimal(8) -> real
        values = ImmutableList.of("-9999999", "9999999");
        for (String from : values) {
            for (String operator : Arrays.asList("=", "<>", ">=", ">", "<=", "<", "IS DISTINCT FROM")) {
                for (String to : values) {
                    validate(operator, "DECIMAL(8, 0)", from, "REAL", Double.valueOf(to));
                }
            }
        }
    }

    @Test
    public void testVarchar()
    {
        for (String from : Arrays.asList(null, "''", "'a'", "'b'")) {
            for (String operator : Arrays.asList("=", "<>", ">=", ">", "<=", "<", "IS DISTINCT FROM")) {
                for (String to : Arrays.asList(null, "''", "'a'", "'aa'", "'b'", "'bb'")) {
                    validate(operator, "VARCHAR(1)", from, "VARCHAR(2)", to);
                }
            }
        }

        // type with no range
        for (String operator : Arrays.asList("=", "<>", ">=", ">", "<=", "<", "IS DISTINCT FROM")) {
            for (String to : Arrays.asList("'" + Strings.repeat("a", 200) + "'", "'" + Strings.repeat("b", 200) + "'")) {
                validate(operator, "VARCHAR(200)", "'" + Strings.repeat("a", 200) + "'", "VARCHAR(300)", to);
            }
        }
    }

    private void validate(String operator, String fromType, Object fromValue, String toType, Object toValue)
    {
        String query = format(
                "SELECT (CAST(v AS %s) %s CAST(%s AS %s)) " +
                        "IS NOT DISTINCT FROM " +
                        "(CAST(%s AS %s) %s CAST(%s AS %s)) " +
                        "FROM (VALUES CAST(%s AS %s)) t(v)",
                toType, operator, toValue, toType,
                fromValue, toType, operator, toValue, toType,
                fromValue, fromType);

        boolean result = (boolean) assertions.execute(query)
                .getMaterializedRows()
                .get(0)
                .getField(0);

        assertTrue(result, "Query evaluated to false: " + query);
    }

    private static List<String> toLiteral(String type, List<Number> values)
    {
        return values.stream()
                .map(value -> value == null ? "NULL" : type + "'" + value + "'")
                .collect(toImmutableList());
    }
}
