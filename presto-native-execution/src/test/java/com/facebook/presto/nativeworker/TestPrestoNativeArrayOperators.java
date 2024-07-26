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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.base.Strings;
import org.testng.annotations.Test;

import java.text.ParseException;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.SystemSessionProperties.FIELD_NAMES_IN_JSON_CAST_ENABLED;
import static com.facebook.presto.operator.aggregation.TypedSet.MAX_FUNCTION_MEMORY;
import static java.lang.Math.toIntExact;

public class TestPrestoNativeArrayOperators
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(false);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @Test
    public void testTypeConstructor()
    {
        assertQuery("select ARRAY[7]");
        assertQuery("select ARRAY[12.34E0, 56.78E0]");
    }

    @Test
    public void testArrayElements()
    {
        assertQuery("select CAST(ARRAY [null] AS ARRAY<INTEGER>)");
        assertQuery("select CAST(ARRAY [1, 2, 3] AS ARRAY<INTEGER>)");
        assertQuery("select CAST(ARRAY [1, null, 3] AS ARRAY<INTEGER>)");

        assertQuery("select CAST(ARRAY [null] AS ARRAY<BIGINT>)");
        assertQuery("select CAST(ARRAY [1, 2, 3] AS ARRAY<BIGINT>)");
        assertQuery("select CAST(ARRAY [1, null, 3] AS ARRAY<BIGINT>)");

        assertQuery("select CAST(ARRAY [1, 2, 3] AS ARRAY<DOUBLE>)");
        assertQuery("select CAST(ARRAY [1, null, 3] AS ARRAY<DOUBLE>)");

        assertQuery("select CAST(ARRAY ['1', '2'] AS ARRAY<VARCHAR>)");
        assertQuery("select CAST(ARRAY ['1', '2'] AS ARRAY<DOUBLE>)");

        assertQuery("select CAST(ARRAY [true, false] AS ARRAY<BOOLEAN>)");
        assertQuery("select CAST(ARRAY [true, false] AS ARRAY<VARCHAR>)");
        assertQuery("select CAST(ARRAY [1, 0] AS ARRAY<BOOLEAN>)");

        assertQuery("select CAST(ARRAY [ARRAY[1], ARRAY[2, 3]] AS ARRAY<ARRAY<DOUBLE>>)");

        assertQuery("select CAST(ARRAY [ARRAY[1.0], ARRAY[2.0, 3.0]] AS ARRAY<ARRAY<DOUBLE>>)");
        assertQuery("select CAST(ARRAY [ARRAY[1.0E0], ARRAY[2.0E0, 3.0E0]] AS ARRAY<ARRAY<DECIMAL(2,1)>>)");

        Session session = Session.builder(getSession()).setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "true").build();
        assertQuery(session, "select CAST(ARRAY [ARRAY[1.0E0], ARRAY[2.0E0, 3.0E0]] AS ARRAY<ARRAY<DECIMAL(20,10)>>)");

        assertQueryFails("select CAST(ARRAY [1, null, 3] AS ARRAY<TIMESTAMP>)",
                ".*Cannot cast array\\(integer\\) to array\\(timestamp\\)");
        assertQueryFails("select CAST(ARRAY [1, null, 3] AS ARRAY<ARRAY<TIMESTAMP>>)",
                ".*Cannot cast array\\(integer\\) to array\\(array\\(timestamp\\)\\)");
        assertQueryFails("select CAST(ARRAY ['puppies', 'kittens'] AS ARRAY<BIGINT>)",
                "Cannot cast 'puppies' to BIGINT");
    }

    @Test
    public void testArraySize()
    {
        int size = toIntExact(MAX_FUNCTION_MEMORY.toBytes() + 1);
        assertQueryFails(
                "array_distinct(ARRAY['" +
                        Strings.repeat("x", size) + "', '" +
                        Strings.repeat("y", size) + "', '" +
                        Strings.repeat("z", size) +
                        "'])",
                "Query text length \\(12582948\\) exceeds the maximum length \\(1000000\\)");
    }

    @Test
    public void testArrayToJson()
    {
        Session session = Session.builder(getSession()).setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "true").build();

        assertQuery(session, "select cast(cast (null as ARRAY<BIGINT>) AS JSON)");
        assertQuery(session, "select cast(ARRAY[] AS JSON)");
        assertQuery(session, "select cast(ARRAY[null, null] AS JSON)");

        assertQuery(session, "select cast(ARRAY[true, false, null] AS JSON)");

        assertQuery(session, "select cast(cast(ARRAY[1, 2, null] AS ARRAY<TINYINT>) AS JSON)");
        assertQuery(session, "select cast(cast(ARRAY[12345, -12345, null] AS ARRAY<SMALLINT>) AS JSON)");
        assertQuery(session, "select cast(cast(ARRAY[123456789, -123456789, null] AS ARRAY<INTEGER>) AS JSON)");
        assertQuery(session, "select cast(cast(ARRAY[1234567890123456789, -1234567890123456789, null] AS ARRAY<BIGINT>) AS JSON)");

        assertQuery(session, "select CAST(CAST(ARRAY[3.14E0, nan(), infinity(), -infinity(), null] AS ARRAY<REAL>) AS JSON)");
        assertQuery(session, "select CAST(ARRAY[3.14E0, 1e-323, 1e308, nan(), infinity(), -infinity(), null] AS JSON)");
        assertQuery(session, "select CAST(ARRAY[DECIMAL '3.14', null] AS JSON)");
        assertQuery(session, "select CAST(ARRAY[DECIMAL '12345678901234567890.123456789012345678', null] AS JSON)");

        assertQuery(session, "select cast(ARRAY['a', 'bb', null] AS JSON)");
        assertQuery(session, "select cast(ARRAY[JSON '123', JSON '3.14', JSON 'false', JSON '\"abc\"', JSON '[1, \"a\", null]', JSON '{\"a\": 1, \"b\": \"str\", \"c\": null}', JSON 'null', null] AS JSON)");

        assertQuery(session, "select CAST(ARRAY[TIMESTAMP '1970-01-01 00:00:01', null] AS JSON)");
        assertQuery(session, "select CAST(ARRAY[DATE '2001-08-22', DATE '2001-08-23', null] AS JSON)");
        assertQuery(session, "select cast(ARRAY[ARRAY[1, 2], ARRAY[3, null], ARRAY[], ARRAY[null, null], null] AS JSON)");
        assertQuery(session, "select cast(ARRAY[MAP(ARRAY['b', 'a'], ARRAY[2, 1]), MAP(ARRAY['three', 'none'], ARRAY[3, null]), MAP(), MAP(ARRAY['h2', 'h1'], ARRAY[null, null]), null] AS JSON)");
        assertQuery(session, "select cast(ARRAY[ROW(1, 2), ROW(3, CAST(null as INTEGER)), CAST(ROW(null, null) AS ROW(INTEGER, INTEGER)), null] AS JSON)");
        assertQuery(session, "select CAST(ARRAY [12345.12345, 12345.12345, 3.0] AS JSON)");
        assertQuery(session, "select CAST(ARRAY [123456789012345678901234567890.87654321, 123456789012345678901234567890.12345678] AS JSON)");
    }

    @Test
    public void testArrayToJsonNoFieldNames()
    {
        assertQuery("select cast(cast (null as ARRAY<BIGINT>) AS JSON)");
        assertQuery("select cast(ARRAY[] AS JSON)");
        assertQuery("select cast(ARRAY[null, null] AS JSON)");

        assertQuery("select cast(ARRAY[true, false, null] AS JSON)");

        assertQuery("select cast(cast(ARRAY[1, 2, null] AS ARRAY<TINYINT>) AS JSON)");
        assertQuery("select cast(cast(ARRAY[12345, -12345, null] AS ARRAY<SMALLINT>) AS JSON)");
        assertQuery("select cast(cast(ARRAY[123456789, -123456789, null] AS ARRAY<INTEGER>) AS JSON)");
        assertQuery("select cast(cast(ARRAY[1234567890123456789, -1234567890123456789, null] AS ARRAY<BIGINT>) AS JSON)");

        assertQuery("select CAST(CAST(ARRAY[3.14E0, nan(), infinity(), -infinity(), null] AS ARRAY<REAL>) AS JSON)");
        assertQuery("select CAST(ARRAY[3.14E0, 1e-323, 1e308, nan(), infinity(), -infinity(), null] AS JSON)");
        assertQuery("select CAST(ARRAY[DECIMAL '3.14', null] AS JSON)");
        assertQuery("select CAST(ARRAY[DECIMAL '12345678901234567890.123456789012345678', null] AS JSON)");

        assertQuery("select cast(ARRAY['a', 'bb', null] AS JSON)");
        assertQuery("select cast(ARRAY[JSON '123', JSON '3.14', JSON 'false', JSON '\"abc\"', JSON '[1, \"a\", null]', JSON '{\"a\": 1, \"b\": \"str\", \"c\": null}', JSON 'null', null] AS JSON)");

        assertQuery("select CAST(ARRAY[TIMESTAMP '1970-01-01 00:00:01', null] AS JSON)");
        assertQuery("select CAST(ARRAY[DATE '2001-08-22', DATE '2001-08-23', null] AS JSON)");

        assertQuery("select cast(ARRAY[ARRAY[1, 2], ARRAY[3, null], ARRAY[], ARRAY[null, null], null] AS JSON)");
        assertQuery("select cast(ARRAY[MAP(ARRAY['b', 'a'], ARRAY[2, 1]), MAP(ARRAY['three', 'none'], ARRAY[3, null]), MAP(), MAP(ARRAY['h2', 'h1'], ARRAY[null, null]), null] AS JSON)");
        assertQuery("select cast(ARRAY[ROW(1, 2), ROW(3, CAST(null as INTEGER)), CAST(ROW(null, null) AS ROW(INTEGER, INTEGER)), null] AS JSON)");
        assertQuery("select CAST(ARRAY [12345.12345, 12345.12345, 3.0] AS JSON)");
        assertQuery("select CAST(ARRAY [123456789012345678901234567890.87654321, 123456789012345678901234567890.12345678] AS JSON)");
    }

    @Test
    public void testJsonToArray()
    {
        // special values
        assertQuery("select CAST(CAST (null AS JSON) AS ARRAY<BIGINT>)");
        assertQuery("select CAST(JSON 'null' AS ARRAY<BIGINT>)");
        assertQuery("select CAST(JSON '[]' AS ARRAY<BIGINT>)");
        assertQuery("select CAST(JSON '[null, null]' AS ARRAY<BIGINT>)");

        // boolean
        assertQuery("select CAST(JSON '[true, false, 12, 0, 12.3, 0.0, \"true\", \"false\", null]' AS ARRAY<BOOLEAN>)");

        // tinyint, smallint, integer, bigint
        assertQuery("select CAST(JSON '[true, false, 12, 12.7, \"12\", null]' AS ARRAY<TINYINT>)");
        assertQuery("select CAST(JSON '[true, false, 12345, 12345.6, \"12345\", null]' AS ARRAY<SMALLINT>)");
        assertQuery("select CAST(JSON '[true, false, 12345678, 12345678.9, \"12345678\", null]' AS ARRAY<INTEGER>)");
        assertQuery("select CAST(JSON '[true, false, 1234567891234567, 1234567891234567.8, \"1234567891234567\", null]' AS ARRAY<BIGINT>)");

        // real, double, decimal
        assertQuery("select CAST(JSON '[true, false, 12345, 12345.67, \"3.14\", \"NaN\", \"Infinity\", \"-Infinity\", null]' AS ARRAY<REAL>)");
        assertQuery("select CAST(JSON '[true, false, 1234567890, 1234567890.1, \"3.14\", \"NaN\", \"Infinity\", \"-Infinity\", null]' AS ARRAY<DOUBLE>)");
        assertQuery("select CAST(JSON '[true, false, 128, 123.456, \"3.14\", null]' AS ARRAY<DECIMAL(10, 5)>)");
        assertQuery("select CAST(JSON '[true, false, 128, 12345678.12345678, \"3.14\", null]' AS ARRAY<DECIMAL(38, 8)>)");

        // varchar, json
        assertQuery("select CAST(JSON '[true, false, 12, 12.3, \"puppies\", \"kittens\", \"null\", \"\", null]' AS ARRAY<VARCHAR>)");
        assertQuery("select CAST(JSON '[5, 3.14, [1, 2, 3], \"e\", {\"a\": \"b\"}, null, \"null\", [null]]' AS ARRAY<JSON>)");

        // nested array/map
        assertQuery("select CAST(JSON '[[1, 2], [3, null], [], [null, null], null]' AS ARRAY<ARRAY<BIGINT>>)");

        // TODO_PRESTISSIMO_FIX - Hitting java.lang.NullPointerException
        // assertQuery("select CAST(JSON '[" +
        //                 "{\"a\": 1, \"b\": 2}, " +
        //                 "{\"none\": null, \"three\": 3}, " +
        //                 "{}, " +
        //                 "{\"h1\": null,\"h2\": null}, " +
        //                 "null]' " +
        //                 "AS ARRAY<MAP<VARCHAR, BIGINT>>)");

        assertQuery("select CAST(JSON '[" +
                "[1, \"two\"], " +
                "[3, null], " +
                "{\"k1\": 1, \"k2\": \"two\"}, " +
                "{\"k2\": null, \"k1\": 3}, " +
                "null]' " +
                "AS ARRAY<ROW(k1 BIGINT, k2 VARCHAR)>)");

        // invalid cast
        assertQueryFails("select CAST(JSON '{\"a\": 1}' AS ARRAY<BIGINT>)", "Cannot cast to array\\(bigint\\). Expected a json array, but got \\{\n\\{\"a\":1\\}");
        assertQueryFails("select CAST(JSON '[1, 2, 3]' AS ARRAY<ARRAY<BIGINT>>)", "Cannot cast to array\\(array\\(bigint\\)\\). Expected a json array, but got 1\n\\[1,2,3\\]");
        assertQueryFails("select CAST(JSON '[1, {}]' AS ARRAY<BIGINT>)", "Cannot cast to array\\(bigint\\). Unexpected token when cast to bigint: \\{\n\\[1,\\{\\}\\]");
        assertQueryFails("select CAST(JSON '[[1], {}]' AS ARRAY<ARRAY<BIGINT>>)", "Cannot cast to array\\(array\\(bigint\\)\\). Expected a json array, but got \\{\n\\[\\[1\\],\\{\\}\\]");

        // TODO_PRESTISSIMO_FIX
        // assertQueryFails("select CAST(unchecked_to_json('1, 2, 3') AS ARRAY<BIGINT>)", "Cannot cast to array\\(bigint\\).\n1, 2, 3");
        // assertQueryFails("select CAST(unchecked_to_json('[1] 2') AS ARRAY<BIGINT>)", "Cannot cast to array\\(bigint\\). Unexpected trailing token: 2\n\\[1\\] 2");
        // assertQueryFails("select CAST(unchecked_to_json('[1, 2, 3') AS ARRAY<BIGINT>)", "Cannot cast to array\\(bigint\\).\n\\[1, 2, 3");

        assertQueryFails("select CAST(JSON '[\"a\", \"b\"]' AS ARRAY<BIGINT>)", "Cannot cast to array\\(bigint\\). Cannot cast 'a' to BIGINT\n\\[\"a\",\"b\"\\]");
        assertQueryFails("select CAST(JSON '[1234567890123.456]' AS ARRAY<INTEGER>)", "Cannot cast to array\\(integer\\). Unable to cast 1.234567890123456E12 to integer\n\\[1.234567890123456E12\\]");

        assertQuery("select CAST(JSON '[1, 2.0, 3]' AS ARRAY(DECIMAL(10,5)))");
        assertQuery("select CAST(CAST(ARRAY [1, 2.0, 3] as JSON) AS ARRAY(DECIMAL(10,5)))");
        assertQuery("select CAST(CAST(ARRAY [123456789012345678901234567890.12345678, 1.2] as JSON) AS ARRAY(DECIMAL(38,8)))");
        assertQuery("select CAST(CAST(ARRAY [12345.87654] as JSON) AS ARRAY(DECIMAL(7,2)))");
        assertQueryFails("select CAST(CAST(ARRAY [12345.12345] as JSON) AS ARRAY(DECIMAL(6,2)))", "Cannot cast to array\\(decimal\\(6,2\\)\\). Cannot cast input json to DECIMAL\\(6,2\\)\n" +
                "\\[12345.12345\\]");
    }

    @Test
    public void testConstructor()
    {
        assertQuery("select ARRAY []");
        assertQuery("select ARRAY [NULL]");
        assertQuery("select ARRAY [1, 2, 3]");
        assertQuery("select ARRAY [1, NULL, 3]");
        assertQuery("select ARRAY [NULL, 2, 3]");
        assertQuery("select ARRAY [1, 2.0E0, 3]");
        assertQuery("select ARRAY [ARRAY[1, 2], ARRAY[3]]");
        assertQuery("select ARRAY [ARRAY[1, 2], NULL, ARRAY[3]]");
        assertQuery("select ARRAY [BIGINT '1', 2, 3]");
        assertQuery("select ARRAY [1, CAST (NULL AS BIGINT), 3]");
        assertQuery("select ARRAY [NULL, 20000000000, 30000000000]");
        assertQuery("select ARRAY [1, 2.0E0, 3]");
        assertQuery("select ARRAY [ARRAY[1, 2], ARRAY[3]]");
        assertQuery("select ARRAY [ARRAY[1, 2], NULL, ARRAY[3]]");
        assertQuery("select ARRAY [ARRAY[1, 2], NULL, ARRAY[BIGINT '3']]");
        assertQuery("select ARRAY [1.0E0, 2.5E0, 3.0E0]");
        assertQuery("select ARRAY [1, 2.5E0, 3]");
        assertQuery("select ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY [TRUE, FALSE]");
        assertQuery("select ARRAY [TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01']");
        assertQuery("select ARRAY [sqrt(-1)]");
        assertQuery("select ARRAY [pow(infinity(), 2)]");
        assertQuery("select ARRAY [pow(-infinity(), 1)]");
        assertQuery("select ARRAY [ARRAY [], NULL]");
        assertQuery("select ARRAY [ARRAY[1.0], ARRAY[2.0, 3.0]]");
        assertQuery("select ARRAY[1.0, 2.0, 3.11]");
        assertQuery("select ARRAY[1, 2.0, 3.11]");
        assertQuery("select ARRAY [ARRAY[1.0], ARRAY[2.0, 123456789123456.789]]");
    }

    @Test
    public void testArrayToArrayConcat()
    {
        assertQuery("select ARRAY [1, NULL] || ARRAY [3]");
        assertQuery("select ARRAY [1, 2] || ARRAY[3, 4]");
        assertQuery("select ARRAY [1, 2] || ARRAY[3, BIGINT '4']");
        assertQuery("select ARRAY [1, 2] || ARRAY[3, 40000000000]");
        assertQuery("select ARRAY [NULL] || ARRAY[NULL]");
        assertQuery("select ARRAY ['puppies'] || ARRAY ['kittens']");
        assertQuery("select ARRAY [TRUE] || ARRAY [FALSE]");
        assertQuery("select concat(ARRAY [1] , ARRAY[2,3])");
        assertQuery("select ARRAY [TIMESTAMP '1970-01-01 00:00:01'] || ARRAY[TIMESTAMP '1973-07-08 22:00:01']");
        assertQuery("select ARRAY [ARRAY[ARRAY[1]]] || ARRAY [ARRAY[ARRAY[2]]]");
        assertQuery("select ARRAY [] || ARRAY []");
        assertQuery("select ARRAY [TRUE] || ARRAY [FALSE] || ARRAY [TRUE]");
        assertQuery("select ARRAY [1] || ARRAY [2] || ARRAY [3] || ARRAY [4]");
        assertQuery("select ARRAY [1] || ARRAY [2.0E0] || ARRAY [3] || ARRAY [4.0E0]");
        assertQuery("select ARRAY [ARRAY [1], ARRAY [2, 8]] || ARRAY [ARRAY [3, 6], ARRAY [4]]");
        assertQuery("select ARRAY[1.0] || ARRAY [2.0, 3.11]");
        assertQuery("select ARRAY[1.0] || ARRAY [2.0] || ARRAY [123456789123456.789]");

        // Tests for concatenating multiple arrays
        List<Object> nullList = Collections.nCopies(2, null);
        assertQuery("select concat(ARRAY[], ARRAY[NULL], ARRAY[], ARRAY[NULL], ARRAY[])");
        assertQuery("select concat(ARRAY[], ARRAY[], ARRAY[], NULL, ARRAY[])");
        assertQuery("select concat(ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[])");
        assertQuery("select concat(ARRAY[], ARRAY[], ARRAY[333], ARRAY[], ARRAY[])");
        assertQuery("select concat(ARRAY[1], ARRAY[2,3], ARRAY[])");
        assertQuery("select concat(ARRAY[1], ARRAY[2,3,3], ARRAY[2,1])");
        assertQuery("select concat(ARRAY[1], ARRAY[], ARRAY[1,2])");
        assertQuery("select concat(ARRAY[], ARRAY[1], ARRAY[], ARRAY[3], ARRAY[], ARRAY[5], ARRAY[])");
        assertQuery("select concat(ARRAY[], ARRAY['123456'], CAST(ARRAY[1,2] AS ARRAY(varchar)), ARRAY[])");

        assertQueryFails("select ARRAY [ARRAY[1]] || ARRAY[ARRAY[true], ARRAY[false]]",
                ".*Unexpected parameters \\(array\\(array\\(integer\\)\\), array\\(array\\(boolean\\)\\)\\) for function concat.*");

        // This query is ambiguous. The result can be [[1], NULL] or [[1], [NULL]] depending on interpretation
        assertQueryFails("select ARRAY [ARRAY [1]] || ARRAY [NULL]",
                ".*Could not choose a best candidate operator. Explicit type casts must be added", true);

        assertQueryFails("select ARRAY [ARRAY [1]] || ARRAY [ARRAY ['x']]",
                ".*Unexpected parameters \\(array\\(array\\(integer\\)\\), array\\(array\\(varchar\\(1\\)\\)\\)\\) for function concat.*");

        assertQuery("select ARRAY [1, NULL] || ARRAY [3]");
    }

    @Test
    public void testElementArrayConcat()
    {
        assertQuery("select CAST (ARRAY [DATE '2001-08-22'] || DATE '2001-08-23' AS JSON)");
        assertQuery("select CAST (DATE '2001-08-23' || ARRAY [DATE '2001-08-22'] AS JSON)");
        assertQuery("select 1 || ARRAY [2]");
        assertQuery("select ARRAY [2] || 1");
        assertQuery("select ARRAY [2] || BIGINT '1'");
        assertQuery("select TRUE || ARRAY [FALSE]");
        assertQuery("select ARRAY [FALSE] || TRUE");
        assertQuery("select 1.0E0 || ARRAY [2.0E0]");
        assertQuery("select ARRAY [2.0E0] || 1.0E0");
        assertQuery("select 'puppies' || ARRAY ['kittens']");
        assertQuery("select ARRAY ['kittens'] || 'puppies'");
        assertQuery("select ARRAY [TIMESTAMP '1970-01-01 00:00:01'] || TIMESTAMP '1973-07-08 22:00:01'");
        assertQuery("select TIMESTAMP '1973-07-08 22:00:01' || ARRAY [TIMESTAMP '1970-01-01 00:00:01']");
        assertQuery("select ARRAY [2, 8] || ARRAY[ARRAY[3, 6], ARRAY[4]]");
        assertQuery("select ARRAY [ARRAY [1], ARRAY [2, 8]] || ARRAY [3, 6]");
        assertQuery("select ARRAY [2.0, 3.11] || 1.0");
        assertQuery("select ARRAY[1.0] || 2.0 || 123456789123456.789");

        assertQueryFails("select ARRAY [ARRAY[1]] || ARRAY ['x']", "Unexpected parameters", true);
        assertQuery("select ARRAY [1, NULL] || 3");
        assertQuery("select 3 || ARRAY [1, NULL]");
    }

    @Test
    public void testArrayContains()
    {
        assertQuery("select CONTAINS(ARRAY ['puppies', 'dogs'], 'dogs')");
        assertQuery("select CONTAINS(ARRAY [1, 2, 3], 2)");
        assertQuery("select CONTAINS(ARRAY [1, BIGINT '2', 3], 2)");
        assertQuery("select CONTAINS(ARRAY [1, 2, 3], BIGINT '2')");
        assertQuery("select CONTAINS(ARRAY [1, 2, 3], 5)");
        assertQuery("select CONTAINS(ARRAY [1, NULL, 3], 1)");
        assertQuery("select CONTAINS(ARRAY [NULL, 2, 3], 1)");
        assertQuery("select CONTAINS(ARRAY [NULL, 2, 3], NULL)");
        assertQuery("select CONTAINS(ARRAY [1, 2.0E0, 3], 3.0E0)");
        assertQuery("select CONTAINS(ARRAY [1.0E0, 2.5E0, 3.0E0], 2.2E0)");
        assertQuery("select CONTAINS(ARRAY ['puppies', 'dogs'], 'dogs')");
        assertQuery("select CONTAINS(ARRAY ['puppies', 'dogs'], 'sharks')");
        assertQuery("select CONTAINS(ARRAY [TRUE, FALSE], TRUE)");
        assertQuery("select CONTAINS(ARRAY [FALSE], TRUE)");
        assertQuery("select CONTAINS(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [3, 4])");
        assertQuery("select CONTAINS(ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [3])");
        assertQuery("select CONTAINS(ARRAY [CAST (NULL AS BIGINT)], 1)");
        assertQuery("select CONTAINS(ARRAY [CAST (NULL AS BIGINT)], NULL)");
        assertQuery("select CONTAINS(ARRAY [], NULL)");
        assertQuery("select CONTAINS(ARRAY [], 1)");
        assertQuery("select CONTAINS(ARRAY [2.2, 1.1], 1.1)");
        assertQuery("select CONTAINS(ARRAY [2.2, 1.1], 1.1)");
        assertQuery("select CONTAINS(ARRAY [2.2, NULL], 1.1)");
        assertQuery("select CONTAINS(ARRAY [2.2, 1.1], 1.2)");
        assertQuery("select CONTAINS(ARRAY [2.2, 1.1], 0000000000001.100)");
        assertQuery("select CONTAINS(ARRAY [2.2, 001.20], 1.2)");
        assertQuery("select CONTAINS(ARRAY [ARRAY [1.1, 2.2], ARRAY [3.3, 4.3]], ARRAY [3.3, 4.300])");
        assertQuery("select CONTAINS(ARRAY [ARRAY [1.1, 2.2], ARRAY [3.3, 4.3]], ARRAY [1.3, null])");

        assertQueryFails("select CONTAINS(ARRAY [ARRAY [1.1, 2.2], ARRAY [3.3, 4.3]], ARRAY [1.1, null])", ".*does not support.*");
        assertQueryFails("select CONTAINS(ARRAY [ARRAY [1.1, null], ARRAY [3.3, 4.3]], ARRAY [1.1, null])", ".*does not support.*");
    }

    @Test
    public void testArrayJoin()
    {
        assertQuery("select ARRAY_JOIN(ARRAY[1, NULL, 2], ',')");
        assertQuery("select ARRAY_JOIN(ARRAY [1, 2, 3], ';', 'N/A')");
        assertQuery("select ARRAY_JOIN(ARRAY [1, 2, null], ';', 'N/A')");
        assertQuery("select ARRAY_JOIN(ARRAY [1, 2, null], ';')");
        assertQuery("select ARRAY_JOIN(ARRAY [1, 2, 3], 'x')");
        assertQuery("select ARRAY_JOIN(ARRAY [BIGINT '1', 2, 3], 'x')");
        assertQuery("select ARRAY_JOIN(ARRAY [null], '=')");
        assertQuery("select ARRAY_JOIN(ARRAY [null,null], '=')");
        assertQuery("select ARRAY_JOIN(ARRAY [], 'S')");
        assertQuery("select ARRAY_JOIN(ARRAY [''], '', '')");
        assertQuery("select ARRAY_JOIN(ARRAY [1, 2, 3, null, 5], ',', '*')");
        assertQuery("select ARRAY_JOIN(ARRAY ['a', 'b', 'c', null, null, 'd'], '-', 'N/A')");
        assertQuery("select ARRAY_JOIN(ARRAY ['a', 'b', 'c', null, null, 'd'], '-')");
        assertQuery("select ARRAY_JOIN(ARRAY [null, null, null, null], 'X')");
        assertQuery("select ARRAY_JOIN(ARRAY [true, false], 'XX')");
        assertQuery("select ARRAY_JOIN(ARRAY [sqrt(-1), infinity()], ',')");
        assertQuery("select ARRAY_JOIN(ARRAY [TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'], '|')");
        assertQuery(
                "select ARRAY_JOIN(ARRAY [null, TIMESTAMP '1970-01-01 00:00:01'], '|')");
        assertQuery(
                "select ARRAY_JOIN(ARRAY [null, TIMESTAMP '1970-01-01 00:00:01'], '|', 'XYZ')");
        assertQuery("select ARRAY_JOIN(ARRAY [1.0, 2.1, 3.3], 'x')");
        assertQuery("select ARRAY_JOIN(ARRAY [1.0, 2.100, 3.3], 'x')");
        assertQuery("select ARRAY_JOIN(ARRAY [1.0, 2.100, NULL], 'x', 'N/A')");
        assertQuery("select ARRAY_JOIN(ARRAY [1.0, DOUBLE '002.100', 3.3], 'x')");

        assertQueryFails("select ARRAY_JOIN(ARRAY [ARRAY [1], ARRAY [2]], '-')", ".*Input type array\\(integer\\) not supported.*");
        assertQueryFails("select ARRAY_JOIN(ARRAY [MAP(ARRAY [1], ARRAY [2])], '-')", ".*Input type map\\(integer,integer\\) not supported.*");
        assertQueryFails("select ARRAY_JOIN(ARRAY [cast(row(1, 2) AS row(col0 bigint, col1 bigint))], '-')", ".*Input type row\\(col0 bigint,col1 bigint\\) not supported.*");
    }

    @Test
    public void testCardinality()
    {
        assertQuery("select CARDINALITY(ARRAY [])");
        assertQuery("select CARDINALITY(ARRAY [NULL])");
        assertQuery("select CARDINALITY(ARRAY [1, 2, 3])");
        assertQuery("select CARDINALITY(ARRAY [1, NULL, 3])");
        assertQuery("select CARDINALITY(ARRAY [1, 2.0E0, 3])");
        assertQuery("select CARDINALITY(ARRAY [ARRAY[1, 2], ARRAY[3]])");
        assertQuery("select CARDINALITY(ARRAY [1.0E0, 2.5E0, 3.0E0])");
        assertQuery("select CARDINALITY(ARRAY ['puppies', 'kittens'])");
        assertQuery("select CARDINALITY(ARRAY [TRUE, FALSE])");
        assertQuery("select CARDINALITY(ARRAY [1.1, 2.2, 3.3])");
        assertQuery("select CARDINALITY(ARRAY [1.1, 33832293522235.23522])");
    }

    @Test
    public void testArrayMin()
    {
        assertQuery("select ARRAY_MIN(ARRAY [])");
        assertQuery("select ARRAY_MIN(ARRAY [NULL])");
        assertQuery("select ARRAY_MIN(ARRAY [NaN()])");
        assertQuery("select ARRAY_MIN(ARRAY [NULL, NULL, NULL])");
        assertQuery("select ARRAY_MIN(ARRAY [NaN(), NaN(), NaN()])");
        assertQuery("select ARRAY_MIN(ARRAY [NULL, 2, 3])");
        assertQuery("select ARRAY_MIN(ARRAY [NaN(), 2, 3])");
        assertQuery("select ARRAY_MIN(ARRAY [NULL, NaN(), 1])");
        assertQuery("select ARRAY_MIN(ARRAY [NaN(), NULL, 3.0])");
        assertQuery("select ARRAY_MIN(ARRAY [1.0E0, NULL, 3])");
        assertQuery("select ARRAY_MIN(ARRAY [1.0, NaN(), 3])");
        assertQuery("select ARRAY_MIN(ARRAY ['1', '2', NULL])");
        assertQuery("select ARRAY_MIN(ARRAY [3, 2, 1])");
        assertQuery("select ARRAY_MIN(ARRAY [1, 2, 3])");
        assertQuery("select ARRAY_MIN(ARRAY [BIGINT '3', 2, 1])");
        assertQuery("select ARRAY_MIN(ARRAY [1, 2.0E0, 3])");
        assertQuery("select ARRAY_MIN(ARRAY [ARRAY[1, 2], ARRAY[3]])");
        assertQuery("select ARRAY_MIN(ARRAY [1.0E0, 2.5E0, 3.0E0])");
        assertQuery("select ARRAY_MIN(ARRAY ['puppies', 'kittens'])");
        assertQuery("select ARRAY_MIN(ARRAY [TRUE, FALSE])");
        assertQuery("select ARRAY_MIN(ARRAY [NULL, FALSE])");
        assertQuery("select ARRAY_MIN(ARRAY [2.1, 2.2, 2.3])");
        assertQuery("select ARRAY_MIN(ARRAY [2.111111222111111114111, 2.22222222222222222, 2.222222222222223])");
        assertQuery("select ARRAY_MIN(ARRAY [1.9, 2, 2.3])");
        assertQuery("select ARRAY_MIN(ARRAY [2.22222222222222222, 2.3])");
    }

    @Test
    public void testArrayMax()
    {
        assertQuery("select ARRAY_MAX(ARRAY [])");
        assertQuery("select ARRAY_MAX(ARRAY [NULL])");
        assertQuery("select ARRAY_MAX(ARRAY [NaN()])");
        assertQuery("select ARRAY_MAX(ARRAY [NULL, NULL, NULL])");
        assertQuery("select ARRAY_MAX(ARRAY [NaN(), NaN(), NaN()])");
        assertQuery("select ARRAY_MAX(ARRAY [NULL, 2, 3])");
        assertQuery("select ARRAY_MAX(ARRAY [NaN(), 2, 3])");
        assertQuery("select ARRAY_MAX(ARRAY [NULL, NaN(), 1])");
        assertQuery("select ARRAY_MAX(ARRAY [NaN(), NULL, 3.0])");
        assertQuery("select ARRAY_MAX(ARRAY [1.0E0, NULL, 3])");
        assertQuery("select ARRAY_MAX(ARRAY [1.0, NaN(), 3])");
        assertQuery("select ARRAY_MAX(ARRAY ['1', '2', NULL])");
        assertQuery("select ARRAY_MAX(ARRAY [3, 2, 1])");
        assertQuery("select ARRAY_MAX(ARRAY [1, 2, 3])");
        assertQuery("select ARRAY_MAX(ARRAY [BIGINT '1', 2, 3])");
        assertQuery("select ARRAY_MAX(ARRAY [1, 2.0E0, 3])");
        assertQuery("select ARRAY_MAX(ARRAY [ARRAY[1, 2], ARRAY[3]])");
        assertQuery("select ARRAY_MAX(ARRAY [1.0E0, 2.5E0, 3.0E0])");
        assertQuery("select ARRAY_MAX(ARRAY ['puppies', 'kittens'])");
        assertQuery("select ARRAY_MAX(ARRAY [TRUE, FALSE])");
        assertQuery("select ARRAY_MAX(ARRAY [NULL, FALSE])");
        assertQuery("select ARRAY_MAX(ARRAY [2.1, 2.2, 2.3])");
        assertQuery("select ARRAY_MAX(ARRAY [2.111111222111111114111, 2.22222222222222222, 2.222222222222223])");
        assertQuery("select ARRAY_MAX(ARRAY [1.9, 2, 2.3])");
        assertQuery("select ARRAY_MAX(ARRAY [2.22222222222222222, 2.3])");
    }

    @Test
    public void testArrayPosition()
    {
        assertQuery("select ARRAY_POSITION(ARRAY [10, 20, 30, 40], 30)");
        assertQuery("select ARRAY_POSITION(CAST (JSON '[]' as array(bigint)), 30)");
        assertQuery("select ARRAY_POSITION(ARRAY [cast(NULL as bigint)], 30)");
        assertQuery("select ARRAY_POSITION(ARRAY [cast(NULL as bigint), NULL, NULL], 30)");
        assertQuery("select ARRAY_POSITION(ARRAY [NULL, NULL, 30, NULL], 30)");

        assertQuery("select ARRAY_POSITION(ARRAY [1.1E0, 2.1E0, 3.1E0, 4.1E0], 3.1E0)");
        assertQuery("select ARRAY_POSITION(ARRAY [false, false, true, true], true)");
        assertQuery("select ARRAY_POSITION(ARRAY ['10', '20', '30', '40'], '30')");

        assertQuery("select ARRAY_POSITION(ARRAY [DATE '2000-01-01', DATE '2000-01-02', DATE '2000-01-03', DATE '2000-01-04'], DATE '2000-01-03')");
        assertQuery("select ARRAY_POSITION(ARRAY [ARRAY [1, 11], ARRAY [2, 12], ARRAY [3, 13], ARRAY [4, 14]], ARRAY [3, 13])");

        assertQuery("select ARRAY_POSITION(ARRAY [], NULL)");
        assertQuery("select ARRAY_POSITION(ARRAY [NULL], NULL)");
        assertQuery("select ARRAY_POSITION(ARRAY [1, NULL, 2], NULL)");
        assertQuery("select ARRAY_POSITION(ARRAY [1, CAST(NULL AS BIGINT), 2], CAST(NULL AS BIGINT))");
        assertQuery("select ARRAY_POSITION(ARRAY [1, NULL, 2], CAST(NULL AS BIGINT))");
        assertQuery("select ARRAY_POSITION(ARRAY [1, CAST(NULL AS BIGINT), 2], NULL)");
        assertQuery("select ARRAY_POSITION(ARRAY [1.0, 2.0, 3.0, 4.0], 3.0)");
        assertQuery("select ARRAY_POSITION(ARRAY [1.0, 2.0, 000000000000000000000003.000, 4.0], 3.0)");
        assertQuery("select ARRAY_POSITION(ARRAY [1.0, 2.0, 3.0, 4.0], 000000000000000000000003.000)");
        assertQuery("select ARRAY_POSITION(ARRAY [1.0, 2.0, 3.0, 4.0], 3)");
        assertQuery("select ARRAY_POSITION(ARRAY [1.0, 2.0, 3, 4.0], 4.0)");
        assertQuery("select ARRAY_POSITION(ARRAY [ARRAY[1]], ARRAY[1])");

        assertQueryFails("select ARRAY_POSITION(ARRAY [ARRAY[null]], ARRAY[1])",
                "array_position does not support arrays with elements that are null or contain null");
        assertQueryFails("select ARRAY_POSITION(ARRAY [ARRAY[null]], ARRAY[null])",
                "array_position does not support arrays with elements that are null or contain null");

        // These should all be valid with respect to the ones above, since 1-index is the default.
        assertQuery("select ARRAY_POSITION(ARRAY [10, 20, 30, 40], 30, 1)");
        assertQuery("select ARRAY_POSITION(CAST (JSON '[]' as array(bigint)), 30, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [cast(NULL as bigint)], 30, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [cast(NULL as bigint), NULL, NULL], 30, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [NULL, NULL, 30, NULL], 30, 1)");

        assertQuery("select ARRAY_POSITION(ARRAY [1.1E0, 2.1E0, 3.1E0, 4.1E0], 3.1E0, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [false, false, true, true], true, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY ['10', '20', '30', '40'], '30', 1)");

        assertQuery("select ARRAY_POSITION(ARRAY [DATE '2000-01-01', DATE '2000-01-02', DATE '2000-01-03', DATE '2000-01-04'], DATE '2000-01-03', 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [ARRAY [1, 11], ARRAY [2, 12], ARRAY [3, 13], ARRAY [4, 14]], ARRAY [3, 13], 1)");

        assertQuery("select ARRAY_POSITION(ARRAY [], NULL, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [NULL], NULL, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [1, NULL, 2], NULL, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [1, CAST(NULL AS BIGINT), 2], CAST(NULL AS BIGINT), 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [1, NULL, 2], CAST(NULL AS BIGINT), 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [1, CAST(NULL AS BIGINT), 2], NULL, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [1.0, 2.0, 3.0, 4.0], 3.0, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [1.0, 2.0, 000000000000000000000003.000, 4.0], 3.0, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [1.0, 2.0, 3.0, 4.0], 000000000000000000000003.000, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [1.0, 2.0, 3.0, 4.0], 3, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [1.0, 2.0, 3, 4.0], 4.0, 1)");
        assertQuery("select ARRAY_POSITION(ARRAY [ARRAY[1]], ARRAY[1], 1)");

        assertQueryFails("select ARRAY_POSITION(ARRAY [ARRAY[null]], ARRAY[1], 1)",
                "array_position does not support arrays with elements that are null or contain null");
        assertQueryFails("select ARRAY_POSITION(ARRAY [ARRAY[null]], ARRAY[null], 1)",
                "array_position does not support arrays with elements that are null or contain null");

        assertQuery("select ARRAY_POSITION(ARRAY[1, 2, 3, 4], 1, -1)");
        assertQuery("select ARRAY_POSITION(ARRAY[4, 3, 2, 1], 1, -1)");
        assertQuery("select ARRAY_POSITION(ARRAY[1, 2, 3, 4, 1], 1, 2)");
        assertQuery("select ARRAY_POSITION(ARRAY[1, 2, 3, 4, 1], 1, -2)");
        assertQuery("select ARRAY_POSITION(ARRAY[1, 2, 3, 4, 1], 1, 3)");
        assertQuery("select ARRAY_POSITION(ARRAY[1, 2, 3, 4, 1], 1, -3)");
        assertQuery("select ARRAY_POSITION(ARRAY[1, 2, 3, 4, 1, 1], 1, 3)");
        assertQuery("select ARRAY_POSITION(ARRAY[1, 2, 3, 4, 1, 1], 1, -1)");
        assertQuery("select ARRAY_POSITION(ARRAY[1, 2, 3, 4, 1, 1], 1, -3)");

        assertQuery("select ARRAY_POSITION(ARRAY[true, false], true, -1)");
        assertQuery("select ARRAY_POSITION(ARRAY[true, false], false, -1)");
        assertQuery("select ARRAY_POSITION(ARRAY[true, true, true, true], true, 3)");
        assertQuery("select ARRAY_POSITION(ARRAY[true, true, true, true], true, 4)");
        assertQuery("select ARRAY_POSITION(ARRAY[true, true, true, true], true, 5)");
        assertQuery("select ARRAY_POSITION(ARRAY[true, true, true, true], true, -2)");
        assertQuery("select ARRAY_POSITION(ARRAY[true, true, true, true], true, -4)");
        assertQuery("select ARRAY_POSITION(ARRAY[true, true, true, true], true, -5)");

        assertQuery("select ARRAY_POSITION(ARRAY[1.0, 2.0, 3.0, 4.0], 1.0, -1)");
        assertQuery("select ARRAY_POSITION(ARRAY[1.0, 2.0, 3.0, 4.0], 2.0, -1)");
        assertQuery("select ARRAY_POSITION(ARRAY[1.0, 2.0, 1.0, 1.0, 2.0], 1.0, 2)");
        assertQuery("select ARRAY_POSITION(ARRAY[1.0, 2.0, 1.0, 1.0, 2.0], 1.0, 3)");
        assertQuery("select ARRAY_POSITION(ARRAY[1.0, 2.0, 1.0, 1.0, 2.0], 1.0, -2)");
        assertQuery("select ARRAY_POSITION(ARRAY[1.0, 2.0, 1.0, 1.0, 2.0], 1.0, -3)");
        assertQuery("select ARRAY_POSITION(ARRAY[1.0, 2.0, 1.0, 1.0, 2.0], 1.0, -6)");
        assertQuery("select ARRAY_POSITION(ARRAY[1.0, 2.0, 1.0, 1.0, 2.0], 1.0, 6)");

        assertQuery("select ARRAY_POSITION(ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]], ARRAY[1], -1)");
        assertQuery("select ARRAY_POSITION(ARRAY[ARRAY[1], ARRAY[2], ARRAY[3]], ARRAY[3], -1)");
        assertQuery("select ARRAY_POSITION(ARRAY[ARRAY[1], ARRAY[2], ARRAY[3], ARRAY[1]], ARRAY[1], -1)");
        assertQuery("select ARRAY_POSITION(ARRAY[ARRAY[1], ARRAY[2], ARRAY[3], ARRAY[1]], ARRAY[1], -2)");
        assertQuery("select ARRAY_POSITION(ARRAY[ARRAY[1], ARRAY[2], ARRAY[3], ARRAY[1]], ARRAY[1], -3)");
        assertQuery("select ARRAY_POSITION(ARRAY[ARRAY[1], ARRAY[2], ARRAY[3], ARRAY[1]], ARRAY[1], 2)");
        assertQuery("select ARRAY_POSITION(ARRAY[ARRAY[1], ARRAY[2], ARRAY[3], ARRAY[1]], ARRAY[1], 3)");

        assertQuery("select ARRAY_POSITION(CAST(ARRAY[] AS ARRAY(BIGINT)), 1, -1)");
        assertQuery("select ARRAY_POSITION(ARRAY[1, 2, 3, 4], null, 2)");
        assertQuery("select ARRAY_POSITION(ARRAY[1, 2, 3, 4], null, -1)");
        assertQuery("select ARRAY_POSITION(ARRAY[1, 2, 3, 4], null, -4)");

        assertQueryFails("select ARRAY_POSITION(ARRAY [ARRAY[null]], ARRAY[1], -1)",
                "array_position does not support arrays with elements that are null or contain null");
        assertQueryFails("select ARRAY_POSITION(ARRAY [ARRAY[null]], ARRAY[null], -1)",
                "array_position does not support arrays with elements that are null or contain null");
        assertQueryFails("select ARRAY_POSITION(ARRAY [1, 2, 3, 4], 4, 0)",
                "array_position cannot take a 0\\-valued instance argument.");
    }

    @Test
    public void testSubscript()
    {
        String outOfBounds = "Array subscript out of bounds";
        String negativeIndex = "Array subscript is negative";
        String indexIsZero = "SQL array indices start at 1";
        assertQueryFails("select ARRAY [][1]", outOfBounds);
        assertQueryFails("select ARRAY [null][-1]", negativeIndex);
        assertQueryFails("select ARRAY [1, 2, 3][0]", indexIsZero);
        assertQueryFails("select ARRAY [1, 2, 3][-1]", negativeIndex);
        assertQueryFails("select ARRAY [1, 2, 3][4]", outOfBounds);

        assertQueryFails("select ARRAY [1, 2, 3][1.1E0]", ".*'\\[\\]' cannot be applied to array\\(integer\\), double");

        assertQuery("select ARRAY[NULL][1]");
        assertQuery("select ARRAY[NULL, NULL, NULL][3]");
        assertQuery("select 1 + ARRAY [2, 1, 3][2]");
        assertQuery("select ARRAY [2, 1, 3][2]");
        assertQuery("select ARRAY [2, NULL, 3][2]");
        assertQuery("select ARRAY [1.0E0, 2.5E0, 3.5E0][3]");
        assertQuery("select ARRAY [ARRAY[1, 2], ARRAY[3]][2]");
        assertQuery("select ARRAY [ARRAY[1, 2], NULL, ARRAY[3]][2]");
        assertQuery("select ARRAY [ARRAY[1, 2], ARRAY[3]][2][1]");
        assertQuery("select ARRAY ['puppies', 'kittens'][2]");
        assertQuery("select ARRAY ['puppies', 'kittens', NULL][3]");
        assertQuery("select ARRAY [TRUE, FALSE][2]");
        assertQuery("select ARRAY [TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'][1]");
        assertQuery("select ARRAY [infinity()][1]");
        assertQuery("select ARRAY [-infinity()][1]");
        assertQuery("select ARRAY [sqrt(-1)][1]");
        assertQuery("select ARRAY [2.1, 2.2, 2.3][3]");
        assertQuery("select ARRAY [2.111111222111111114111, 2.22222222222222222, 2.222222222222223][3]");
        assertQuery("select ARRAY [1.9, 2, 2.3][3]");
        assertQuery("select ARRAY [2.22222222222222222, 2.3][1]");
    }

    @Test
    public void testElementAt()
    {
        assertQueryFails("select ELEMENT_AT(ARRAY [], 0)", "SQL array indices start at 1");
        assertQueryFails("select ELEMENT_AT(ARRAY [1, 2, 3], 0)", "SQL array indices start at 1");

        assertQuery("select ELEMENT_AT(ARRAY [], 1)");
        assertQuery("select ELEMENT_AT(ARRAY [], -1)");
        assertQuery("select ELEMENT_AT(ARRAY [1, 2, 3], 4)");
        assertQuery("select ELEMENT_AT(ARRAY [1, 2, 3], -4)");
        assertQuery("select ELEMENT_AT(ARRAY [NULL], 1)");
        assertQuery("select ELEMENT_AT(ARRAY [NULL], -1)");
        assertQuery("select ELEMENT_AT(ARRAY [NULL, NULL, NULL], 3)");
        assertQuery("select ELEMENT_AT(ARRAY [NULL, NULL, NULL], -1)");
        assertQuery("select 1 + ELEMENT_AT(ARRAY [2, 1, 3], 2)");
        assertQuery("select 10000000000 + ELEMENT_AT(ARRAY [2, 1, 3], -2)");
        assertQuery("select ELEMENT_AT(ARRAY [2, 1, 3], 2)");
        assertQuery("select ELEMENT_AT(ARRAY [2, 1, 3], -2)");
        assertQuery("select ELEMENT_AT(ARRAY [2, NULL, 3], 2)");
        assertQuery("select ELEMENT_AT(ARRAY [2, NULL, 3], -2)");
        assertQuery("select ELEMENT_AT(ARRAY [BIGINT '2', 1, 3], -2)");
        assertQuery("select ELEMENT_AT(ARRAY [2, NULL, BIGINT '3'], -2)");
        assertQuery("select ELEMENT_AT(ARRAY [1.0E0, 2.5E0, 3.5E0], 3)");
        assertQuery("select ELEMENT_AT(ARRAY [1.0E0, 2.5E0, 3.5E0], -1)");
        assertQuery("select ELEMENT_AT(ARRAY [ARRAY [1, 2], ARRAY [3]], 2)");
        assertQuery("select ELEMENT_AT(ARRAY [ARRAY [1, 2], ARRAY [3]], -1)");
        assertQuery("select ELEMENT_AT(ARRAY [ARRAY [1, 2], NULL, ARRAY [3]], 2)");
        assertQuery("select ELEMENT_AT(ARRAY [ARRAY [1, 2], NULL, ARRAY [3]], -2)");
        assertQuery("select ELEMENT_AT(ELEMENT_AT(ARRAY [ARRAY[1, 2], ARRAY [3]], 2) , 1)");
        assertQuery("select ELEMENT_AT(ELEMENT_AT(ARRAY [ARRAY[1, 2], ARRAY [3]], -1) , 1)");
        assertQuery("select ELEMENT_AT(ELEMENT_AT(ARRAY [ARRAY[1, 2], ARRAY [3]], 2) , -1)");
        assertQuery("select ELEMENT_AT(ELEMENT_AT(ARRAY [ARRAY[1, 2], ARRAY [3]], -1) , -1)");
        assertQuery("select ELEMENT_AT(ARRAY ['puppies', 'kittens'], 2)");
        assertQuery("select ELEMENT_AT(ARRAY ['crocodiles', 'kittens'], 2)");
        assertQuery("select ELEMENT_AT(ARRAY ['puppies', 'kittens'], -1)");
        assertQuery("select ELEMENT_AT(ARRAY ['puppies', 'kittens', NULL], 3)");
        assertQuery("select ELEMENT_AT(ARRAY ['puppies', 'kittens', NULL], -1)");
        assertQuery("select ELEMENT_AT(ARRAY [TRUE, FALSE], 2)");
        assertQuery("select ELEMENT_AT(ARRAY [TRUE, FALSE], -1)");
        assertQuery("select ELEMENT_AT(ARRAY [TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'], 1)");
        assertQuery("select ELEMENT_AT(ARRAY [TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'], -2)");
        assertQuery("select ELEMENT_AT(ARRAY [infinity()], 1)");
        assertQuery("select ELEMENT_AT(ARRAY [infinity()], -1)");
        assertQuery("select ELEMENT_AT(ARRAY [-infinity()], 1)");
        assertQuery("select ELEMENT_AT(ARRAY [-infinity()], -1)");
        assertQuery("select ELEMENT_AT(ARRAY [sqrt(-1)], 1)");
        assertQuery("select ELEMENT_AT(ARRAY [sqrt(-1)], -1)");
        assertQuery("select ELEMENT_AT(ARRAY [2.1, 2.2, 2.3], 3)");
        assertQuery("select ELEMENT_AT(ARRAY [2.111111222111111114111, 2.22222222222222222, 2.222222222222223], 3)");
        assertQuery("select ELEMENT_AT(ARRAY [1.9, 2, 2.3], -1)");
        assertQuery("select ELEMENT_AT(ARRAY [2.22222222222222222, 2.3], -2)");
    }

    @Test
    public void testSort()
    {
        assertQuery("select ARRAY_SORT(ARRAY[2, 3, 4, 1])");
        assertQuery("select ARRAY_SORT(ARRAY[2, BIGINT '3', 4, 1])");
        assertQuery("select ARRAY_SORT(ARRAY [2.3, 2.1, 2.2])");
        assertQuery("select ARRAY_SORT(ARRAY [2, 1.900, 2.330])");
        assertQuery("select ARRAY_SORT(ARRAY['z', 'f', 's', 'd', 'g'])");
        assertQuery("select ARRAY_SORT(ARRAY[TRUE, FALSE])");
        assertQuery("select ARRAY_SORT(ARRAY[22.1E0, 11.1E0, 1.1E0, 44.1E0])");
        assertQuery("select ARRAY_SORT(ARRAY [TIMESTAMP '1973-07-08 22:00:01', TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1989-02-06 12:00:00'])");
        assertQuery("select ARRAY_SORT(ARRAY [ARRAY [1], ARRAY [2]])");

        // with lambda function
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY[2, 3, 2, null, null, 4, 1], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN x < y THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "ELSE -1 END)",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY[2, 3, 2, null, null, 4, 1], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN 1 " +
                        "WHEN y IS NULL THEN -1 " +
                        "WHEN x < y THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "ELSE -1 END)",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY[2, null, BIGINT '3', 4, null, 1], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN x < y THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "ELSE -1 END)",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY['bc', null, 'ab', 'dc', null], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN x < y THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "ELSE -1 END)",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY['a', null, 'abcd', null, 'abc', 'zx'], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN length(x) < length(y) THEN 1 " +
                        "WHEN length(x) = length(y) THEN 0 " +
                        "ELSE -1 END)",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY[TRUE, null, FALSE, TRUE, null, FALSE, TRUE], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "WHEN x THEN -1 " +
                        "ELSE 1 END)",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY[22.1E0, null, null, 11.1E0, 1.1E0, 44.1E0], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN x < y THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "ELSE -1 END)",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY[TIMESTAMP '1973-07-08 22:00:01', NULL, TIMESTAMP '1970-01-01 00:00:01', NULL, TIMESTAMP '1989-02-06 12:00:00'], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN date_diff('millisecond', y, x) < 0 THEN 1 " +
                        "WHEN date_diff('millisecond', y, x) = 0 THEN 0 " +
                        "ELSE -1 END)",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY[ARRAY[2, 3, 1], null, ARRAY[4, null, 2, 1, 4], ARRAY[1, 2], null], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN cardinality(x) < cardinality(y) THEN 1 " +
                        "WHEN cardinality(x) = cardinality(y) THEN 0 " +
                        "ELSE -1 END)",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY[2.3, null, 2.1, null, 2.2], (x, y) -> CASE " +
                        "WHEN x IS NULL THEN -1 " +
                        "WHEN y IS NULL THEN 1 " +
                        "WHEN x < y THEN 1 " +
                        "WHEN x = y THEN 0 " +
                        "ELSE -1 END)",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);

        // with null in the array, should be in nulls-last order
        assertQuery("select ARRAY_SORT(ARRAY[1, null, 0, null, -1])");
        assertQuery("select ARRAY_SORT(ARRAY[1, null, null, -1, 0])");

        // invalid functions
        assertQueryFails("select ARRAY_SORT(ARRAY[color('red'), color('blue')])",
                ".*Unexpected parameters \\(array\\(color\\)\\) for function array_sort.*");
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY[2, 1, 2, 4], (x, y) -> y - x)",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY[1, 2], (x, y) -> x / COALESCE(y, 0))",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY[2, 3, 2, 4, 1], (x, y) -> IF(x > y, NULL, IF(x = y, 0, -1)))",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        // TODO_PRESTISSIMO_FIX
        assertQueryFails("select ARRAY_SORT(ARRAY[1, null], (x, y) -> x / COALESCE(y, 0))",
                "array_sort with comparator lambda that cannot be rewritten into a transform is not supported", true);
        assertQueryFails("select ARRAY_SORT(ARRAY[ARRAY[1], ARRAY[null]])",
                "Array contains elements not supported for comparison");
        assertQueryFails("select ARRAY_SORT(ARRAY[ROW(1), ROW(null)])",
                "Array contains elements not supported for comparison");

        assertQuery("select ARRAY_SORT(ARRAY[2, 3, 4, 1])");
    }

    @Test
    public void testReverse()
    {
        assertQuery("select REVERSE(ARRAY[1])");
        assertQuery("select REVERSE(ARRAY[1, 2, 3, 4])");
        assertQuery("select REVERSE(ARRAY_SORT(ARRAY[2, 3, 4, 1]))");
        assertQuery("select REVERSE(ARRAY[2, BIGINT '3', 4, 1])");
        assertQuery("select REVERSE(ARRAY['a', 'b', 'c', 'd'])");
        assertQuery("select REVERSE(ARRAY[TRUE, FALSE])");
        assertQuery("select REVERSE(ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0])");
        assertQuery("select REVERSE(ARRAY[1.1E0, 2.2E0, 3.3E0, 4.4E0])");
    }

    @Test
    public void testDistinct()
    {
        assertQuery("select ARRAY_DISTINCT(ARRAY [])");

        // Order matters here. Result should be stable.
        assertQuery("select ARRAY_DISTINCT(ARRAY [0, NULL])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [0, NULL, 0, NULL])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [2, 3, 4, 3, 1, 2, 3])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [0.0E0, NULL])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [2.2E0, 3.3E0, 4.4E0, 3.3E0, 1, 2.2E0, 3.3E0])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [FALSE, NULL])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [FALSE, TRUE, NULL])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [TRUE, TRUE, TRUE])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [TRUE, FALSE, FALSE, TRUE])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [TIMESTAMP '1973-07-08 22:00:01', TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'])");
        assertQuery("select ARRAY_DISTINCT(ARRAY ['2', '3', '2'])");
        assertQuery("select ARRAY_DISTINCT(ARRAY ['BB', 'CCC', 'BB'])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [ARRAY [1], ARRAY [1, 2], ARRAY [1, 2, 3], ARRAY [1, 2]])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [NULL, 2.2E0, 3.3E0, 4.4E0, 3.3E0, 1, 2.2E0, 3.3E0])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [2, 3, NULL, 4, 3, 1, 2, 3])");
        assertQuery("select ARRAY_DISTINCT(ARRAY ['BB', 'CCC', 'BB', NULL])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [NULL])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [NULL, NULL])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [NULL, NULL, NULL])");

        // Test for BIGINT-optimized implementation
        assertQuery("select ARRAY_DISTINCT(ARRAY [CAST(5 AS BIGINT), NULL, CAST(12 AS BIGINT), NULL])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [CAST(100 AS BIGINT), NULL, CAST(100 AS BIGINT), NULL, 0, -2, 0])");

        assertQuery("select ARRAY_DISTINCT(ARRAY [2.3, 2.3, 2.2])");
        assertQuery("select ARRAY_DISTINCT(ARRAY [2.330, 1.900, 2.330])");

        assertQuery("select ARRAY_DISTINCT(ARRAY[2, 3, 4, 1, 2])");
        assertQuery("select ARRAY_DISTINCT(ARRAY[CAST(5 AS BIGINT), NULL, CAST(12 AS BIGINT), NULL])");
        assertQuery("select ARRAY_DISTINCT(ARRAY[true, true, false, true, false])");
        assertQuery("select ARRAY_DISTINCT(ARRAY['cat', 'dog', 'dog', 'coffee', 'apple'])");
    }

    @Test
    public void testDistinctWithIndeterminateRows()
    {
        assertQuery("select ARRAY_DISTINCT(ARRAY[(123, 'abc'), (123, NULL)])");
        assertQuery("select ARRAY_DISTINCT(ARRAY[(NULL, NULL), (42, 'def'), (NULL, 'abc'), (123, NULL), (42, 'def'), (NULL, NULL), (NULL, 'abc'), (123, NULL)])");
    }

    @Test
    public void testSlice()
    {
        assertQuery("select SLICE(ARRAY [1, 2, 3, 4, 5], 1, 4)");
        assertQuery("select SLICE(ARRAY [1, 2, 3], 1, 3)");
        assertQuery("select SLICE(ARRAY [1, 2], 1, 4)");
        assertQuery("select SLICE(ARRAY [1, 2, 3, 4, 5], 3, 2)");
        assertQuery("select SLICE(ARRAY ['1', '2', '3', '4'], 2, 1)");
        assertQuery("select SLICE(ARRAY [1, 2, 3, 4], 3, 3)");
        assertQuery("select SLICE(ARRAY [1, 2, 3, 4], -3, 3)");
        assertQuery("select SLICE(ARRAY [1, 2, 3, 4], -3, 5)");
        assertQuery("select SLICE(ARRAY [1, 2, 3, 4], 1, 0)");
        assertQuery("select SLICE(ARRAY [1, 2, 3, 4], -2, 0)");
        assertQuery("select SLICE(ARRAY [1, 2, 3, 4], -5, 5)");
        assertQuery("select SLICE(ARRAY [1, 2, 3, 4], -6, 5)");
        assertQuery("select SLICE(ARRAY [ARRAY [1], ARRAY [2, 3], ARRAY [4, 5, 6]], 1, 2)");
        assertQuery("select SLICE(ARRAY [2.3, 2.3, 2.2], 2, 3)");
        assertQuery("select SLICE(ARRAY [2.330, 1.900, 2.330], 1, 2)");

        assertQueryFails("select SLICE(ARRAY [1, 2, 3, 4], 1, -1)", "length must be greater than or equal to 0");
        assertQueryFails("select SLICE(ARRAY [1, 2, 3, 4], 0, 1)", "SQL array indices start at 1");
    }

    @Test
    public void testArraysOverlap()
    {
        assertQuery("select ARRAYS_OVERLAP(ARRAY [1, 2], ARRAY [2, 3])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [2, 1], ARRAY [2, 3])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [2, 1], ARRAY [3, 2])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [1, 2], ARRAY [3, 2])");

        assertQuery("select ARRAYS_OVERLAP(ARRAY [1, 3], ARRAY [2, 4])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [3, 1], ARRAY [2, 4])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [3, 1], ARRAY [4, 2])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [1, 3], ARRAY [4, 2])");

        assertQuery("select ARRAYS_OVERLAP(ARRAY [1, 3], ARRAY [2, 3, 4])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [3, 1], ARRAY [5, 4, 1])");

        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(1 AS BIGINT), 2], ARRAY [CAST(2 AS BIGINT), 3])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(2 AS BIGINT), 1], ARRAY [CAST(2 AS BIGINT), 3])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(2 AS BIGINT), 1], ARRAY [CAST(3 AS BIGINT), 2])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(1 AS BIGINT), 2], ARRAY [CAST(3 AS BIGINT), 2])");

        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(1 AS BIGINT), 3], ARRAY [CAST(2 AS BIGINT), 4])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(3 AS BIGINT), 1], ARRAY [CAST(2 AS BIGINT), 4])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(3 AS BIGINT), 1], ARRAY [CAST(4 AS BIGINT), 2])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(1 AS BIGINT), 3], ARRAY [CAST(4 AS BIGINT), 2])");

        assertQuery("select ARRAYS_OVERLAP(ARRAY ['dog', 'cat'], ARRAY ['monkey', 'dog'])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY ['dog', 'cat'], ARRAY ['monkey', 'fox'])");

        // Test arrays with NULLs
        assertQuery("select ARRAYS_OVERLAP(ARRAY [1, 2], ARRAY [NULL, 2])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [1, 2], ARRAY [2, NULL])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [2, 1], ARRAY [NULL, 3])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [2, 1], ARRAY [3, NULL])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [NULL, 2], ARRAY [1, 2])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [2, NULL], ARRAY [1, 2])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [NULL, 3], ARRAY [2, 1])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [3, NULL], ARRAY [2, 1])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [3, NULL], ARRAY [2, 1, NULL])");

        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(1 AS BIGINT), 2], ARRAY [NULL, CAST(2 AS BIGINT)])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(1 AS BIGINT), 2], ARRAY [CAST(2 AS BIGINT), NULL])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(2 AS BIGINT), 1], ARRAY [CAST(3 AS BIGINT), NULL])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(2 AS BIGINT), 1], ARRAY [NULL, CAST(3 AS BIGINT)])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [NULL, CAST(2 AS BIGINT)], ARRAY [CAST(1 AS BIGINT), 2])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(2 AS BIGINT), NULL], ARRAY [CAST(1 AS BIGINT), 2])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [CAST(3 AS BIGINT), NULL], ARRAY [CAST(2 AS BIGINT), 1])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [NULL, CAST(3 AS BIGINT)], ARRAY [CAST(2 AS BIGINT), 1])");

        assertQuery("select ARRAYS_OVERLAP(ARRAY ['dog', 'cat'], ARRAY [NULL, 'dog'])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY ['dog', 'cat'], ARRAY ['monkey', NULL])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [NULL, 'dog'], ARRAY ['dog', 'cat'])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY ['monkey', NULL], ARRAY ['dog', 'cat'])");

        assertQuery("select ARRAYS_OVERLAP(ARRAY [ARRAY [1, 2], ARRAY[3]], ARRAY [ARRAY[4], ARRAY [1, 2]])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [ARRAY [1, 2], ARRAY[3]], ARRAY [ARRAY[4], NULL])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [ARRAY [2], ARRAY[3]], ARRAY [ARRAY[4], ARRAY[1, 2]])");

        assertQuery("select ARRAYS_OVERLAP(ARRAY [], ARRAY [])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [], ARRAY [1, 2])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [], ARRAY [NULL])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [NULL], ARRAY [])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [NULL], ARRAY [NULL])");

        assertQuery("select ARRAYS_OVERLAP(ARRAY [true], ARRAY [true, false])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [false], ARRAY [true, true])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [true, false], ARRAY [NULL])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [false], ARRAY [true, NULL])");

        assertQuery("select ARRAYS_OVERLAP(ARRAY [2.01], ARRAY [2.01, 9.0, 9.4])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [10.1, 9.1], ARRAY [9.09, 9.0])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [NULL, 9.1], ARRAY [NULL])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [NULL, 9.1], ARRAY [9.1, 10.2, 3.0])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [2.4], ARRAY [2.4])");
        assertQuery("select ARRAYS_OVERLAP(ARRAY [2.4, 9.0, 10.9999999, 9.1, 4.1, 8.1], ARRAY [2.1, 10.999])");
    }

    @Test
    public void testComparison()
    {
        assertQuery("select ARRAY [1, 2, 3] = ARRAY [1, 2, 3]");
        assertQuery("select ARRAY [1, 2, 3] != ARRAY [1, 2, 3]");
        assertQuery("select ARRAY [TRUE, FALSE] = ARRAY [TRUE, FALSE]");
        assertQuery("select ARRAY [TRUE, FALSE] != ARRAY [TRUE, FALSE]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] = ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] != ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0]");
        assertQuery("select ARRAY ['puppies', 'kittens'] = ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY ['puppies', 'kittens'] != ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] = ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] != ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [timestamp '2012-10-31 08:00 UTC'] = ARRAY [timestamp '2012-10-31 01:00 America/Los_Angeles']");
        assertQuery("select ARRAY [timestamp '2012-10-31 08:00 UTC'] != ARRAY [timestamp '2012-10-31 01:00 America/Los_Angeles']");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] = ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] != ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]");
        assertQuery("select ARRAY [1.0, 2.0, 3.0] = ARRAY [1.0, 2.0, 3.0]");
        assertQuery("select ARRAY [1.0, 2.0, 3.0] = ARRAY [1.0, 2.0, 3.1]");
        assertQuery("select ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "= ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]");
        assertQuery("select ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "= ARRAY [1234567890.1234567890, 9876543210.9876543210, 0]");
        assertQuery("select ARRAY [1.0, 2.0, 3.0] != ARRAY [1.0, 2.0, 3.0]");
        assertQuery("select ARRAY [1.0, 2.0, 3.0] != ARRAY [1.0, 2.0, 3.1]");
        assertQuery("select ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "!= ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]");
        assertQuery("select ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "!= ARRAY [1234567890.1234567890, 9876543210.9876543210, 0]");
        assertQuery("select ARRAY [1, 2, null] = ARRAY [1, null]");
        assertQuery("select ARRAY ['1', '2', null] = ARRAY ['1', null]");
        assertQuery("select ARRAY [1.0, 2.0, null] = ARRAY [1.0, null]");
        assertQuery("select ARRAY [1.0E0, 2.0E0, null] = ARRAY [1.0E0, null]");
        assertQuery("select ARRAY [1, 2, null] = ARRAY [1, 2, null]");
        assertQuery("select ARRAY ['1', '2', null] = ARRAY ['1', '2', null]");
        assertQuery("select ARRAY [1.0, 2.0, null] = ARRAY [1.0, 2.0, null]");
        assertQuery("select ARRAY [1.0E0, 2.0E0, null] = ARRAY [1.0E0, 2.0E0, null]");
        assertQuery("select ARRAY [1, 3, null] = ARRAY [1, 2, null]");
        assertQuery("select ARRAY [1E0, 3E0, null] = ARRAY [1E0, 2E0, null]");
        assertQuery("select ARRAY ['1', '3', null] = ARRAY ['1', '2', null]");
        assertQuery("select ARRAY [ARRAY[1], ARRAY[null], ARRAY[2]] = ARRAY [ARRAY[1], ARRAY[2], ARRAY[3]]");
        assertQuery("select ARRAY [ARRAY[1], ARRAY[null], ARRAY[3]] = ARRAY [ARRAY[1], ARRAY[2], ARRAY[3]]");

        assertQuery("select ARRAY [10, 20, 30] != ARRAY [5]");
        assertQuery("select ARRAY [10, 20, 30] = ARRAY [5]");
        assertQuery("select ARRAY [1, 2, 3] != ARRAY [3, 2, 1]");
        assertQuery("select ARRAY [1, 2, 3] = ARRAY [3, 2, 1]");
        assertQuery("select ARRAY [TRUE, FALSE, TRUE] != ARRAY [TRUE]");
        assertQuery("select ARRAY [TRUE, FALSE, TRUE] = ARRAY [TRUE]");
        assertQuery("select ARRAY [TRUE, FALSE] != ARRAY [FALSE, FALSE]");
        assertQuery("select ARRAY [TRUE, FALSE] = ARRAY [FALSE, FALSE]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] != ARRAY [1.1E0, 2.2E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] = ARRAY [1.1E0, 2.2E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0] != ARRAY [11.1E0, 22.1E0, 1.1E0, 44.1E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0] = ARRAY [11.1E0, 22.1E0, 1.1E0, 44.1E0]");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] != ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] = ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY ['puppies', 'kittens'] != ARRAY ['z', 'f', 's', 'd', 'g']");
        assertQuery("select ARRAY ['puppies', 'kittens'] = ARRAY ['z', 'f', 's', 'd', 'g']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] != ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] = ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] != ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] = ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] != ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5, 6]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] = ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5, 6]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] != ARRAY [ARRAY [1, 2, 3], ARRAY [4, 5]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] = ARRAY [ARRAY [1, 2, 3], ARRAY [4, 5]]");
        assertQuery("select ARRAY [1.0, 2.0, 3.0] = ARRAY [1.0, 2.0]");
        assertQuery("select ARRAY [1.0, 2.0, 3.0] != ARRAY [1.0, 2.0]");
        assertQuery("select ARRAY [1, 2, null] != ARRAY [1, 2, null]");
        assertQuery("select ARRAY [1, 2, null] != ARRAY [1, null]");
        assertQuery("select ARRAY [1, 3, null] != ARRAY [1, 2, null]");
        assertQuery("select ARRAY [ARRAY[1], ARRAY[null], ARRAY[2]] != ARRAY [ARRAY[1], ARRAY[2], ARRAY[3]]");
        assertQuery("select ARRAY [ARRAY[1], ARRAY[null], ARRAY[3]] != ARRAY [ARRAY[1], ARRAY[2], ARRAY[3]]");

        assertQuery("select ARRAY [10, 20, 30] < ARRAY [10, 20, 40, 50]");
        assertQuery("select ARRAY [10, 20, 30] >= ARRAY [10, 20, 40, 50]");
        assertQuery("select ARRAY [10, 20, 30] < ARRAY [10, 40]");
        assertQuery("select ARRAY [10, 20, 30] >= ARRAY [10, 40]");
        assertQuery("select ARRAY [10, 20] < ARRAY [10, 20, 30]");
        assertQuery("select ARRAY [10, 20] >= ARRAY [10, 20, 30]");
        assertQuery("select ARRAY [TRUE, FALSE] < ARRAY [TRUE, TRUE, TRUE]");
        assertQuery("select ARRAY [TRUE, FALSE] >= ARRAY [TRUE, TRUE, TRUE]");
        assertQuery("select ARRAY [TRUE, FALSE, FALSE] < ARRAY [TRUE, TRUE]");
        assertQuery("select ARRAY [TRUE, FALSE, FALSE] >= ARRAY [TRUE, TRUE]");
        assertQuery("select ARRAY [TRUE, FALSE] < ARRAY [TRUE, FALSE, FALSE]");
        assertQuery("select ARRAY [TRUE, FALSE] >= ARRAY [TRUE, FALSE, FALSE]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] < ARRAY [1.1E0, 2.2E0, 4.4E0, 4.4E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] >= ARRAY [1.1E0, 2.2E0, 4.4E0, 4.4E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] < ARRAY [1.1E0, 2.2E0, 5.5E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] >= ARRAY [1.1E0, 2.2E0, 5.5E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0] < ARRAY [1.1E0, 2.2E0, 5.5E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0] >= ARRAY [1.1E0, 2.2E0, 5.5E0]");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] < ARRAY ['puppies', 'lizards', 'lizards']");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] >= ARRAY ['puppies', 'lizards', 'lizards']");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] < ARRAY ['puppies', 'lizards']");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] >= ARRAY ['puppies', 'lizards']");
        assertQuery("select ARRAY ['puppies', 'kittens'] < ARRAY ['puppies', 'kittens', 'lizards']");
        assertQuery("select ARRAY ['puppies', 'kittens'] >= ARRAY ['puppies', 'kittens', 'lizards']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] < ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5, 6]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] >= ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5, 6]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] < ARRAY [ARRAY [1, 2], ARRAY [3, 5, 6]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] >= ARRAY [ARRAY [1, 2], ARRAY [3, 5, 6]]");
        assertQuery("select ARRAY [1.0, 2.0, 3.0] > ARRAY [1.0, 2.0, 3.0]");
        assertQuery("select ARRAY [1.0, 2.0, 3.0] >= ARRAY [1.0, 2.0, 3.0]");
        assertQuery("select ARRAY [1.0, 2.0, 3.0] < ARRAY [1.0, 2.0, 3.1]");
        assertQuery("select ARRAY [1.0, 2.0, 3.0] <= ARRAY [1.0, 2.0, 3.1]");
        assertQuery("select ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "> ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]");
        assertQuery("select ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                ">= ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]");
        assertQuery("select ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "< ARRAY [1234567890.1234567890, 9876543210.9876543210, 0]");
        assertQuery("select ARRAY [1234567890.1234567890, 9876543210.9876543210] " +
                "< ARRAY [1234567890.1234567890, 9876543210.9876543210, 0]");
        assertQuery("select ARRAY [1234567890.1234567890, 0] " +
                "< ARRAY [1234567890.1234567890, 9876543210.9876543210, 0]");
        assertQuery("select ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543] " +
                "<= ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543]");

        assertQuery("select ARRAY [10, 20, 30] > ARRAY [10, 20, 20]");
        assertQuery("select ARRAY [10, 20, 30] <= ARRAY [10, 20, 20]");
        assertQuery("select ARRAY [10, 20, 30] > ARRAY [10, 20]");
        assertQuery("select ARRAY [10, 20, 30] <= ARRAY [10, 20]");
        assertQuery("select ARRAY [TRUE, TRUE, TRUE] > ARRAY [TRUE, TRUE, FALSE]");
        assertQuery("select ARRAY [TRUE, TRUE, TRUE] <= ARRAY [TRUE, TRUE, FALSE]");
        assertQuery("select ARRAY [TRUE, TRUE, FALSE] > ARRAY [TRUE, TRUE]");
        assertQuery("select ARRAY [TRUE, TRUE, FALSE] <= ARRAY [TRUE, TRUE]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] > ARRAY [1.1E0, 2.2E0, 2.2E0, 4.4E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] <= ARRAY [1.1E0, 2.2E0, 2.2E0, 4.4E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] > ARRAY [1.1E0, 2.2E0, 3.3E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] <= ARRAY [1.1E0, 2.2E0, 3.3E0]");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] > ARRAY ['puppies', 'kittens', 'kittens']");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] <= ARRAY ['puppies', 'kittens', 'kittens']");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] > ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] <= ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:20.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:20.456 America/Los_Angeles']");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] > ARRAY [ARRAY [1, 2], ARRAY [3, 4]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] <= ARRAY [ARRAY [1, 2], ARRAY [3, 4]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] > ARRAY [ARRAY [1, 2], ARRAY [3, 3, 4]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] <= ARRAY [ARRAY [1, 2], ARRAY [3, 3, 4]]");

        assertQuery("select ARRAY [10, 20, 30] <= ARRAY [50]");
        assertQuery("select ARRAY [10, 20, 30] > ARRAY [50]");
        assertQuery("select ARRAY [10, 20, 30] <= ARRAY [10, 20, 30]");
        assertQuery("select ARRAY [10, 20, 30] > ARRAY [10, 20, 30]");
        assertQuery("select ARRAY [TRUE, FALSE] <= ARRAY [TRUE, FALSE, true]");
        assertQuery("select ARRAY [TRUE, FALSE] > ARRAY [TRUE, FALSE, true]");
        assertQuery("select ARRAY [TRUE, FALSE] <= ARRAY [TRUE, FALSE]");
        assertQuery("select ARRAY [TRUE, FALSE] > ARRAY [TRUE, FALSE]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] <= ARRAY [2.2E0, 5.5E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] > ARRAY [2.2E0, 5.5E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] <= ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] > ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0]");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] <= ARRAY ['puppies', 'lizards']");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] > ARRAY ['puppies', 'lizards']");
        assertQuery("select ARRAY ['puppies', 'kittens'] <= ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY ['puppies', 'kittens'] > ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '04:05:06.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] <= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] > ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] <= ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] > ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] <= ARRAY [ARRAY [1, 2], ARRAY [3, 5, 6]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] > ARRAY [ARRAY [1, 2], ARRAY [3, 5, 6]]");

        assertQuery("select ARRAY [10, 20, 30] >= ARRAY [10, 20, 30]");
        assertQuery("select ARRAY [10, 20, 30] < ARRAY [10, 20, 30]");
        assertQuery("select ARRAY [TRUE, FALSE, TRUE] >= ARRAY [TRUE, FALSE, TRUE]");
        assertQuery("select ARRAY [TRUE, FALSE, TRUE] < ARRAY [TRUE, FALSE, TRUE]");
        assertQuery("select ARRAY [TRUE, FALSE, TRUE] >= ARRAY [TRUE]");
        assertQuery("select ARRAY [TRUE, FALSE, TRUE] < ARRAY [TRUE]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] >= ARRAY [1.1E0, 2.2E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] < ARRAY [1.1E0, 2.2E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] >= ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0]");
        assertQuery("select ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0] < ARRAY [1.1E0, 2.2E0, 3.3E0, 4.4E0]");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] >= ARRAY ['puppies', 'kittens', 'kittens']");
        assertQuery("select ARRAY ['puppies', 'kittens', 'lizards'] < ARRAY ['puppies', 'kittens', 'kittens']");
        assertQuery("select ARRAY ['puppies', 'kittens'] >= ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY ['puppies', 'kittens'] < ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] >= ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles'] < ARRAY [TIME '01:02:03.456 America/Los_Angeles', TIME '10:20:30.456 America/Los_Angeles']");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] >= ARRAY [ARRAY [1, 2], ARRAY [3, 4]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] < ARRAY [ARRAY [1, 2], ARRAY [3, 4]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] >= ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]");
        assertQuery("select ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]] < ARRAY [ARRAY [1, 2], ARRAY [3, 4, 5]]");
    }

    @Test
    public void testDistinctFrom()
    {
        assertQuery("select CAST(NULL AS ARRAY(UNKNOWN)) IS DISTINCT FROM CAST(NULL AS ARRAY(UNKNOWN))");
        assertQuery("select ARRAY [NULL] IS DISTINCT FROM ARRAY [NULL]");
        assertQuery("select NULL IS DISTINCT FROM ARRAY [1, 2]");
        assertQuery("select ARRAY [1, 2] IS DISTINCT FROM NULL");
        assertQuery("select ARRAY [1, 2] IS DISTINCT FROM ARRAY [1, 2]");
        assertQuery("select ARRAY [1, 2, 3] IS DISTINCT FROM ARRAY [1, 2]");
        assertQuery("select ARRAY [1, 2] IS DISTINCT FROM ARRAY [1, NULL]");
        assertQuery("select ARRAY [1, 2] IS DISTINCT FROM ARRAY [1, 3]");
        assertQuery("select ARRAY [1, NULL] IS DISTINCT FROM ARRAY [1, NULL]");
        assertQuery("select ARRAY [1, NULL] IS DISTINCT FROM ARRAY [1, NULL]");
        assertQuery("select ARRAY [1, 2, NULL] IS DISTINCT FROM ARRAY [1, 2]");
        assertQuery("select ARRAY [TRUE, FALSE] IS DISTINCT FROM ARRAY [TRUE, FALSE]");
        assertQuery("select ARRAY [TRUE, NULL] IS DISTINCT FROM ARRAY [TRUE, FALSE]");
        assertQuery("select ARRAY [FALSE, NULL] IS DISTINCT FROM ARRAY [NULL, FALSE]");
        assertQuery("select ARRAY ['puppies', 'kittens'] IS DISTINCT FROM ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY ['puppies', NULL] IS DISTINCT FROM ARRAY ['puppies', 'kittens']");
        assertQuery("select ARRAY ['puppies', NULL] IS DISTINCT FROM ARRAY [NULL, 'kittens']");
        assertQuery("select ARRAY [ARRAY ['puppies'], ARRAY ['kittens']] IS DISTINCT FROM ARRAY [ARRAY ['puppies'], ARRAY ['kittens']]");
        assertQuery("select ARRAY [ARRAY ['puppies'], NULL] IS DISTINCT FROM ARRAY [ARRAY ['puppies'], ARRAY ['kittens']]");
        assertQuery("select ARRAY [ARRAY ['puppies'], NULL] IS DISTINCT FROM ARRAY [NULL, ARRAY ['kittens']]");
    }

    @Test
    public void testArrayRemove()
    {
        assertQuery("select ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'foo')");
        assertQuery("select ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'bar')");
        assertQuery("select ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'baz')");
        assertQuery("select ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'zzz')");
        assertQuery("select ARRAY_REMOVE(ARRAY ['foo', 'foo', 'foo'], 'foo')");
        assertQuery("select ARRAY_REMOVE(ARRAY [NULL, 'bar', 'baz'], 'foo')");
        assertQuery("select ARRAY_REMOVE(ARRAY ['foo', 'bar', NULL], 'foo')");
        assertQuery("select ARRAY_REMOVE(ARRAY [1, 2, 3], 1)");
        assertQuery("select ARRAY_REMOVE(ARRAY [1, 2, 3], 2)");
        assertQuery("select ARRAY_REMOVE(ARRAY [1, 2, 3], 3)");
        assertQuery("select ARRAY_REMOVE(ARRAY [1, 2, 3], 4)");
        assertQuery("select ARRAY_REMOVE(ARRAY [1, 1, 1], 1)");
        assertQuery("select ARRAY_REMOVE(ARRAY [NULL, 2, 3], 1)");
        assertQuery("select ARRAY_REMOVE(ARRAY [1, NULL, 3], 1)");
        assertQuery("select ARRAY_REMOVE(ARRAY [-1.23E0, 3.14E0], 3.14E0)");
        assertQuery("select ARRAY_REMOVE(ARRAY [3.14E0], 0.0E0)");
        assertQuery("select ARRAY_REMOVE(ARRAY [sqrt(-1), 3.14E0], 3.14E0)");
        assertQuery("select ARRAY_REMOVE(ARRAY [-1.23E0, sqrt(-1)], nan())");
        assertQuery("select ARRAY_REMOVE(ARRAY [-1.23E0, nan()], nan())");
        assertQuery("select ARRAY_REMOVE(ARRAY [-1.23E0, infinity()], -1.23E0)");
        assertQuery("select ARRAY_REMOVE(ARRAY [infinity(), 3.14E0], infinity())");
        assertQuery("select ARRAY_REMOVE(ARRAY [-1.23E0, NULL, 3.14E0], 3.14E0)");
        assertQuery("select ARRAY_REMOVE(ARRAY [TRUE, FALSE, TRUE], TRUE)");
        assertQuery("select ARRAY_REMOVE(ARRAY [TRUE, FALSE, TRUE], FALSE)");
        assertQuery("select ARRAY_REMOVE(ARRAY [NULL, FALSE, TRUE], TRUE)");
        assertQuery("select ARRAY_REMOVE(ARRAY [ARRAY ['foo'], ARRAY ['bar'], ARRAY ['baz']], ARRAY ['bar'])");
        assertQuery("select ARRAY_REMOVE(ARRAY [1.0, 2.0, 3.0], 2.0)");
        assertQuery("select ARRAY_REMOVE(ARRAY [1.0, 2.0, 3.0], 4.0)");
        assertQuery("select ARRAY_REMOVE(ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543], 1234567890.1234567890)");
        assertQuery("select ARRAY_REMOVE(ARRAY [1234567890.1234567890, 9876543210.9876543210, 123123123456.6549876543], 4.0)");
        assertQuery("select ARRAY_REMOVE(ARRAY ['foo', 'bar', 'baz'], 'foo')");

        assertQueryFails("select ARRAY_REMOVE(ARRAY [ARRAY[CAST(null AS BIGINT)]], ARRAY[CAST(1 AS BIGINT)])", "array_remove does not support arrays with elements that are null or contain null");
        assertQueryFails("select ARRAY_REMOVE(ARRAY [ARRAY[CAST(null AS BIGINT)]], ARRAY[CAST(null AS BIGINT)])", "array_remove does not support arrays with elements that are null or contain null");
        assertQueryFails("select ARRAY_REMOVE(ARRAY [ARRAY[CAST(1 AS BIGINT)]], ARRAY[CAST(null AS BIGINT)])", "array_remove does not support arrays with elements that are null or contain null");
    }

    @Test
    public void testRemoveNulls()
    {
        assertQuery("select REMOVE_NULLS(ARRAY ['foo', 'bar'])");
        assertQuery("select REMOVE_NULLS(ARRAY ['foo', NULL, 'bar'])");
        assertQuery("select REMOVE_NULLS(ARRAY [1, NULL, NULL, 3])");
        assertQuery("select REMOVE_NULLS(ARRAY [ARRAY ['foo'], NULL, ARRAY['bar']])");
        assertQuery("select REMOVE_NULLS(ARRAY [TRUE, FALSE, TRUE])");
        assertQuery("select REMOVE_NULLS(ARRAY [TRUE, FALSE, NULL])");
        assertQuery("select REMOVE_NULLS(ARRAY [ARRAY[NULL]])");
        assertQuery("select REMOVE_NULLS(ARRAY [ARRAY[NULL], NULL])");
    }

    @Test
    public void testRepeat()
    {
        // concrete values
        assertQuery("select REPEAT(1, 5)");
        assertQuery("select REPEAT('varchar', 3)");
        assertQuery("select REPEAT(true, 1)");
        assertQuery("select REPEAT(0.5E0, 4)");
        assertQuery("select REPEAT(array[1], 4)");
        assertQuery("select REPEAT(cast(1 as integer), 10)");
        assertQuery("select REPEAT(cast(1 as integer), 0)");
        assertQuery("select REPEAT(cast(1 as bigint), 10)");
        assertQuery("select REPEAT(cast('ab' as varchar), 10)");
        assertQuery("select REPEAT(array[cast(2 as bigint)], 10)");
        assertQuery("select REPEAT(array[cast(2 as bigint), 3], 10)");
        assertQuery("select REPEAT(array[cast(2 as integer)], 10)");
        assertQuery("select REPEAT(map(array[cast(2 as integer)], array[cast('ab' as varchar)]), 10)");
        assertQuery("select REPEAT('loooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooongvarchar', 9999)");
        assertQuery("select REPEAT(array[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20], 9999)");

        // null values
        assertQuery("select REPEAT(null, 4)");
        assertQuery("select REPEAT(cast(null as bigint), 4)");
        assertQuery("select REPEAT(cast(null as double), 4)");
        assertQuery("select REPEAT(cast(null as varchar), 4)");
        assertQuery("select REPEAT(cast(null as boolean), 4)");
        assertQuery("select REPEAT(cast(null as array(boolean)), 4)");
        assertQuery("select REPEAT(cast(null as varchar), 10)");

        // 0 counts
        assertQuery("select REPEAT(cast(null as bigint), 0)");
        assertQuery("select REPEAT(1, 0)");
        assertQuery("select REPEAT('varchar', 0)");
        assertQuery("select REPEAT(true, 0)");
        assertQuery("select REPEAT(0.5E0, 0)");
        assertQuery("select REPEAT(array[1], 0)");

        // illegal inputs
        assertQueryFails("select REPEAT(2, -1)", "count argument of repeat function must be greater than or equal to 0");
        assertQueryFails("select REPEAT(1, 1000000)", "count argument of repeat function must be less than or equal to 10000");
    }

    @Test
    public void testIndeterminate()
    {
        assertQuery("select \"$operator$indeterminate\"(cast(null as array(bigint)))");
        assertQuery("select \"$operator$indeterminate\"(array[1,2,3])");
        assertQuery("select \"$operator$indeterminate\"(array[1,2,3,null])");
        assertQuery("select \"$operator$indeterminate\"(array['test1', 'test2', 'test3', 'test4'])");
        assertQuery("select \"$operator$indeterminate\"(array['test1', 'test2', 'test3', null])");
        assertQuery("select \"$operator$indeterminate\"(array['test1', null, 'test2', 'test3'])");
        assertQuery("select \"$operator$indeterminate\"(array[null, 'test1', 'test2', 'test3'])");
        assertQuery("select \"$operator$indeterminate\"(array[null, time '12:34:56', time '01:23:45'])");
        assertQuery("select \"$operator$indeterminate\"(array[null, timestamp '2016-01-02 12:34:56', timestamp '2016-12-23 01:23:45'])");
        assertQuery("select \"$operator$indeterminate\"(array[null])");
        assertQuery("select \"$operator$indeterminate\"(array[null, null, null])");
        assertQuery("select \"$operator$indeterminate\"(array[row(1), row(2), row(3)])");
        assertQuery("select \"$operator$indeterminate\"(array[cast(row(1) as row(a bigint)), cast(null as row(a bigint)), cast(row(3) as row(a bigint))])");
        assertQuery("select \"$operator$indeterminate\"(array[cast(row(1) as row(a bigint)), cast(row(null) as row(a bigint)), cast(row(3) as row(a bigint))])");
        assertQuery("select \"$operator$indeterminate\"(array[map(array[2], array[-2]), map(array[1], array[-1])])");
        assertQuery("select \"$operator$indeterminate\"(array[map(array[2], array[-2]), null])");
        assertQuery("select \"$operator$indeterminate\"(array[map(array[2], array[-2]), map(array[1], array[null])])");
        assertQuery("select \"$operator$indeterminate\"(array[array[1], array[2], array[3]])");
        assertQuery("select \"$operator$indeterminate\"(array[array[1], array[null], array[3]])");
        assertQuery("select \"$operator$indeterminate\"(array[array[1], array[2], null])");
        assertQuery("select \"$operator$indeterminate\"(array[1E0, 2E0, null])");
        assertQuery("select \"$operator$indeterminate\"(array[1E0, 2E0, 3E0])");
        assertQuery("select \"$operator$indeterminate\"(array[true, false, null])");
        assertQuery("select \"$operator$indeterminate\"(array[true, false, true])");
    }

    @Test
    public void testSequence()
            throws ParseException
    {
        // defaults to a step of 1
        assertQuery("select SEQUENCE(1, 5)");
        assertQuery("select SEQUENCE(-10, -5)");
        assertQuery("select SEQUENCE(-5, 2)");
        assertQuery("select SEQUENCE(2, 2)");
        assertQuery("select SEQUENCE(date '2016-04-12', date '2016-04-14')");

        // defaults to a step of -1
        assertQuery("select SEQUENCE(5, 1)");
        assertQuery("select SEQUENCE(-5, -10)");
        assertQuery("select SEQUENCE(2, -5)");
        assertQuery("select SEQUENCE(date '2016-04-14', date '2016-04-12')");

        // with increment
        assertQuery("select SEQUENCE(1, 9, 4)");
        assertQuery("select SEQUENCE(-10, -5, 2)");
        assertQuery("select SEQUENCE(-5, 2, 3)");
        assertQuery("select SEQUENCE(2, 2, 2)");
        assertQuery("select SEQUENCE(5, 1, -1)");
        assertQuery("select SEQUENCE(10, 2, -2)");

        // failure modes
        assertQueryFails(
                "select SEQUENCE(2, -1, 1)",
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertQueryFails(
                "select SEQUENCE(-1, -10, 1)",
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertQueryFails(
                "select SEQUENCE(1, 1000000)",
                "result of sequence function must not have more than 10000 entries");
        assertQueryFails(
                "select SEQUENCE(date '2000-04-14', date '2030-04-12')",
                "result of sequence function must not have more than 10000 entries");
    }

    @Test
    public void testSequenceDateTimeDayToSecond()
            throws ParseException
    {
        assertQuery("select SEQUENCE(date '2016-04-12', date '2016-04-14', interval '1' day)");
        assertQuery("select SEQUENCE(date '2016-04-14', date '2016-04-12', interval '-1' day)");

        assertQuery("select SEQUENCE(date '2016-04-12', date '2016-04-16', interval '2' day)");
        assertQuery("select SEQUENCE(date '2016-04-16', date '2016-04-12', interval '-2' day)");

        assertQuery("select SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-16 01:07:00', interval '3' minute)");
        assertQuery("select SEQUENCE(timestamp '2016-04-16 01:10:10', timestamp '2016-04-16 01:03:00', interval '-3' minute)");

        assertQuery("select SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-16 01:01:00', interval '20' second)");
        assertQuery("select SEQUENCE(timestamp '2016-04-16 01:01:10', timestamp '2016-04-16 01:00:20', interval '-20' second)");

        assertQuery("select SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-18 01:01:00', interval '19' hour)");
        assertQuery("select SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-14 01:00:20', interval '-19' hour)");

        // failure modes
        assertQueryFails("select SEQUENCE(date '2016-04-12', date '2016-04-14', interval '-1' day)",
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertQueryFails("select SEQUENCE(date '2016-04-14', date '2016-04-12', interval '1' day)",
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertQueryFails("select SEQUENCE(date '2000-04-14', date '2030-04-12', interval '1' day)",
                "result of sequence function must not have more than 10000 entries");
        assertQueryFails("select SEQUENCE(date '2018-01-01', date '2018-01-04', interval '18' hour)",
                "sequence step must be a day interval if start and end values are dates");
        assertQueryFails("select SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-16 01:01:00', interval '-20' second)",
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertQueryFails("select SEQUENCE(timestamp '2016-04-16 01:10:10', timestamp '2016-04-16 01:01:00', interval '20' second)",
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertQueryFails("select SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-04-16 09:01:00', interval '1' second)",
                "result of sequence function must not have more than 10000 entries");
    }

    @Test
    public void testSequenceDateTimeYearToMonth()
            throws ParseException
    {
        assertQuery("select SEQUENCE(date '2016-04-12', date '2016-06-12', interval '1' month)");
        assertQuery("select SEQUENCE(date '2016-06-12', date '2016-04-12', interval '-1' month)");

        assertQuery("select SEQUENCE(date '2016-04-12', date '2016-08-12', interval '2' month)");
        assertQuery("select SEQUENCE(date '2016-08-12', date '2016-04-12', interval '-2' month)");

        assertQuery("select SEQUENCE(date '2016-04-12', date '2018-04-12', interval '1' year)");
        assertQuery("select SEQUENCE(date '2018-04-12', date '2016-04-12', interval '-1' year)");

        assertQuery("select SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2016-09-16 01:10:00', interval '2' month)");
        assertQuery("select SEQUENCE(timestamp '2016-09-16 01:10:10', timestamp '2016-04-16 01:00:00', interval '-2' month)");

        assertQuery("select SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '2021-04-16 01:01:00', interval '2' year)");
        assertQuery("select SEQUENCE(timestamp '2016-04-16 01:01:10', timestamp '2011-04-16 01:00:00', interval '-2' year)");

        // failure modes
        assertQueryFails(
                "select SEQUENCE(date '2016-06-12', date '2016-04-12', interval '1' month)",
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertQueryFails(
                "select SEQUENCE(date '2016-04-12', date '2016-06-12', interval '-1' month)",
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertQueryFails(
                "select SEQUENCE(date '2000-04-12', date '3000-06-12', interval '1' month)",
                "result of sequence function must not have more than 10000 entries");
        assertQueryFails(
                "select SEQUENCE(timestamp '2016-05-16 01:00:10', timestamp '2016-04-16 01:01:00', interval '1' month)",
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertQueryFails(
                "select SEQUENCE(timestamp '2016-04-16 01:10:10', timestamp '2016-05-16 01:01:00', interval '-1' month)",
                "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start");
        assertQueryFails(
                "select SEQUENCE(timestamp '2016-04-16 01:00:10', timestamp '3000-04-16 09:01:00', interval '1' month)",
                "result of sequence function must not have more than 10000 entries");
    }

    @Test
    public void testFlatten()
    {
        // BOOLEAN Tests
        assertQuery("select flatten(ARRAY [ARRAY [TRUE, FALSE], ARRAY [FALSE]])");
        assertQuery("select flatten(ARRAY [ARRAY [TRUE, FALSE], NULL])");
        assertQuery("select flatten(ARRAY [ARRAY [TRUE, FALSE]])");
        assertQuery("select flatten(ARRAY [NULL, ARRAY [TRUE, FALSE]])");
        assertQuery("select flatten(ARRAY [ARRAY [TRUE], ARRAY [FALSE], ARRAY [TRUE, FALSE]])");
        assertQuery("select flatten(ARRAY [NULL, ARRAY [TRUE], NULL, ARRAY [FALSE], ARRAY [FALSE, TRUE]])");

        // VARCHAR Tests
        assertQuery("select flatten(ARRAY [ARRAY ['1', '2'], ARRAY ['3']])");
        assertQuery("select flatten(ARRAY [ARRAY ['1', '2'], NULL])");
        assertQuery("select flatten(ARRAY [NULL, ARRAY ['1', '2']])");
        assertQuery("select flatten(ARRAY [ARRAY ['0'], ARRAY ['1'], ARRAY ['2', '3']])");
        assertQuery("select flatten(ARRAY [NULL, ARRAY ['0'], NULL, ARRAY ['1'], ARRAY ['2', '3']])");

        // BIGINT Tests
        assertQuery("select flatten(ARRAY [ARRAY [1, 2], ARRAY [3]])");
        assertQuery("select flatten(ARRAY [ARRAY [1, 2], NULL])");
        assertQuery("select flatten(ARRAY [NULL, ARRAY [1, 2]])");
        assertQuery("select flatten(ARRAY [ARRAY [0], ARRAY [1], ARRAY [2, 3]])");
        assertQuery("select flatten(ARRAY [NULL, ARRAY [0], NULL, ARRAY [1], ARRAY [2, 3]])");

        // DOUBLE Tests
        assertQuery("select flatten(ARRAY [ARRAY [1.2E0, 2.2E0], ARRAY [3.2E0]])");
        assertQuery("select flatten(ARRAY [ARRAY [1.2E0, 2.2E0], NULL])");
        assertQuery("select flatten(ARRAY [NULL, ARRAY [1.2E0, 2.2E0]])");
        assertQuery("select flatten(ARRAY [ARRAY[0.2E0], ARRAY [1.2E0], ARRAY [2.2E0, 3.2E0]])");
        assertQuery("select flatten(ARRAY [NULL, ARRAY [0.2E0], NULL, ARRAY [1.2E0], ARRAY [2.2E0, 3.2E0]])");

        // ARRAY<BIGINT> tests
        assertQuery("select flatten(ARRAY [ARRAY [ARRAY [1, 2], ARRAY [3, 4]], ARRAY [ARRAY [5, 6], ARRAY [7, 8]]])");
        assertQuery("select flatten(ARRAY [ARRAY [ARRAY [1, 2], ARRAY [3, 4]], NULL])");
        assertQuery("select flatten(ARRAY [NULL, ARRAY [ARRAY [5, 6], ARRAY [7, 8]]])");

        // MAP<BIGINT, BIGINT> Tests
        assertQuery("select flatten(ARRAY [ARRAY [MAP (ARRAY [1, 2], ARRAY [1, 2])], ARRAY [MAP (ARRAY [3, 4], ARRAY [3, 4])]])");
        assertQuery("select flatten(ARRAY [ARRAY [MAP (ARRAY [1, 2], ARRAY [1, 2])], NULL])");
        assertQuery("select flatten(ARRAY [NULL, ARRAY [MAP (ARRAY [3, 4], ARRAY [3, 4])]])");
    }

    @Test
    public void testArrayHashOperator()
    {
        assertQuery("select \"$operator$hash_code\"(ARRAY[1, 2])");
        assertQuery("select \"$operator$hash_code\"(ARRAY[true, false])");

        // test with ARRAY[ MAP( ARRAY[1], ARRAY[2] ) ]
        assertQuery("select \"$operator$hash_code\"(ARRAY[MAP(ARRAY[1], ARRAY[2])])");
    }
}
