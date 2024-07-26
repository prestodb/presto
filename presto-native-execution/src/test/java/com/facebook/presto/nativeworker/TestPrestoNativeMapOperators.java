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
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.FIELD_NAMES_IN_JSON_CAST_ENABLED;
import static java.util.Arrays.asList;

public class TestPrestoNativeMapOperators
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
    public void testConstructor()
    {
        assertQuery("select MAP(ARRAY ['1','3'], ARRAY [2,4])");
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select MAP(ARRAY [1, 3], ARRAY[2, NULL])");
        assertQuery("select MAP(ARRAY [1, 3], ARRAY [2.0E0, 4.0E0])");
        assertQuery("select MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3])");
        assertQuery("select MAP(ARRAY[1.0E0, 2.0E0], ARRAY[ ARRAY[BIGINT '1', BIGINT '2'], ARRAY[ BIGINT '3' ]])");
        assertQuery("select MAP(ARRAY['puppies'], ARRAY['kittens'])");
        assertQuery("select MAP(ARRAY[TRUE, FALSE], ARRAY[2,4])");
        assertQuery("select MAP(ARRAY['1', '100'], ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'])");
        assertQuery("select MAP(ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'], ARRAY[1.0E0, 100.0E0])");

        assertQueryFails("select MAP(ARRAY [1], ARRAY [2, 4])", "Key and value arrays must be the same length");
        assertQueryFails("select MAP(ARRAY [1, 2, 3, 2], ARRAY [4, 5, 6, 7])", "Duplicate map keys \\(2\\) are not allowed");
        assertQueryFails("select MAP(ARRAY [ARRAY [1, 2], ARRAY [1, 3], ARRAY [1, 2]], ARRAY [1, 2, 3])",
                "Duplicate map keys \\(\\[1, 2\\]\\) are not allowed");

        assertQuery("select MAP(ARRAY ['1','3'], ARRAY [2,4])");

        assertQuery("select MAP(ARRAY [ARRAY[1]], ARRAY[2])");
        assertQueryFails("select MAP(ARRAY [NULL], ARRAY[2])", "map key cannot be null");
        assertQueryFails("select MAP(ARRAY [ARRAY[NULL]], ARRAY[2])", "map key cannot be indeterminate: \\[null\\]");
    }

    @Test
    public void testEmptyMapConstructor()
    {
        assertQuery("select MAP()");
    }

    @Test
    public void testCardinality()
    {
        assertQuery("select CARDINALITY(MAP(ARRAY ['1','3'], ARRAY [2,4]))");
        assertQuery("select CARDINALITY(MAP(ARRAY [1, 3], ARRAY[2, NULL]))");
        assertQuery("select CARDINALITY(MAP(ARRAY [1, 3], ARRAY [2.0E0, 4.0E0]))");
        assertQuery("select CARDINALITY(MAP(ARRAY[1.0E0, 2.0E0], ARRAY[ ARRAY[1, 2], ARRAY[3]]))");
        assertQuery("select CARDINALITY(MAP(ARRAY['puppies'], ARRAY['kittens']))");
        assertQuery("select CARDINALITY(MAP(ARRAY[TRUE], ARRAY[2]))");
        assertQuery("select CARDINALITY(MAP(ARRAY['1'], ARRAY[from_unixtime(1)]))");
        assertQuery("select CARDINALITY(MAP(ARRAY[from_unixtime(1)], ARRAY[1.0E0]))");
        assertQuery("select CARDINALITY(MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]))");
        assertQuery("select CARDINALITY(MAP(ARRAY [1.0], ARRAY [2.2]))");
    }

    @Test
    public void testMapToJson()
    {
        Session session = Session.builder(getSession()).setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "true").build();
        // Test key ordering
        assertQuery(session, "select CAST(MAP(ARRAY[7,5,3,1], ARRAY[8,6,4,2]) AS JSON)");
        assertQuery(session, "select CAST(MAP(ARRAY[1,3,5,7], ARRAY[2,4,6,8]) AS JSON)");

        // Test null value
        assertQuery(session, "select cast(cast (null as MAP<BIGINT, BIGINT>) AS JSON)");
        assertQuery(session, "select cast(MAP() AS JSON)");
        assertQuery(session, "select cast(MAP(ARRAY[1, 2], ARRAY[null, null]) AS JSON)");

        // Test key types
        assertQuery(session, "select CAST(MAP(ARRAY[true, false], ARRAY[1, 2]) AS JSON)");

        assertQuery(session, "select cast(MAP(cast(ARRAY[1, 2, 3] AS ARRAY<TINYINT>), ARRAY[5, 8, null]) AS JSON)");
        assertQuery(session, "select cast(MAP(cast(ARRAY[12345, 12346, 12347] AS ARRAY<SMALLINT>), ARRAY[5, 8, null]) AS JSON)");
        assertQuery(session, "select cast(MAP(cast(ARRAY[123456789,123456790,123456791] AS ARRAY<INTEGER>), ARRAY[5, 8, null]) AS JSON)");
        assertQuery(session, "select cast(MAP(cast(ARRAY[1234567890123456111,1234567890123456222,1234567890123456777] AS ARRAY<BIGINT>), ARRAY[111, 222, null]) AS JSON)");

        assertQuery(session, "select cast(MAP(cast(ARRAY[3.14E0, 1e10, 1e20] AS ARRAY<REAL>), ARRAY[null, 10, 20]) AS JSON)");

        assertQuery(session, "select cast(MAP(ARRAY[1e-323,1e308,nan()], ARRAY[-323,308,null]) AS JSON)");
        assertQuery(session, "select cast(MAP(ARRAY[DECIMAL '3.14', DECIMAL '0.01'], ARRAY[0.14, null]) AS JSON)");

        assertQuery(session, "select cast(MAP(ARRAY[DECIMAL '12345678901234567890.1234567890666666', DECIMAL '0.0'], ARRAY[666666, null]) AS JSON)");

        assertQuery(session, "select CAST(MAP(ARRAY['a', 'bb', 'ccc'], ARRAY[1, 2, 3]) AS JSON)");

        // Test value types
        assertQuery(session, "select cast(MAP(ARRAY[1, 2, 3], ARRAY[true, false, null]) AS JSON)");

        assertQuery(session, "select cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[5, 8, null] AS ARRAY<TINYINT>)) AS JSON)");
        assertQuery(session, "select cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[12345, -12345, null] AS ARRAY<SMALLINT>)) AS JSON)");
        assertQuery(session, "select cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[123456789, -123456789, null] AS ARRAY<INTEGER>)) AS JSON)");
        assertQuery(session, "select cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[1234567890123456789, -1234567890123456789, null] AS ARRAY<BIGINT>)) AS JSON)");

        assertQuery(session, "select CAST(MAP(ARRAY[1, 2, 3, 5, 8], CAST(ARRAY[3.14E0, nan(), infinity(), -infinity(), null] AS ARRAY<REAL>)) AS JSON)");
        assertQuery(session, "select CAST(MAP(ARRAY[1, 2, 3, 5, 8, 13, 21], ARRAY[3.14E0, 1e-323, 1e308, nan(), infinity(), -infinity(), null]) AS JSON)");
        assertQuery(session, "select CAST(MAP(ARRAY[1, 2], ARRAY[DECIMAL '3.14', null]) AS JSON)");
        assertQuery(session, "select CAST(MAP(ARRAY[1, 2], ARRAY[DECIMAL '12345678901234567890.123456789012345678', null]) AS JSON)");

        assertQuery(session, "select cast(MAP(ARRAY[1, 2, 3], ARRAY['a', 'bb', null]) AS JSON)");
        assertQuery(session, "select CAST(MAP(ARRAY[1, 2, 3, 5, 8, 13, 21, 34], ARRAY[JSON '123', JSON '3.14', JSON 'false', JSON '\"abc\"', JSON '[1, \"a\", null]', JSON '{\"a\": 1, \"b\": \"str\", \"c\": null}', JSON 'null', null]) AS JSON)");

        assertQuery(session, "select CAST(MAP(ARRAY[1, 2], ARRAY[TIMESTAMP '1970-01-01 00:00:01', null]) AS JSON)");
        assertQuery(session, "select CAST(MAP(ARRAY[2, 5, 3], ARRAY[DATE '2001-08-22', DATE '2001-08-23', null]) AS JSON)");

        assertQuery(session, "select cast(MAP(ARRAY[1, 2, 3, 5, 8], ARRAY[ARRAY[1, 2], ARRAY[3, null], ARRAY[], ARRAY[null, null], null]) AS JSON)");
        assertQuery(session, "select cast(MAP(ARRAY[1, 2, 8, 5, 3], ARRAY[MAP(ARRAY['b', 'a'], ARRAY[2, 1]), MAP(ARRAY['three', 'none'], ARRAY[3, null]), MAP(), MAP(ARRAY['h2', 'h1'], ARRAY[null, null]), null]) AS JSON)");
        assertQuery(session, "select cast(MAP(ARRAY[1, 2, 3, 5], ARRAY[ROW(1, 2), ROW(3, CAST(null as INTEGER)), CAST(ROW(null, null) AS ROW(INTEGER, INTEGER)), null]) AS JSON)");

        assertQuery(session, "select CAST(MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]) AS JSON)");
        assertQuery(session, "select CAST(MAP(ARRAY [1.0], ARRAY [2.2]) AS JSON)");
    }

    @Test
    public void testMapToJsonNoFieldNames()
    {
        // Test key ordering
        assertQuery("select CAST(MAP(ARRAY[7,5,3,1], ARRAY[8,6,4,2]) AS JSON)");
        assertQuery("select CAST(MAP(ARRAY[1,3,5,7], ARRAY[2,4,6,8]) AS JSON)");

        // Test null value
        assertQuery("select cast(cast (null as MAP<BIGINT, BIGINT>) AS JSON)");
        assertQuery("select cast(MAP() AS JSON)");
        assertQuery("select cast(MAP(ARRAY[1, 2], ARRAY[null, null]) AS JSON)");

        // Test key types
        assertQuery("select CAST(MAP(ARRAY[true, false], ARRAY[1, 2]) AS JSON)");

        assertQuery("select cast(MAP(cast(ARRAY[1, 2, 3] AS ARRAY<TINYINT>), ARRAY[5, 8, null]) AS JSON)");
        assertQuery("select cast(MAP(cast(ARRAY[12345, 12346, 12347] AS ARRAY<SMALLINT>), ARRAY[5, 8, null]) AS JSON)");
        assertQuery("select cast(MAP(cast(ARRAY[123456789,123456790,123456791] AS ARRAY<INTEGER>), ARRAY[5, 8, null]) AS JSON)");
        assertQuery("select cast(MAP(cast(ARRAY[1234567890123456111,1234567890123456222,1234567890123456777] AS ARRAY<BIGINT>), ARRAY[111, 222, null]) AS JSON)");

        assertQuery("select cast(MAP(cast(ARRAY[3.14E0, 1e10, 1e20] AS ARRAY<REAL>), ARRAY[null, 10, 20]) AS JSON)");

        assertQuery("select cast(MAP(ARRAY[1e-323,1e308,nan()], ARRAY[-323,308,null]) AS JSON)");
        assertQuery("select cast(MAP(ARRAY[DECIMAL '3.14', DECIMAL '0.01'], ARRAY[0.14, null]) AS JSON)");

        assertQuery("select cast(MAP(ARRAY[DECIMAL '12345678901234567890.1234567890666666', DECIMAL '0.0'], ARRAY[666666, null]) AS JSON)");

        assertQuery("select CAST(MAP(ARRAY['a', 'bb', 'ccc'], ARRAY[1, 2, 3]) AS JSON)");

        // Test value types
        assertQuery("select cast(MAP(ARRAY[1, 2, 3], ARRAY[true, false, null]) AS JSON)");

        assertQuery("select cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[5, 8, null] AS ARRAY<TINYINT>)) AS JSON)");
        assertQuery("select cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[12345, -12345, null] AS ARRAY<SMALLINT>)) AS JSON)");
        assertQuery("select cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[123456789, -123456789, null] AS ARRAY<INTEGER>)) AS JSON)");
        assertQuery("select cast(MAP(ARRAY[1, 2, 3], cast(ARRAY[1234567890123456789, -1234567890123456789, null] AS ARRAY<BIGINT>)) AS JSON)");

        assertQuery("select CAST(MAP(ARRAY[1, 2, 3, 5, 8], CAST(ARRAY[3.14E0, nan(), infinity(), -infinity(), null] AS ARRAY<REAL>)) AS JSON)");
        assertQuery("select CAST(MAP(ARRAY[1, 2, 3, 5, 8, 13, 21], ARRAY[3.14E0, 1e-323, 1e308, nan(), infinity(), -infinity(), null]) AS JSON)");
        assertQuery("select CAST(MAP(ARRAY[1, 2], ARRAY[DECIMAL '3.14', null]) AS JSON)");
        assertQuery("select CAST(MAP(ARRAY[1, 2], ARRAY[DECIMAL '12345678901234567890.123456789012345678', null]) AS JSON)");

        assertQuery("select cast(MAP(ARRAY[1, 2, 3], ARRAY['a', 'bb', null]) AS JSON)");
        assertQuery("select CAST(MAP(ARRAY[1, 2, 3, 5, 8, 13, 21, 34], ARRAY[JSON '123', JSON '3.14', JSON 'false', JSON '\"abc\"', JSON '[1, \"a\", null]', JSON '{\"a\": 1, \"b\": \"str\", \"c\": null}', JSON 'null', null]) AS JSON)");

        assertQuery("select CAST(MAP(ARRAY[1, 2], ARRAY[TIMESTAMP '1970-01-01 00:00:01', null]) AS JSON)");
        assertQuery("select CAST(MAP(ARRAY[2, 5, 3], ARRAY[DATE '2001-08-22', DATE '2001-08-23', null]) AS JSON)");

        assertQuery("select cast(MAP(ARRAY[1, 2, 3, 5, 8], ARRAY[ARRAY[1, 2], ARRAY[3, null], ARRAY[], ARRAY[null, null], null]) AS JSON)");
        assertQuery("select cast(MAP(ARRAY[1, 2, 8, 5, 3], ARRAY[MAP(ARRAY['b', 'a'], ARRAY[2, 1]), MAP(ARRAY['three', 'none'], ARRAY[3, null]), MAP(), MAP(ARRAY['h2', 'h1'], ARRAY[null, null]), null]) AS JSON)");
        assertQuery("select cast(MAP(ARRAY[1, 2, 3, 5], ARRAY[ROW(1, 2), ROW(3, CAST(null as INTEGER)), CAST(ROW(null, null) AS ROW(INTEGER, INTEGER)), null]) AS JSON)");
        assertQuery("select CAST(MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]) AS JSON)");
        assertQuery("select CAST(MAP(ARRAY [1.0], ARRAY [2.2]) AS JSON)");
    }

    @Test
    public void testJsonToMap()
    {
        // special values
        assertQuery("select CAST(CAST (null AS JSON) AS MAP<BIGINT, BIGINT>)");
        assertQuery("select CAST(JSON 'null' AS MAP<BIGINT, BIGINT>)");
        assertQuery("select CAST(JSON '{}' AS MAP<BIGINT, BIGINT>)");
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{\"1\": null, \"2\": null}' AS MAP<BIGINT, BIGINT>)");
        assertQueryFails("select CAST(CAST(MAP(ARRAY[12345.12345], ARRAY[12345.12345]) AS JSON) AS MAP<DECIMAL(10,5), DECIMAL(2,1)>)",
                "Cannot cast to map\\(decimal\\(10,5\\),decimal\\(2,1\\)\\). Cannot cast input json to DECIMAL\\(2,1\\)\n" +
                        "\\{\"12345.12345\":12345.12345\\}");

        // key type: boolean
        assertQuery("select CAST(JSON '{\"true\": 1, \"false\": 0}' AS MAP<BOOLEAN, BIGINT>)");

        // key type: tinyint, smallint, integer, bigint
        assertQuery("select CAST(JSON '{\"1\": 5, \"2\": 8, \"3\": 13}' AS MAP<TINYINT, BIGINT>)");
        assertQuery("select CAST(JSON '{\"12345\": 5, \"12346\": 8, \"12347\": 13}' AS MAP<SMALLINT, BIGINT>)");
        assertQuery("select CAST(JSON '{\"123456789\": 5, \"123456790\": 8, \"123456791\": 13}' AS MAP<INTEGER, BIGINT>)");
        assertQuery("select CAST(JSON '{\"1234567890123456111\": 5, \"1234567890123456222\": 8, \"1234567890123456777\": 13}' AS MAP<BIGINT, BIGINT>)");

        // key type: real, double, decimal
        assertQuery("select CAST(JSON '{\"3.14\": 5, \"NaN\": 8, \"Infinity\": 13, \"-Infinity\": 21}' AS MAP<REAL, BIGINT>)");
        assertQuery("select CAST(JSON '{\"3.1415926\": 5, \"NaN\": 8, \"Infinity\": 13, \"-Infinity\": 21}' AS MAP<DOUBLE, BIGINT>)");
        assertQuery("select CAST(JSON '{\"123.456\": 5, \"3.14\": 8}' AS MAP<DECIMAL(10, 5), BIGINT>)");
        assertQuery("select CAST(JSON '{\"12345678.12345678\": 5, \"3.1415926\": 8}' AS MAP<DECIMAL(38, 8), BIGINT>)");

        // key type: varchar
        assertQuery("select CAST(JSON '{\"a\": 5, \"bb\": 8, \"ccc\": 13}' AS MAP<VARCHAR, BIGINT>)");

        // value type: boolean
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{\"1\": true, \"2\": false, \"3\": 12, \"5\": 0, \"8\": 12.3, \"13\": 0.0, \"21\": \"true\", \"34\": \"false\", \"55\": null}' AS MAP<BIGINT, BOOLEAN>)");

        // value type: tinyint, smallint, integer, bigint
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{\"1\": true, \"2\": false, \"3\": 12, \"5\": 12.7, \"8\": \"12\", \"13\": null}' AS MAP<BIGINT, TINYINT>)");
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{\"1\": true, \"2\": false, \"3\": 12345, \"5\": 12345.6, \"8\": \"12345\", \"13\": null}' AS MAP<BIGINT, SMALLINT>)");
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{\"1\": true, \"2\": false, \"3\": 12345678, \"5\": 12345678.9, \"8\": \"12345678\", \"13\": null}' AS MAP<BIGINT, INTEGER>)");
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{\"1\": true, \"2\": false, \"3\": 1234567891234567, \"5\": 1234567891234567.8, \"8\": \"1234567891234567\", \"13\": null}' AS MAP<BIGINT, BIGINT>)");

        // value type: real, double, decimal
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{\"1\": true, \"2\": false, \"3\": 12345, \"5\": 12345.67, \"8\": \"3.14\", \"13\": \"NaN\", \"21\": \"Infinity\", \"34\": \"-Infinity\", \"55\": null}' AS MAP<BIGINT, REAL>)");
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{\"1\": true, \"2\": false, \"3\": 1234567890, \"5\": 1234567890.1, \"8\": \"3.14\", \"13\": \"NaN\", \"21\": \"Infinity\", \"34\": \"-Infinity\", \"55\": null}' AS MAP<BIGINT, DOUBLE>)");
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{\"1\": true, \"2\": false, \"3\": 128, \"5\": 123.456, \"8\": \"3.14\", \"13\": null}' AS MAP<BIGINT, DECIMAL(10, 5)>)");
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{\"1\": true, \"2\": false, \"3\": 128, \"5\": 12345678.12345678, \"8\": \"3.14\", \"13\": null}' AS MAP<BIGINT, DECIMAL(38, 8)>)");

        // varchar, json
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{\"1\": true, \"2\": false, \"3\": 12, \"5\": 12.3, \"8\": \"puppies\", \"13\": \"kittens\", \"21\": \"null\", \"34\": \"\", \"55\": null}' AS MAP<BIGINT, VARCHAR>)");

        assertQuery("select CAST(JSON '{\"k1\": 5, \"k2\": 3.14, \"k3\":[1, 2, 3], \"k4\":\"e\", \"k5\":{\"a\": \"b\"}, \"k6\":null, \"k7\":\"null\", \"k8\":[null]}' AS MAP<VARCHAR, JSON>)");

        // These two tests verifies that partial json cast preserves input order
        // The second test should never happen in real life because valid json in presto requires natural key ordering.
        // However, it is added to make sure that the order in the first test is not a coincidence.
        assertQuery("select CAST(JSON '{\"k1\": {\"1klmnopq\":1, \"2klmnopq\":2, \"3klmnopq\":3, \"4klmnopq\":4, \"5klmnopq\":5, \"6klmnopq\":6, \"7klmnopq\":7}}' AS MAP<VARCHAR, JSON>)");
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(unchecked_to_json('{\"k1\": {\"7klmnopq\":7, \"6klmnopq\":6, \"5klmnopq\":5, \"4klmnopq\":4, \"3klmnopq\":3, \"2klmnopq\":2, \"1klmnopq\":1}}') AS MAP<VARCHAR, JSON>)");

        // nested array/map
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{\"1\": [1, 2], \"2\": [3, null], \"3\": [], \"5\": [null, null], \"8\": null}' AS MAP<BIGINT, ARRAY<BIGINT>>)");

        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{" +
        //                 "\"1\": {\"a\": 1, \"b\": 2}, " +
        //                 "\"2\": {\"none\": null, \"three\": 3}, " +
        //                 "\"3\": {}, " +
        //                 "\"5\": {\"h1\": null,\"h2\": null}, " +
        //                 "\"8\": null}' " +
        //                 "AS MAP<BIGINT, MAP<VARCHAR, BIGINT>>)");

        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(JSON '{" +
        //                 "\"row1\": [1, \"two\"], " +
        //                 "\"row2\": [3, null], " +
        //                 "\"row3\": {\"k1\": 1, \"k2\": \"two\"}, " +
        //                 "\"row4\": {\"k2\": null, \"k1\": 3}, " +
        //                 "\"row5\": null}' " +
        //                 "AS MAP<VARCHAR, ROW(k1 BIGINT, k2 VARCHAR)>)");

        // invalid cast
        assertQueryFails("select CAST(JSON '{\"[]\": 1}' AS MAP<ARRAY<BIGINT>, BIGINT>)", "Cannot cast JSON to map\\(array\\(bigint\\),bigint\\)");

        assertQueryFails("select CAST(JSON '[1, 2]' AS MAP<BIGINT, BIGINT>)", "Cannot cast to map\\(bigint,bigint\\). Expected a json object, but got \\[\n\\[1,2\\]");
        assertQueryFails("select CAST(JSON '{\"a\": 1, \"b\": 2}' AS MAP<VARCHAR, MAP<VARCHAR, BIGINT>>)", "Cannot cast to map\\(varchar,map\\(varchar,bigint\\)\\). Expected a json object, but got 1\n\\{\"a\":1,\"b\":2\\}");
        assertQueryFails("select CAST(JSON '{\"a\": 1, \"b\": []}' AS MAP<VARCHAR, BIGINT>)", "Cannot cast to map\\(varchar,bigint\\). Unexpected token when cast to bigint: \\[\n\\{\"a\":1,\"b\":\\[\\]\\}");
        assertQueryFails("select CAST(JSON '{\"1\": {\"a\": 1}, \"2\": []}' AS MAP<VARCHAR, MAP<VARCHAR, BIGINT>>)", "Cannot cast to map\\(varchar,map\\(varchar,bigint\\)\\). Expected a json object, but got \\[\n\\{\"1\":\\{\"a\":1\\},\"2\":\\[\\]\\}");

        // assertQueryFails("select CAST(unchecked_to_json('\"a\": 1, \"b\": 2') AS MAP<VARCHAR, BIGINT>)", "Cannot cast to map\\(varchar,bigint\\). Expected a json object, but got a\n\"a\": 1, \"b\": 2");
        // assertQueryFails("select CAST(unchecked_to_json('{\"a\": 1} 2') AS MAP<VARCHAR, BIGINT>)", "Cannot cast to map\\(varchar,bigint\\). Unexpected trailing token: 2\n\\{\"a\": 1\\} 2");
        // assertQueryFails("select CAST(unchecked_to_json('{\"a\": 1') AS MAP<VARCHAR, BIGINT>)", "Cannot cast to map\\(varchar,bigint\\).\n\\{\"a\": 1");

        assertQueryFails("select CAST(JSON '{\"a\": \"b\"}' AS MAP<VARCHAR, BIGINT>)", "Cannot cast to map\\(varchar,bigint\\). Cannot cast 'b' to BIGINT\n\\{\"a\":\"b\"\\}");
        assertQueryFails("select CAST(JSON '{\"a\": 1234567890123.456}' AS MAP<VARCHAR, INTEGER>)", "Cannot cast to map\\(varchar,integer\\). Unable to cast 1.234567890123456E12 to integer\n\\{\"a\":1.234567890123456E12\\}");

        assertQueryFails("select CAST(JSON '{\"1\":1, \"01\": 2}' AS MAP<BIGINT, BIGINT>)", "Cannot cast to map\\(bigint,bigint\\). Duplicate keys are not allowed\n\\{\"01\":2,\"1\":1\\}");
        assertQueryFails("select CAST(JSON '[{\"1\":1, \"01\": 2}]' AS ARRAY<MAP<BIGINT, BIGINT>>)", "Cannot cast to array\\(map\\(bigint,bigint\\)\\). Duplicate keys are not allowed\n\\[\\{\"01\":2,\"1\":1\\}\\]");

        // some other key/value type combinations
        assertQuery("select CAST(JSON '{\"puppies\":\"kittens\"}' AS MAP<VARCHAR, VARCHAR>)");
        assertQuery("select CAST(JSON '{\"true\":\"kittens\"}' AS MAP<BOOLEAN, VARCHAR>)");
        assertQuery("select CAST(JSON 'null' AS MAP<BOOLEAN, VARCHAR>)");
        // cannot use JSON literal containing DECIMAL values right now.
        // Decimal literal are interpreted internally by JSON parser as double and precision is lost.

        assertQuery("select CAST(CAST(MAP(ARRAY[1.0, 383838383838383.12324234234234], ARRAY[2.2, 3.3]) AS JSON) AS MAP<DECIMAL(29,14), DECIMAL(2,1)>)");
        assertQuery("select CAST(CAST(MAP(ARRAY[2.2, 3.3], ARRAY[1.0, 383838383838383.12324234234234]) AS JSON) AS MAP<DECIMAL(2,1), DECIMAL(29,14)>)");
        assertQueryFails("select CAST(CAST(MAP(ARRAY[12345.12345], ARRAY[12345.12345]) AS JSON) AS MAP<DECIMAL(2,1), DECIMAL(10,5)>)",
                "Cannot cast to map\\(decimal\\(2,1\\),decimal\\(10,5\\)\\). Cannot cast input json to DECIMAL\\(2,1\\)\n" +
                        "\\{\"12345.12345\":12345.12345\\}");
        assertQueryFails("select CAST(CAST(MAP(ARRAY[12345.12345], ARRAY[12345.12345]) AS JSON) AS MAP<DECIMAL(10,5), DECIMAL(2,1)>)",
                "Cannot cast to map\\(decimal\\(10,5\\),decimal\\(2,1\\)\\). Cannot cast input json to DECIMAL\\(2,1\\)\n" +
                        "\\{\"12345.12345\":12345.12345\\}");
    }

    @Test
    public void testElementAt()
    {
        // empty map
        assertQuery("select element_at(MAP(CAST(ARRAY [] AS ARRAY(BIGINT)), CAST(ARRAY [] AS ARRAY(BIGINT))), 1)");

        // missing key
        assertQuery("select element_at(MAP(ARRAY [1], ARRAY [1e0]), 2)");
        assertQuery("select element_at(MAP(ARRAY [1.0], ARRAY ['a']), 2.0)");
        assertQuery("select element_at(MAP(ARRAY ['a'], ARRAY [true]), 'b')");
        assertQuery("select element_at(MAP(ARRAY [true], ARRAY [ARRAY [1]]), false)");
        assertQuery("select element_at(MAP(ARRAY [ARRAY [1]], ARRAY [1]), ARRAY [2])");

        // null value associated with the requested key
        assertQuery("select element_at(MAP(ARRAY [1], ARRAY [null]), 1)");
        assertQuery("select element_at(MAP(ARRAY [1.0E0], ARRAY [null]), 1.0E0)");
        assertQuery("select element_at(MAP(ARRAY [TRUE], ARRAY [null]), TRUE)");
        assertQuery("select element_at(MAP(ARRAY ['puppies'], ARRAY [null]), 'puppies')");
        assertQuery("select element_at(MAP(ARRAY [ARRAY [1]], ARRAY [null]), ARRAY [1])");

        // general tests
        assertQuery("select element_at(MAP(ARRAY [1, 3], ARRAY [2, 4]), 3)");
        assertQuery("select element_at(MAP(ARRAY [BIGINT '1', 3], ARRAY [BIGINT '2', 4]), 3)");
        assertQuery("select element_at(MAP(ARRAY [1, 3], ARRAY [2, NULL]), 3)");
        assertQuery("select element_at(MAP(ARRAY [BIGINT '1', 3], ARRAY [2, NULL]), 3)");
        assertQuery("select element_at(MAP(ARRAY [1, 3], ARRAY [2.0E0, 4.0E0]), 1)");
        assertQuery("select element_at(MAP(ARRAY [1.0E0, 2.0E0], ARRAY [ARRAY [1, 2], ARRAY [3]]), 1.0E0)");
        assertQuery("select element_at(MAP(ARRAY ['puppies'], ARRAY ['kittens']), 'puppies')");
        assertQuery("select element_at(MAP(ARRAY [TRUE, FALSE], ARRAY [2, 4]), TRUE)");
        assertQuery("select element_at(MAP(ARRAY [ARRAY [1, 2], ARRAY [3]], ARRAY [1e0, 2e0]), ARRAY [1, 2])");
        assertQuery("select element_at(MAP(ARRAY ['1', '100'], ARRAY [TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '2005-09-10 13:00:00']), '1')");
        assertQuery("select element_at(MAP(ARRAY [from_unixtime(1), from_unixtime(100)], ARRAY [1.0E0, 100.0E0]), from_unixtime(1))");
    }

    @Test
    public void testSubscript()
    {
        assertQuery("select MAP(ARRAY [1], ARRAY [null])[1]");
        assertQuery("select MAP(ARRAY [1.0E0], ARRAY [null])[1.0E0]");
        assertQuery("select MAP(ARRAY [TRUE], ARRAY [null])[TRUE]");
        assertQuery("select MAP(ARRAY['puppies'], ARRAY [null])['puppies']");
        assertQueryFails("select MAP(ARRAY [CAST(null as bigint)], ARRAY [1])", "map key cannot be null");
        assertQueryFails("select MAP(ARRAY [CAST(null as bigint)], ARRAY [CAST(null as bigint)])", "map key cannot be null");
        assertQueryFails("select MAP(ARRAY [1,null], ARRAY [null,2])", "map key cannot be null");
        assertQuery("select MAP(ARRAY [1, 3], ARRAY [2, 4])[3]");
        assertQuery("select MAP(ARRAY [BIGINT '1', 3], ARRAY [BIGINT '2', 4])[3]");
        assertQuery("select MAP(ARRAY [1, 3], ARRAY[2, NULL])[3]");
        assertQuery("select MAP(ARRAY [BIGINT '1', 3], ARRAY[2, NULL])[3]");
        assertQuery("select MAP(ARRAY [1, 3], ARRAY [2.0E0, 4.0E0])[1]");
        assertQuery("select MAP(ARRAY[1.0E0, 2.0E0], ARRAY[ ARRAY[1, 2], ARRAY[3]])[1.0E0]");
        assertQuery("select MAP(ARRAY['puppies'], ARRAY['kittens'])['puppies']");
        assertQuery("select MAP(ARRAY[TRUE,FALSE],ARRAY[2,4])[TRUE]");
        assertQuery("select MAP(ARRAY['1', '100'], ARRAY[TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP '1973-07-08 22:00:01'])['1']");
        assertQuery("select MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[1.0E0, 100.0E0])[from_unixtime(1)]");
        assertQueryFails("select MAP(ARRAY [BIGINT '1'], ARRAY [BIGINT '2'])[3]", "Key not present in map: 3");
        assertQueryFails("select MAP(ARRAY ['hi'], ARRAY [2])['missing']", "Key not present in map: missing");
        assertQuery("select MAP(ARRAY[array[1,1]], ARRAY['a'])[ARRAY[1,1]]");
        assertQuery("select MAP(ARRAY[('a', 'b')], ARRAY[ARRAY[100, 200]])[('a', 'b')]");
        assertQuery("select MAP(ARRAY[1.0], ARRAY [2.2])[1.0]");
        assertQuery("select MAP(ARRAY[000000000000001.00000000000000], ARRAY [2.2])[000000000000001.00000000000000]");
        assertQueryFails("select MAP(ARRAY[cast('1' as varbinary)], ARRAY[null])[cast('2' as varbinary)]", "Key not present in map");
    }

    @Test
    public void testMapKeys()
    {
        assertQuery("select MAP_KEYS(MAP(ARRAY['1', '3'], ARRAY['2', '4']))");
        assertQuery("select MAP_KEYS(MAP(ARRAY[1.0E0, 2.0E0], ARRAY[ARRAY[1, 2], ARRAY[3]]))");
        assertQuery("select MAP_KEYS(MAP(ARRAY['puppies'], ARRAY['kittens']))");
        assertQuery("select MAP_KEYS(MAP(ARRAY[TRUE], ARRAY[2]))");
        assertQuery("select MAP_KEYS(MAP(ARRAY[TIMESTAMP '1970-01-01 00:00:01'], ARRAY[1.0E0]))");
        assertQuery("select MAP_KEYS(MAP(ARRAY[CAST('puppies' as varbinary)], ARRAY['kittens']))");
        assertQuery("select MAP_KEYS(MAP(ARRAY[1,2],  ARRAY[ARRAY[1, 2], ARRAY[3]]))");
        assertQuery("select MAP_KEYS(MAP(ARRAY[1,4], ARRAY[MAP(ARRAY[2], ARRAY[3]), MAP(ARRAY[5], ARRAY[6])]))");
        assertQuery("select MAP_KEYS(MAP(ARRAY [ARRAY [1], ARRAY [2, 3]],  ARRAY [ARRAY [3, 4], ARRAY [5]]))");
        assertQuery("select MAP_KEYS(MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]))");
        assertQuery("select MAP_KEYS(MAP(ARRAY [1.0, 2.01], ARRAY [2.2, 3.3]))");
    }

    @Test
    public void testMapValues()
    {
        assertQuery("select MAP_VALUES(MAP(ARRAY['1'], ARRAY[ARRAY[TRUE, FALSE, NULL]]))");
        assertQuery("select MAP_VALUES(MAP(ARRAY['1'], ARRAY[ARRAY[ARRAY[1, 2]]]))");
        assertQuery("select MAP_VALUES(MAP(ARRAY [1, 3], ARRAY ['2', '4']))");
        assertQuery("select MAP_VALUES(MAP(ARRAY[1.0E0,2.0E0], ARRAY[ARRAY[1, 2], ARRAY[3]]))");
        assertQuery("select MAP_VALUES(MAP(ARRAY['puppies'], ARRAY['kittens']))");
        assertQuery("select MAP_VALUES(MAP(ARRAY[TRUE], ARRAY[2]))");
        assertQuery("select MAP_VALUES(MAP(ARRAY['1'], ARRAY[NULL]))");
        assertQuery("select MAP_VALUES(MAP(ARRAY['1'], ARRAY[TRUE]))");
        assertQuery("select MAP_VALUES(MAP(ARRAY['1'], ARRAY[1.0E0]))");
        assertQuery("select MAP_VALUES(MAP(ARRAY['1', '2'], ARRAY[ARRAY[1.0E0, 2.0E0], ARRAY[3.0E0, 4.0E0]]))");
        assertQuery("select MAP_VALUES(MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]))");
        assertQuery("select MAP_VALUES(MAP(ARRAY [1.0, 2.01], ARRAY [383838383838383.12324234234234, 3.3]))");
    }

    @Test
    public void testEquals()
    {
        // single item
        assertQuery("select MAP(ARRAY[1], ARRAY[2]) = MAP(ARRAY[1], ARRAY[2])");
        assertQuery("select MAP(ARRAY[1], ARRAY[2]) = MAP(ARRAY[1], ARRAY[4])");
        assertQuery("select MAP(ARRAY[3], ARRAY[1]) = MAP(ARRAY[2], ARRAY[1])");
        assertQuery("select MAP(ARRAY[2.2], ARRAY[3.1]) = MAP(ARRAY[2.2], ARRAY[3.1])");
        assertQuery("select MAP(ARRAY[2.2], ARRAY[3.1]) = MAP(ARRAY[2.2], ARRAY[3.0])");
        assertQuery("select MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000]) " +
                "= MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000])");
        assertQuery("select MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000]) " +
                "= MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000013.30000000000000])");

        // multiple items
        assertQuery("select MAP(ARRAY[1], ARRAY[2]) = MAP(ARRAY[1, 3], ARRAY[2, 4])");
        assertQuery("select MAP(ARRAY[1, 3], ARRAY[2, 4]) = MAP(ARRAY[1], ARRAY[2])");
        assertQuery("select MAP(ARRAY[1, 3], ARRAY[2, 4]) = MAP(ARRAY[3, 1], ARRAY[4, 2])");
        assertQuery("select MAP(ARRAY[1, 3], ARRAY[2, 4]) = MAP(ARRAY[3, 1], ARRAY[2, 4])");
        assertQuery("select MAP(ARRAY['1', '3'], ARRAY[2.0E0, 4.0E0]) = MAP(ARRAY['3', '1'], ARRAY[4.0E0, 2.0E0])");
        assertQuery("select MAP(ARRAY['1', '3'], ARRAY[2.0E0, 4.0E0]) = MAP(ARRAY['3', '1'], ARRAY[2.0E0, 4.0E0])");
        assertQuery("select MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) = MAP(ARRAY[FALSE, TRUE], ARRAY['4', '2'])");
        assertQuery("select MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) = MAP(ARRAY[FALSE, TRUE], ARRAY['2', '4'])");
        assertQuery("select MAP(ARRAY[1.0E0, 3.0E0], ARRAY[TRUE, FALSE]) = MAP(ARRAY[3.0E0, 1.0E0], ARRAY[FALSE, TRUE])");
        assertQuery("select MAP(ARRAY[1.0E0, 3.0E0], ARRAY[TRUE, FALSE]) = MAP(ARRAY[3.0E0, 1.0E0], ARRAY[TRUE, FALSE])");
        assertQuery("select MAP(ARRAY[1.0E0, 3.0E0], ARRAY[from_unixtime(1), from_unixtime(100)]) = MAP(ARRAY[3.0E0, 1.0E0], ARRAY[from_unixtime(100), from_unixtime(1)])");
        assertQuery("select MAP(ARRAY[1.0E0, 3.0E0], ARRAY[from_unixtime(1), from_unixtime(100)]) = MAP(ARRAY[3.0E0, 1.0E0], ARRAY[from_unixtime(1), from_unixtime(100)])");
        assertQuery("select MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens', 'puppies']) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['puppies', 'kittens'])");
        assertQuery("select MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens', 'puppies']) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['kittens', 'puppies'])");
        assertQuery("select MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])");
        assertQuery("select MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[3], ARRAY[1, 2]])");
        assertQuery("select MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]])");
        assertQuery("select MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[3], ARRAY[1, 2]])");
        assertQuery("select MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['cat', 'dog']], ARRAY[ARRAY[1, 2], ARRAY[3]]) = MAP(ARRAY[ARRAY['kittens', 'puppies'], ARRAY['dog', 'cat']], ARRAY[ARRAY[1, 2], ARRAY[3]])");
        assertQuery("select MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]) = MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3])");
        assertQuery("select MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]) = MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.2])");

        // nulls
        assertQuery("select MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])");
        assertQuery("select MAP(ARRAY['kittens', 'puppies'], ARRAY[3, 3]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])");
        assertQuery("select MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 2])");
        assertQuery("select MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL]) = MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL])");
        assertQuery("select MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[NULL, FALSE]) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[FALSE, NULL])");
        assertQuery("select MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[TRUE, NULL]) = MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[TRUE, NULL])");
        assertQuery("select MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, null]) = MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, null])");
        assertQuery("select MAP(ARRAY [1.0, 2.1], ARRAY [null, null]) = MAP(ARRAY [1.0, 2.1], ARRAY [null, null])");
    }

    @Test
    public void testNotEquals()
    {
        // single item
        assertQuery("select MAP(ARRAY[1], ARRAY[2]) != MAP(ARRAY[1], ARRAY[2])");
        assertQuery("select MAP(ARRAY[1], ARRAY[2]) != MAP(ARRAY[1], ARRAY[4])");
        assertQuery("select MAP(ARRAY[3], ARRAY[1]) != MAP(ARRAY[2], ARRAY[1])");
        assertQuery("select MAP(ARRAY[2.2], ARRAY[3.1]) != MAP(ARRAY[2.2], ARRAY[3.1])");
        assertQuery("select MAP(ARRAY[2.2], ARRAY[3.1]) != MAP(ARRAY[2.2], ARRAY[3.0])");
        assertQuery("select MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000]) " +
                "!= MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000])");
        assertQuery("select MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000003.30000000000000]) " +
                "!= MAP(ARRAY[383838383838383.12324234234234], ARRAY[000000000000013.30000000000000])");

        // multiple items
        assertQuery("select MAP(ARRAY[1], ARRAY[2]) != MAP(ARRAY[1, 3], ARRAY[2, 4])");
        assertQuery("select MAP(ARRAY[1, 3], ARRAY[2, 4]) != MAP(ARRAY[1], ARRAY[2])");
        assertQuery("select MAP(ARRAY[1, 3], ARRAY[2, 4]) != MAP(ARRAY[3, 1], ARRAY[4, 2])");
        assertQuery("select MAP(ARRAY[1, 3], ARRAY[2, 4]) != MAP(ARRAY[3, 1], ARRAY[2, 4])");
        assertQuery("select MAP(ARRAY['1', '3'], ARRAY[2.0E0, 4.0E0]) != MAP(ARRAY['3', '1'], ARRAY[4.0E0, 2.0E0])");
        assertQuery("select MAP(ARRAY['1', '3'], ARRAY[2.0E0, 4.0E0]) != MAP(ARRAY['3', '1'], ARRAY[2.0E0, 4.0E0])");
        assertQuery("select MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) != MAP(ARRAY[FALSE, TRUE], ARRAY['4', '2'])");
        assertQuery("select MAP(ARRAY[TRUE, FALSE], ARRAY['2', '4']) != MAP(ARRAY[FALSE, TRUE], ARRAY['2', '4'])");
        assertQuery("select MAP(ARRAY[1.0E0, 3.0E0], ARRAY[TRUE, FALSE]) != MAP(ARRAY[3.0E0, 1.0E0], ARRAY[FALSE, TRUE])");
        assertQuery("select MAP(ARRAY[1.0E0, 3.0E0], ARRAY[TRUE, FALSE]) != MAP(ARRAY[3.0E0, 1.0E0], ARRAY[TRUE, FALSE])");
        assertQuery("select MAP(ARRAY[1.0E0, 3.0E0], ARRAY[from_unixtime(1), from_unixtime(100)]) != MAP(ARRAY[3.0E0, 1.0E0], ARRAY[from_unixtime(100), from_unixtime(1)])");
        assertQuery("select MAP(ARRAY[1.0E0, 3.0E0], ARRAY[from_unixtime(1), from_unixtime(100)]) != MAP(ARRAY[3.0E0, 1.0E0], ARRAY[from_unixtime(1), from_unixtime(100)])");
        assertQuery("select MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens','puppies']) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['puppies', 'kittens'])");
        assertQuery("select MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY['kittens','puppies']) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY['kittens', 'puppies'])");
        assertQuery("select MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) != MAP(ARRAY['kittens','puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]])");
        assertQuery("select MAP(ARRAY['kittens', 'puppies'], ARRAY[ARRAY[1, 2], ARRAY[3]]) != MAP(ARRAY['kittens','puppies'], ARRAY[ARRAY[3], ARRAY[1, 2]])");
        assertQuery("select MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]) != MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3])");
        assertQuery("select MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]) != MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.2])");

        // nulls
        assertQuery("select MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3]) != MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])");
        assertQuery("select MAP(ARRAY['kittens', 'puppies'], ARRAY[3, 3]) != MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3])");
        assertQuery("select MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 3]) != MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, 2])");
        assertQuery("select MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL]) != MAP(ARRAY['kittens', 'puppies'], ARRAY[NULL, NULL])");
        assertQuery("select MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[NULL, FALSE]) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[FALSE, NULL])");
        assertQuery("select MAP(ARRAY[from_unixtime(1), from_unixtime(100)], ARRAY[TRUE, NULL]) != MAP(ARRAY[from_unixtime(100), from_unixtime(1)], ARRAY[TRUE, NULL])");
        assertQuery("select MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, null]) != MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, null])");
        assertQuery("select MAP(ARRAY [1.0, 2.1], ARRAY [null, null]) != MAP(ARRAY [1.0, 2.1], ARRAY [null, null])");
    }

    @Test
    public void testDistinctFrom()
    {
        assertQuery("select CAST(NULL AS MAP<INTEGER, VARCHAR>) IS DISTINCT FROM CAST(NULL AS MAP<INTEGER, VARCHAR>)");
        assertQuery("select MAP(ARRAY[1], ARRAY[2]) IS DISTINCT FROM NULL");
        assertQuery("select NULL IS DISTINCT FROM MAP(ARRAY[1], ARRAY[2])");

        assertQuery("select MAP(ARRAY[1], ARRAY[2]) IS DISTINCT FROM MAP(ARRAY[1], ARRAY[2])");
        assertQuery("select MAP(ARRAY[1], ARRAY[NULL]) IS DISTINCT FROM MAP(ARRAY[1], ARRAY[NULL])");
        assertQuery("select MAP(ARRAY[1], ARRAY[0]) IS DISTINCT FROM MAP(ARRAY[1], ARRAY[NULL])");
        assertQuery("select MAP(ARRAY[1], ARRAY[NULL]) IS DISTINCT FROM MAP(ARRAY[1], ARRAY[0])");

        assertQuery("select MAP(ARRAY[1, 2], ARRAY['kittens','puppies']) IS DISTINCT FROM MAP(ARRAY[1, 2], ARRAY['puppies', 'kittens'])");
        assertQuery("select MAP(ARRAY[1, 2], ARRAY['kittens','puppies']) IS DISTINCT FROM MAP(ARRAY[1, 2], ARRAY['kittens', 'puppies'])");
        assertQuery("select MAP(ARRAY[1, 3], ARRAY['kittens','puppies']) IS DISTINCT FROM MAP(ARRAY[1, 2], ARRAY['kittens', 'puppies'])");
        assertQuery("select MAP(ARRAY[1, 3], ARRAY['kittens','puppies']) IS DISTINCT FROM MAP(ARRAY[1, 2], ARRAY['kittens', 'pupp111'])");
        assertQuery("select MAP(ARRAY[1, 3], ARRAY['kittens','puppies']) IS DISTINCT FROM MAP(ARRAY[1, 2], ARRAY['kittens', NULL])");
        assertQuery("select MAP(ARRAY[1, 3], ARRAY['kittens','puppies']) IS DISTINCT FROM MAP(ARRAY[1, 2], ARRAY[NULL, NULL])");

        assertQuery("select MAP(ARRAY[1, 3], ARRAY[MAP(ARRAY['kittens'], ARRAY[1e0]), MAP(ARRAY['puppies'], ARRAY[3e0])]) " +
                "IS DISTINCT FROM MAP(ARRAY[1, 3], ARRAY[MAP(ARRAY['kittens'], ARRAY[1e0]), MAP(ARRAY['puppies'], ARRAY[3e0])])");
        assertQuery("select MAP(ARRAY[1, 3], ARRAY[MAP(ARRAY['kittens'], ARRAY[1e0]), MAP(ARRAY['puppies'], ARRAY[3e0])]) " +
                "IS DISTINCT FROM MAP(ARRAY[1, 3], ARRAY[MAP(ARRAY['kittens'], ARRAY[1e0]), MAP(ARRAY['puppies'], ARRAY[4e0])])");
    }

    @Test
    public void testMapConcat()
    {
        assertQuery("select MAP_CONCAT(MAP (ARRAY [TRUE], ARRAY [1]), MAP (CAST(ARRAY [] AS ARRAY(BOOLEAN)), CAST(ARRAY [] AS ARRAY(INTEGER))))");
        // <BOOLEAN, INTEGER> Tests
        assertQuery("select MAP_CONCAT(MAP (ARRAY [TRUE], ARRAY [1]), MAP (ARRAY [TRUE, FALSE], ARRAY [10, 20]))");
        assertQuery("select MAP_CONCAT(MAP (ARRAY [TRUE, FALSE], ARRAY [1, 2]), MAP (ARRAY [TRUE, FALSE], ARRAY [10, 20]))");
        assertQuery("select MAP_CONCAT(MAP (ARRAY [TRUE, FALSE], ARRAY [1, 2]), MAP (ARRAY [TRUE], ARRAY [10]))");

        // <VARCHAR, INTEGER> Tests
        assertQuery("select MAP_CONCAT(MAP (ARRAY ['1', '2', '3'], ARRAY [1, 2, 3]), MAP (ARRAY ['1', '2', '3', '4'], ARRAY [10, 20, 30, 40]))");
        assertQuery("select MAP_CONCAT(MAP (ARRAY ['1', '2', '3', '4'], ARRAY [1, 2, 3, 4]), MAP (ARRAY ['1', '2', '3', '4'], ARRAY [10, 20, 30, 40]))");
        assertQuery("select MAP_CONCAT(MAP (ARRAY ['1', '2', '3', '4'], ARRAY [1, 2, 3, 4]), MAP (ARRAY ['1', '2', '3'], ARRAY [10, 20, 30]))");

        // <BIGINT, ARRAY<DOUBLE>> Tests
        assertQuery("select MAP_CONCAT(MAP (ARRAY [1, 2, 3], ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0]]), MAP (ARRAY [1, 2, 3, 4], ARRAY [ARRAY [10.0E0], ARRAY [20.0E0], ARRAY [30.0E0], ARRAY [40.0E0]]))");
        assertQuery("select MAP_CONCAT(MAP (ARRAY [1, 2, 3, 4], ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0], ARRAY [4.0E0]]), MAP (ARRAY [1, 2, 3, 4], ARRAY [ARRAY [10.0E0], ARRAY [20.0E0], ARRAY [30.0E0], ARRAY [40.0E0]]))");
        assertQuery("select MAP_CONCAT(MAP (ARRAY [1, 2, 3, 4], ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0], ARRAY [4.0E0]]), MAP (ARRAY [1, 2, 3], ARRAY [ARRAY [10.0E0], ARRAY [20.0E0], ARRAY [30.0E0]]))");

        // <ARRAY<DOUBLE>, VARCHAR> Tests
        assertQuery("select MAP_CONCAT(MAP (ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0]], ARRAY ['1', '2', '3']), MAP (ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0], ARRAY [4.0E0]], ARRAY ['10', '20', '30', '40']))");
        assertQuery("select MAP_CONCAT(MAP (ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0]], ARRAY ['1', '2', '3']), MAP (ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0], ARRAY [4.0E0]], ARRAY ['10', '20', '30', '40']))");
        assertQuery("select MAP_CONCAT(MAP (ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0], ARRAY [4.0E0]], ARRAY ['1', '2', '3', '4']), MAP (ARRAY [ARRAY [1.0E0], ARRAY [2.0E0], ARRAY [3.0E0]], ARRAY ['10', '20', '30']))");

        // Tests for concatenating multiple maps
        assertQuery("select MAP_CONCAT(MAP(ARRAY[1], ARRAY[-1]), NULL, MAP(ARRAY[3], ARRAY[-3]))");
        assertQuery("select MAP_CONCAT(MAP(ARRAY[1], ARRAY[-1]), MAP(ARRAY[2], ARRAY[-2]), MAP(ARRAY[3], ARRAY[-3]))");
        assertQuery("select MAP_CONCAT(MAP(ARRAY[1], ARRAY[-1]), MAP(ARRAY[1], ARRAY[-2]), MAP(ARRAY[1], ARRAY[-3]))");
        assertQuery("select MAP_CONCAT(MAP(ARRAY[1], ARRAY[-1]), MAP(ARRAY[], ARRAY[]), MAP(ARRAY[3], ARRAY[-3]))");
        assertQuery("select MAP_CONCAT(MAP(ARRAY[], ARRAY[]), MAP(ARRAY['a_string'], ARRAY['b_string']), cast(MAP(ARRAY[], ARRAY[]) AS MAP(VARCHAR, VARCHAR)))");
        assertQuery("select MAP_CONCAT(MAP(ARRAY[], ARRAY[]), MAP(ARRAY[], ARRAY[]), MAP(ARRAY[], ARRAY[]))");
        assertQuery("select MAP_CONCAT(MAP(), MAP(), MAP())");
        assertQuery("select MAP_CONCAT(MAP(ARRAY[1], ARRAY[-1]), MAP(), MAP(ARRAY[3], ARRAY[-3]))");
        assertQuery("select MAP_CONCAT(MAP(ARRAY[TRUE], ARRAY[1]), MAP(ARRAY[TRUE, FALSE], ARRAY[10, 20]), MAP(ARRAY[FALSE], ARRAY[0]))");

        assertQuery("select MAP_CONCAT(MAP (ARRAY ['1', '2', '3'], ARRAY [1, 2, 3]), MAP (ARRAY ['1', '2', '3', '4'], ARRAY [10, 20, 30, 40]))");

        // <DECIMAL, DECIMAL>
        assertQuery("select MAP_CONCAT(MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.2, 3.3]), MAP(ARRAY [1.0, 383838383838383.12324234234234], ARRAY [2.1, 3.2]))");
        assertQuery("select MAP_CONCAT(MAP(ARRAY [1.0], ARRAY [2.2]), MAP(ARRAY [5.1], ARRAY [3.2]))");

        // Decimal with type only coercion
        assertQuery("select MAP_CONCAT(MAP(ARRAY [1.0], ARRAY [2.2]), MAP(ARRAY [5.1], ARRAY [33.2]))");
        assertQuery("select MAP_CONCAT(MAP(ARRAY [1.0], ARRAY [2.2]), MAP(ARRAY [55.1], ARRAY [33.2]))");

        assertQuery("select MAP_CONCAT(MAP(ARRAY [1.0], ARRAY [2.2]), MAP(ARRAY [5.1], ARRAY [33.22]))");
        assertQuery("select MAP_CONCAT(MAP(ARRAY [1.0], ARRAY [2.2]), MAP(ARRAY [5.1], ARRAY [00000000000000002.2]))");
    }

    @Test
    public void testMapToMapCast()
    {
        assertQuery("select CAST(MAP(ARRAY['1', '100'], ARRAY[true, false]) AS MAP<varchar,bigint>)");
        assertQuery("select CAST(MAP(ARRAY[1,2], ARRAY[1,2]) AS MAP<bigint, boolean>)");
        assertQuery("select CAST(MAP(ARRAY[1,2], ARRAY[array[1],array[2]]) AS MAP<bigint, array<boolean>>)");
        assertQuery("select CAST(MAP(ARRAY[1], ARRAY[MAP(ARRAY[1.0E0], ARRAY[false])]) AS MAP<varchar, MAP(bigint,bigint)>)");
        assertQuery("select CAST(MAP(ARRAY[1,2], ARRAY[DATE '2016-01-02', DATE '2016-02-03']) AS MAP(bigint, varchar))");
        assertQuery("select CAST(MAP(ARRAY[1,2], ARRAY[TIMESTAMP '2016-01-02 01:02:03', TIMESTAMP '2016-02-03 03:04:05']) AS MAP(bigint, varchar))");
        assertQuery("select CAST(MAP(ARRAY['123', '456'], ARRAY[1.23456E0, 2.34567E0]) AS MAP(integer, real))");
        assertQuery("select CAST(MAP(ARRAY['123', '456'], ARRAY[1.23456E0, 2.34567E0]) AS MAP(smallint, decimal(6,5)))");
        assertQuery("select CAST(MAP(ARRAY[json '1'], ARRAY[1]) AS MAP(bigint, bigint))");
        assertQuery("select CAST(MAP(ARRAY['1'], ARRAY[json '1']) AS MAP(bigint, bigint))");

        // null values
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select CAST(MAP(ARRAY[0, 1, 2, 3], ARRAY[1,NULL, NULL, 2]) AS MAP<BIGINT, DOUBLE>)");

        assertQueryFails("select CAST(MAP(ARRAY[1, 2], ARRAY[6, 9]) AS MAP<boolean, bigint>)", "duplicate keys");
        assertQueryFails("select CAST(MAP(ARRAY[json 'null'], ARRAY[1]) AS MAP<bigint, bigint>)", "map key is null");
    }

    @Test
    public void testMapFromEntries()
    {
        assertQuery("select map_from_entries(null)");
        assertQuery("select map_from_entries(ARRAY[])");
        assertQuery("select map_from_entries(CAST(ARRAY[] AS ARRAY(ROW(DOUBLE, BIGINT))))");
        assertQuery("select map_from_entries(ARRAY[(1, 3)])");
        assertQuery("select map_from_entries(ARRAY[(1, 'x'), (2, 'y')])");
        assertQuery("select map_from_entries(ARRAY[('x', 1.0E0), ('y', 2.0E0)])");

        assertQuery("select map_from_entries(ARRAY[('x', ARRAY[1, 2]), ('y', ARRAY[3, 4])])");
        assertQuery("select map_from_entries(ARRAY[(ARRAY[1, 2], 'x'), (ARRAY[3, 4], 'y')])");
        assertQuery("select map_from_entries(ARRAY[('x', MAP(ARRAY[1], ARRAY[2])), ('y', MAP(ARRAY[3], ARRAY[4]))])");
        assertQuery("select map_from_entries(ARRAY[(MAP(ARRAY[1], ARRAY[2]), 'x'), (MAP(ARRAY[3], ARRAY[4]), 'y')])");

        // null values
        // TODO_PRESTISSIMO_FIX
        // assertQuery("select map_from_entries(ARRAY[('x', null), ('y', null)])");

        // invalid invocation
        assertQueryFails("select map_from_entries(ARRAY[('a', 1), ('a', 2)])", "Duplicate keys \\(a\\) are not allowed");
        assertQueryFails("select map_from_entries(ARRAY[(1, 1), (1, 2)])", "Duplicate keys \\(1\\) are not allowed");
        assertQueryFails("select map_from_entries(ARRAY[(1.0, 1), (1.0, 2)])", "Duplicate keys \\(1.0\\) are not allowed");
        assertQueryFails("select map_from_entries(ARRAY[(ARRAY[1, 2], 1), (ARRAY[1, 2], 2)])", "Duplicate keys \\(\\[1, 2\\]\\) are not allowed");
        assertQueryFails("select map_from_entries(ARRAY[(MAP(ARRAY[1], ARRAY[2]), 1), (MAP(ARRAY[1], ARRAY[2]), 2)])", "Duplicate keys \\(\\{1=2\\}\\) are not allowed");
        assertQueryFails("select map_from_entries(ARRAY[(null, 1), (null, 2)])", "map key cannot be null");
        assertQueryFails("select map_from_entries(ARRAY[null])", "map entry cannot be null");
        assertQueryFails("select map_from_entries(ARRAY[(1, 2), null])", "map entry cannot be null");

        assertQuery("select map_from_entries(ARRAY[('a', 1.0), ('b', 2.0), ('c', 3.0), ('d', 4.0), ('e', 5.0), ('f', 6.0)])");
    }

    @Test
    public void testMultimapFromEntries()
    {
        assertQuery("select multimap_from_entries(null)");
        assertQuery("select multimap_from_entries(ARRAY[])");
        assertQuery("select multimap_from_entries(CAST(ARRAY[] AS ARRAY(ROW(DOUBLE, BIGINT))))");

        assertQuery("select multimap_from_entries(ARRAY[(1, 3), (2, 4), (1, 6), (1, 8), (2, 10)])");
        assertQuery("select multimap_from_entries(ARRAY[(1, 'x'), (2, 'y'), (1, 'a'), (3, 'b'), (2, 'c'), (3, null)])");
        assertQuery("select multimap_from_entries(ARRAY[('x', 1.0E0), ('y', 2.0E0), ('z', null), ('x', 1.5E0), ('y', 2.5E0)])");

        // invalid invocation
        assertQueryFails("select multimap_from_entries(ARRAY[(null, 1), (null, 2)])", "map key cannot be null");
        assertQueryFails("select multimap_from_entries(ARRAY[null])", "map entry cannot be null");
        assertQueryFails("select multimap_from_entries(ARRAY[(1, 2), null])", "map entry cannot be null");

        assertQuery("select multimap_from_entries(ARRAY[('a', 1.0), ('b', 2.0), ('a', 3.0), ('c', 4.0), ('b', 5.0), ('c', 6.0)])");
    }

    @Test
    public void testMapEntries()
    {
        assertQuery("select map_entries(null)");
        assertQuery("select map_entries(MAP(ARRAY[], null))");
        assertQuery("select map_entries(MAP(null, ARRAY[]))");
        assertQuery("select map_entries(MAP(ARRAY[1, 2, 3], null))");
        assertQuery("select map_entries(MAP(null, ARRAY[1, 2, 3]))");
        assertQuery("select map_entries(MAP(ARRAY[], ARRAY[]))");
        assertQuery("select map_entries(MAP(ARRAY[1], ARRAY['x']))");
        assertQuery("select map_entries(MAP(ARRAY[1, 2], ARRAY['x', 'y']))");

        assertQuery("select map_entries(MAP(ARRAY['x', 'y'], ARRAY[ARRAY[1, 2], ARRAY[3, 4]]))");
        assertQuery("select map_entries(MAP(ARRAY[ARRAY[1.0E0, 2.0E0], ARRAY[3.0E0, 4.0E0]], ARRAY[5.0E0, 6.0E0]))");
        assertQuery("select map_entries(MAP(ARRAY['x', 'y'], ARRAY[MAP(ARRAY[1], ARRAY[2]), MAP(ARRAY[3], ARRAY[4])]))");
        assertQuery("select map_entries(MAP(ARRAY[MAP(ARRAY[1], ARRAY[2]), MAP(ARRAY[3], ARRAY[4])], ARRAY['x', 'y']))");

        // null values
        List<Object> expectedEntries = ImmutableList.of(asList("x", null), asList("y", null));
        assertQuery("select map_entries(MAP(ARRAY['x', 'y'], ARRAY[null, null]))");

        assertQuery("select map_entries(MAP(ARRAY[1, 2], ARRAY['x', 'y']))");
    }

    @Test
    public void testEntryMappings()
    {
        assertQuery("select map_from_entries(map_entries(MAP(ARRAY[1, 2, 3], ARRAY['x', 'y', 'z'])))");
        assertQuery("select map_entries(map_from_entries(ARRAY[(1, 'x'), (2, 'y'), (3, 'z')]))");
    }

    @Test
    public void testIndeterminate()
    {
        assertQuery("select \"$operator$indeterminate\"(cast(null as map(bigint, bigint)))");
        assertQuery("select \"$operator$indeterminate\"(map(array[1,2], array[3,4]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[1,2], array[1.0,2.0]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[1,2], array[null, 3]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[1,2], array[null, 3.0]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[1,2], array[array[11], array[22]]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[1,2], array[array[11], array[null]]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[1,2], array[array[11], array[22,null]]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[1,2], array[array[11, null], array[22,null]]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[1,2], array[array[null], array[null]]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[array[1], array[2]], array[array[11], array[22]]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[row(1), row(2)], array[array[11], array[22]]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[row(1), row(2)], array[array[11], array[22, null]]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[1E0, 2E0], array[11E0, null]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[1E0, 2E0], array[11E0, 12E0]))");
        assertQuery("select \"$operator$indeterminate\"(map(array['a', 'b'], array['c', null]))");
        assertQuery("select \"$operator$indeterminate\"(map(array['a', 'b'], array['c', 'd']))");
        assertQuery("select \"$operator$indeterminate\"(map(array[true,false], array[false,true]))");
        assertQuery("select \"$operator$indeterminate\"(map(array[true,false], array[false,null]))");
    }

    @Test
    public void testMapHashOperator()
    {
        assertQuery("select \"$operator$hash_code\"(MAP(ARRAY[1], ARRAY[2]))");
        assertQuery("select \"$operator$hash_code\"(MAP(ARRAY[1, 2147483647], ARRAY[2147483647, 2]))");
        assertQuery("select \"$operator$hash_code\"(MAP(ARRAY[8589934592], ARRAY[2]))");
        assertQuery("select \"$operator$hash_code\"(MAP(ARRAY[true], ARRAY[false]))");
        assertQuery("select \"$operator$hash_code\"(MAP(ARRAY['123'], ARRAY['456']))");
    }
}
