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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.Charset;

import static com.google.common.io.BaseEncoding.base16;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestJsonValueFunction
{
    private static final String INPUT = "[\"a\", \"b\", \"c\"]";
    private static final String INCORRECT_INPUT = "[...";
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testJsonValue()
    {
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $[1]')",
                "VALUES VARCHAR 'b'");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'strict $[1]')",
                "VALUES VARCHAR 'b'");

        // structural error suppressed by the path engine in lax mode. resulting sequence consists of the last item in the array
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $[2 to 100]')",
                "VALUES VARCHAR 'c'");

        // structural error not suppressed by the path engine in strict mode, and handled accordingly to the ON ERROR clause

        // default error behavior is NULL ON ERROR
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'strict $[100]')",
                "VALUES cast(null AS varchar)");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'strict $[100]' NULL ON ERROR)",
                "VALUES cast(null AS varchar)");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'strict $[100]' DEFAULT 'x' ON ERROR)",
                "VALUES VARCHAR 'x'");

        assertions.assertFails(
                "SELECT json_value('" + INPUT + "', 'strict $[100]' ERROR ON ERROR)",
                "path evaluation failed: structural error: invalid array subscript: \\[100, 100\\] for array of size 3");

        // structural error suppressed by the path engine in lax mode. empty sequence is returned, so ON EMPTY behavior is applied

        // default empty behavior is NULL ON EMPTY
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $[100]')",
                "VALUES cast(null AS varchar)");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $[100]' NULL ON EMPTY)",
                "VALUES cast(null AS varchar)");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $[100]' DEFAULT 'x' ON EMPTY)",
                "VALUES VARCHAR 'x'");

        assertions.assertFails(
                "SELECT json_value('" + INPUT + "', 'lax $[100]' ERROR ON EMPTY)",
                "cannot extract SQL scalar from JSON: JSON path found no items");

        // path returns multiple items. this case is handled accordingly to the ON ERROR clause

        // default error behavior is NULL ON ERROR
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $[0 to 2]')",
                "VALUES cast(null AS varchar)");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $[0 to 2]' NULL ON ERROR)",
                "VALUES cast(null AS varchar)");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $[0 to 2]' DEFAULT 'x' ON ERROR)",
                "VALUES VARCHAR 'x'");

        assertions.assertFails(
                "SELECT json_value('" + INPUT + "', 'lax $[0 to 2]' ERROR ON ERROR)",
                "cannot extract SQL scalar from JSON: JSON path found multiple items");
    }

    @Test
    public void testInputFormat()
    {
        // FORMAT JSON is default for character string input
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $[1]')",
                "VALUES VARCHAR 'b'");

        // FORMAT JSON is the only supported format for character string input
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "' FORMAT JSON, 'lax $[1]')",
                "VALUES VARCHAR 'b'");

        assertions.assertFails(
                "SELECT json_value('" + INPUT + "' FORMAT JSON ENCODING UTF8, 'lax $[1]')",
                "line 1:19: Cannot read input of type varchar\\(15\\) as JSON using formatting JSON ENCODING UTF8");

        // FORMAT JSON is default for binary string input
        byte[] bytes = INPUT.getBytes(UTF_8);
        String varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertions.assertQuery(
                "SELECT json_value(" + varbinaryLiteral + ", 'lax $[1]')",
                "VALUES VARCHAR 'b'");

        assertions.assertQuery(
                "SELECT json_value(" + varbinaryLiteral + " FORMAT JSON, 'lax $[1]')",
                "VALUES VARCHAR 'b'");

        // FORMAT JSON ENCODING ... is supported for binary string input
        assertions.assertQuery(
                "SELECT json_value(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF8, 'lax $[1]')",
                "VALUES VARCHAR 'b'");

        bytes = INPUT.getBytes(UTF_16LE);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertions.assertQuery(
                "SELECT json_value(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF16, 'lax $[1]')",
                "VALUES VARCHAR 'b'");

        bytes = INPUT.getBytes(Charset.forName("UTF-32LE"));
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertions.assertQuery(
                "SELECT json_value(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF32, 'lax $[1]')",
                "VALUES VARCHAR 'b'");

        // the encoding must match the actual data
        String finalVarbinaryLiteral = varbinaryLiteral;
        assertions.assertFails(
                "SELECT json_value(" + finalVarbinaryLiteral + " FORMAT JSON ENCODING UTF8, 'lax $[1]' ERROR ON ERROR)",
                "conversion to JSON failed: ");
    }

    @Test
    public void testInputConversionError()
    {
        // input conversion error is handled accordingly to the ON ERROR clause

        // default error behavior is NULL ON ERROR
        assertions.assertQuery(
                "SELECT json_value('" + INCORRECT_INPUT + "', 'lax $[1]')",
                "VALUES cast(null AS varchar)");

        assertions.assertQuery(
                "SELECT json_value('" + INCORRECT_INPUT + "', 'lax $[1]' NULL ON ERROR)",
                "VALUES cast(null AS varchar)");

        assertions.assertQuery(
                "SELECT json_value('" + INCORRECT_INPUT + "', 'lax $[1]' DEFAULT 'x' ON ERROR)",
                "VALUES VARCHAR 'x'");

        assertions.assertFails(
                "SELECT json_value('" + INCORRECT_INPUT + "', 'lax $[1]' ERROR ON ERROR)",
                "conversion to JSON failed: ");
    }

    @Test
    public void testPassingClause()
    {
        // watch out for case sensitive identifiers in JSON path
        assertions.assertFails(
                "SELECT json_value('" + INPUT + "', 'lax $number + 1' PASSING 2 AS number)",
                "line 1:38: no value passed for parameter number. Try quoting \"number\" in the PASSING clause to match case");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $number + 1' PASSING 5 AS \"number\")",
                "VALUES VARCHAR '6'");

        // JSON parameter
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $array[0]' PASSING '[1, 2, 3]' FORMAT JSON AS \"array\")",
                "VALUES VARCHAR '1'");

        // input conversion error of JSON parameter is handled accordingly to the ON ERROR clause
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $array[0]' PASSING '[...' FORMAT JSON AS \"array\")",
                "VALUES cast(null AS varchar)");

        assertions.assertFails(
                "SELECT json_value('" + INPUT + "', 'lax $array[0]' PASSING '[...' FORMAT JSON AS \"array\" ERROR ON ERROR)",
                "conversion to JSON failed: ");

        // array index out of bounds
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $[$number]' PASSING 5 AS \"number\")",
                "VALUES cast(null AS varchar)");
    }

    @Test
    public void testReturnedType()
    {
        // default returned type is varchar
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax 1')",
                "VALUES VARCHAR '1'");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax true')",
                "VALUES VARCHAR 'true'");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax null')",
                "VALUES cast(null AS varchar)");

        // explicit returned type
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $[1]' RETURNING char(10))",
                "VALUES cast('b' AS char(10))");

        // the actual value does not fit in the expected returned type. the error is handled accordingly to the ON ERROR clause
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax 1000' RETURNING tinyint)",
                "VALUES cast(null AS tinyint)");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax 1000' RETURNING tinyint DEFAULT TINYINT '-1' ON ERROR)",
                "VALUES TINYINT '-1'");

        // default value cast to the expected returned type
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax 1000000000000 * 1000000000000' RETURNING bigint DEFAULT TINYINT '-1' ON ERROR)",
                "VALUES BIGINT '-1'");
    }

    @Test
    public void testPathResultNonScalar()
    {
        // JSON array ir object cannot be returned as SQL scalar value. the error is handled accordingly to the ON ERROR clause

        // default error behavior is NULL ON ERROR
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $')",
                "VALUES cast(null AS varchar)");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $' NULL ON ERROR)",
                "VALUES cast(null AS varchar)");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $' DEFAULT 'x' ON ERROR)",
                "VALUES VARCHAR 'x'");

        assertions.assertFails(
                "SELECT json_value('" + INPUT + "', 'lax $' ERROR ON ERROR)",
                "cannot extract SQL scalar from JSON: JSON path found an item that cannot be converted to an SQL value");
    }

    @Test
    public void testIncorrectPath()
    {
        assertions.assertFails(
                "SELECT json_value('" + INPUT + "', 'certainly not a valid path')",
                "line 1:40: mismatched input 'certainly' expecting \\{'lax', 'strict'\\}");
    }

    @Test
    public void testNullInput()
    {
        // null as input item
        assertions.assertQuery(
                "SELECT json_value(null, 'lax $')",
                "VALUES cast(null AS varchar)");

        // null as SQL-value parameter is evaluated to a JSON null, and the corresponding returned SQL value is null
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $var' PASSING null AS \"var\")",
                "VALUES cast(null AS varchar)");

        // null as JSON parameter is evaluated to empty sequence. this condition is handled accordingly to ON EMPTY behavior
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $var' PASSING null FORMAT JSON AS \"var\" DEFAULT 'was empty...' ON EMPTY)",
                "VALUES cast('was empty...' AS  varchar)");

        // null as empty default and error default
        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax 1' DEFAULT null ON EMPTY DEFAULT null ON ERROR)",
                "VALUES cast(1 AS varchar)");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax $[100]' DEFAULT null ON EMPTY)",
                "VALUES cast(null AS  varchar)");

        assertions.assertQuery(
                "SELECT json_value('" + INPUT + "', 'lax 1 + $[0]' DEFAULT null ON ERROR)",
                "VALUES cast(null AS  varchar)");
    }
}
