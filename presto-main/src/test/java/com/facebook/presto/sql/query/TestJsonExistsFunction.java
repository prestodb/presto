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
public class TestJsonExistsFunction
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
    public void testJsonExists()
    {
        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'lax $[1]')",
                "VALUES true");

        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'strict $[1]')",
                "VALUES true");

        // structural error suppressed by the path engine in lax mode. empty sequence is returned, so exists returns false
        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'lax $[100]')",
                "VALUES false");

        // structural error not suppressed by the path engine in strict mode, and handled accordingly to the ON ERROR clause

        // default error behavior is FALSE ON ERROR
        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'strict $[100]')",
                "VALUES false");

        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'strict $[100]' TRUE ON ERROR)",
                "VALUES true");

        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'strict $[100]' FALSE ON ERROR)",
                "VALUES false");

        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'strict $[100]' UNKNOWN ON ERROR)",
                "VALUES cast(null AS boolean)");

        assertions.assertFails(
                "SELECT json_exists('" + INPUT + "', 'strict $[100]' ERROR ON ERROR)",
                "path evaluation failed: structural error: invalid array subscript: \\[100, 100\\] for array of size 3");
    }

    @Test
    public void testInputFormat()
    {
        // FORMAT JSON is default for character string input
        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'lax $[1]')",
                "VALUES true");

        // FORMAT JSON is the only supported format for character string input
        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "' FORMAT JSON, 'lax $[1]')",
                "VALUES true");

        assertions.assertFails(
                "SELECT json_exists('" + INPUT + "' FORMAT JSON ENCODING UTF8, 'lax $[1]')",
                "line 1:20: Cannot read input of type varchar[(]15[)] as JSON using formatting JSON ENCODING UTF8");

        // FORMAT JSON is default for binary string input
        byte[] bytes = INPUT.getBytes(UTF_8);
        String varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertions.assertQuery(
                "SELECT json_exists(" + varbinaryLiteral + ", 'lax $[1]')",
                "VALUES true");

        assertions.assertQuery(
                "SELECT json_exists(" + varbinaryLiteral + " FORMAT JSON, 'lax $[1]')",
                "VALUES true");

        // FORMAT JSON ENCODING ... is supported for binary string input
        assertions.assertQuery(
                "SELECT json_exists(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF8, 'lax $[1]')",
                "VALUES true");

        bytes = INPUT.getBytes(UTF_16LE);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertions.assertQuery(
                "SELECT json_exists(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF16, 'lax $[1]')",
                "VALUES true");

        bytes = INPUT.getBytes(Charset.forName("UTF-32LE"));
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertions.assertQuery(
                "SELECT json_exists(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF32, 'lax $[1]')",
                "VALUES true");

        // the encoding must match the actual data
        String finalVarbinaryLiteral = varbinaryLiteral;
        assertions.assertFails(
                "SELECT json_exists(" + finalVarbinaryLiteral + " FORMAT JSON ENCODING UTF8, 'lax $[1]' ERROR ON ERROR)",
                "conversion to JSON failed: ");
    }

    @Test
    public void testInputConversionError()
    {
        // input conversion error is handled accordingly to the ON ERROR clause

        // default error behavior is FALSE ON ERROR
        assertions.assertQuery(
                "SELECT json_exists('" + INCORRECT_INPUT + "', 'lax $[1]')",
                "VALUES false");

        assertions.assertQuery(
                "SELECT json_exists('" + INCORRECT_INPUT + "', 'strict $[1]' TRUE ON ERROR)",
                "VALUES true");

        assertions.assertQuery(
                "SELECT json_exists('" + INCORRECT_INPUT + "', 'strict $[1]' FALSE ON ERROR)",
                "VALUES false");

        assertions.assertQuery(
                "SELECT json_exists('" + INCORRECT_INPUT + "', 'strict $[1]' UNKNOWN ON ERROR)",
                "VALUES cast(null AS boolean)");

        assertions.assertFails(
                "SELECT json_exists('" + INCORRECT_INPUT + "', 'strict $[1]' ERROR ON ERROR)",
                "conversion to JSON failed: ");
    }

    @Test
    public void testPassingClause()
    {
        // watch out for case sensitive identifiers in JSON path
        assertions.assertFails(
                "SELECT json_exists('" + INPUT + "', 'lax $number + 1' PASSING 2 AS number)",
                "line 1:39: no value passed for parameter number. Try quoting \"number\" in the PASSING clause to match case");

        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'lax $number + 1' PASSING 5 AS \"number\")",
                "VALUES true");

        // JSON parameter
        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'lax $array[0]' PASSING '[1, 2, 3]' FORMAT JSON AS \"array\")",
                "VALUES true");

        // input conversion error of JSON parameter is handled accordingly to the ON ERROR clause
        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'lax $array[0]' PASSING '[...' FORMAT JSON AS \"array\")",
                "VALUES false");

        assertions.assertFails(
                "SELECT json_exists('" + INPUT + "', 'lax $array[0]' PASSING '[...' FORMAT JSON AS \"array\" ERROR ON ERROR)",
                "conversion to JSON failed: ");

        // array index out of bounds
        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'lax $[$number]' PASSING 5 AS \"number\")",
                "VALUES false");
    }

    @Test
    public void testIncorrectPath()
    {
        assertions.assertFails(
                "SELECT json_exists('" + INPUT + "', 'certainly not a valid path')",
                "line 1:41: mismatched input 'certainly' expecting [{]'lax', 'strict'[}]");
    }

    @Test
    public void testNullInput()
    {
        // null as input item
        assertions.assertQuery(
                "SELECT json_exists(null, 'lax $')",
                "VALUES cast(null AS boolean)");

        // null as SQL-value parameter is evaluated to a JSON null
        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'lax $var' PASSING null AS \"var\")",
                "VALUES true");

        // null as JSON parameter is evaluated to empty sequence
        assertions.assertQuery(
                "SELECT json_exists('" + INPUT + "', 'lax $var' PASSING null FORMAT JSON AS \"var\")",
                "VALUES false");
    }
}
