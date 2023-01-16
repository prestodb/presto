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
package com.facebook.presto.operator.scalar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.charset.Charset;

import static com.facebook.presto.json.JsonInputErrorNode.JSON_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.JSON_INPUT_CONVERSION_ERROR;
import static com.facebook.presto.type.Json2016Type.JSON_2016;
import static com.google.common.io.BaseEncoding.base16;
import static java.nio.charset.StandardCharsets.UTF_16BE;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TestJsonInputFunctions
        extends AbstractTestFunctions
{
    private static final String INPUT = "{\"key1\" : 1e0, \"key2\" : true, \"key3\" : null}";
    private static final JsonNode JSON_OBJECT = new ObjectNode(
            JsonNodeFactory.instance,
            ImmutableMap.of("key1", DoubleNode.valueOf(1e0), "key2", BooleanNode.TRUE, "key3", NullNode.instance));
    private static final String ERROR_INPUT = "[...";

    @Test
    public void testVarcharToJson()
    {
        assertFunction(
                "\"$varchar_to_json\"('[]', true)",
                JSON_2016,
                new ArrayNode(JsonNodeFactory.instance));

        assertFunction(
                "\"$varchar_to_json\"('" + INPUT + "', true)",
                JSON_2016,
                JSON_OBJECT);

        // with unsuppressed input conversion error
        assertInvalidFunction(
                "\"$varchar_to_json\"('" + ERROR_INPUT + "', true)",
                JSON_INPUT_CONVERSION_ERROR,
                "conversion to JSON failed: ");

        // with input conversion error suppressed and converted to JSON_ERROR
        assertFunction(
                "\"$varchar_to_json\"('" + ERROR_INPUT + "', false)",
                JSON_2016,
                JSON_ERROR);
    }

    @Test
    public void testVarbinaryUtf8ToJson()
    {
        byte[] bytes = INPUT.getBytes(UTF_8);
        String varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertFunction(
                "\"$varbinary_to_json\"(" + varbinaryLiteral + ", true)",
                JSON_2016,
                JSON_OBJECT);

        assertFunction(
                "\"$varbinary_utf8_to_json\"(" + varbinaryLiteral + ", true)",
                JSON_2016,
                JSON_OBJECT);

        // wrong input encoding
        bytes = INPUT.getBytes(UTF_16LE);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertInvalidFunction(
                "\"$varbinary_utf8_to_json\"(" + varbinaryLiteral + ", true)",
                JSON_INPUT_CONVERSION_ERROR,
                "conversion to JSON failed: ");

        // wrong input encoding; conversion error suppressed and converted to JSON_ERROR
        assertFunction(
                "\"$varbinary_utf8_to_json\"(" + varbinaryLiteral + ", false)",
                JSON_2016,
                JSON_ERROR);

        // correct encoding, incorrect input
        bytes = ERROR_INPUT.getBytes(UTF_8);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        // with unsuppressed input conversion error
        assertInvalidFunction(
                "\"$varbinary_utf8_to_json\"(" + varbinaryLiteral + ", true)",
                JSON_INPUT_CONVERSION_ERROR,
                "conversion to JSON failed: ");

        // with input conversion error suppressed and converted to JSON_ERROR
        assertFunction(
                "\"$varbinary_utf8_to_json\"(" + varbinaryLiteral + ", false)",
                JSON_2016,
                JSON_ERROR);
    }

    @Test
    public void testVarbinaryUtf16ToJson()
    {
        byte[] bytes = INPUT.getBytes(UTF_16LE);
        String varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertFunction(
                "\"$varbinary_utf16_to_json\"(" + varbinaryLiteral + ", true)",
                JSON_2016,
                JSON_OBJECT);

        // wrong input encoding
        bytes = INPUT.getBytes(UTF_16BE);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertInvalidFunction(
                "\"$varbinary_utf16_to_json\"(" + varbinaryLiteral + ", true)",
                JSON_INPUT_CONVERSION_ERROR,
                "conversion to JSON failed: ");

        bytes = INPUT.getBytes(UTF_8);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertInvalidFunction(
                "\"$varbinary_utf16_to_json\"(" + varbinaryLiteral + ", true)",
                JSON_INPUT_CONVERSION_ERROR,
                "conversion to JSON failed: ");

        // wrong input encoding; conversion error suppressed and converted to JSON_ERROR
        assertFunction(
                "\"$varbinary_utf16_to_json\"(" + varbinaryLiteral + ", false)",
                JSON_2016,
                JSON_ERROR);

        // correct encoding, incorrect input
        bytes = ERROR_INPUT.getBytes(UTF_16LE);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        // with unsuppressed input conversion error
        assertInvalidFunction(
                "\"$varbinary_utf16_to_json\"(" + varbinaryLiteral + ", true)",
                JSON_INPUT_CONVERSION_ERROR,
                "conversion to JSON failed: ");

        // with input conversion error suppressed and converted to JSON_ERROR
        assertFunction(
                "\"$varbinary_utf16_to_json\"(" + varbinaryLiteral + ", false)",
                JSON_2016,
                JSON_ERROR);
    }

    @Test
    public void testVarbinaryUtf32ToJson()
    {
        byte[] bytes = INPUT.getBytes(Charset.forName("UTF-32LE"));
        String varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertFunction(
                "\"$varbinary_utf32_to_json\"(" + varbinaryLiteral + ", true)",
                JSON_2016,
                JSON_OBJECT);

        // wrong input encoding
        bytes = INPUT.getBytes(Charset.forName("UTF-32BE"));
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertInvalidFunction(
                "\"$varbinary_utf32_to_json\"(" + varbinaryLiteral + ", true)",
                JSON_INPUT_CONVERSION_ERROR,
                "conversion to JSON failed: ");

        bytes = INPUT.getBytes(UTF_8);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertInvalidFunction(
                "\"$varbinary_utf32_to_json\"(" + varbinaryLiteral + ", true)",
                JSON_INPUT_CONVERSION_ERROR,
                "conversion to JSON failed: ");

        // wrong input encoding; conversion error suppressed and converted to JSON_ERROR
        assertFunction(
                "\"$varbinary_utf32_to_json\"(" + varbinaryLiteral + ", false)",
                JSON_2016,
                JSON_ERROR);

        // correct encoding, incorrect input
        bytes = ERROR_INPUT.getBytes(Charset.forName("UTF-32LE"));
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        // with unsuppressed input conversion error
        assertInvalidFunction(
                "\"$varbinary_utf32_to_json\"(" + varbinaryLiteral + ", true)",
                JSON_INPUT_CONVERSION_ERROR,
                "conversion to JSON failed: ");

        // with input conversion error suppressed and converted to JSON_ERROR
        assertFunction(
                "\"$varbinary_utf32_to_json\"(" + varbinaryLiteral + ", false)",
                JSON_2016,
                JSON_ERROR);
    }

    @Test
    public void testNullInput()
    {
        assertFunction(
                "\"$varchar_to_json\"(null, true)",
                JSON_2016,
                null);
    }
}
