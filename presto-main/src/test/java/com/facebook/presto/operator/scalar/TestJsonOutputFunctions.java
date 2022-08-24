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

import com.facebook.presto.common.type.SqlVarbinary;
import org.testng.annotations.Test;

import java.nio.charset.Charset;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TestJsonOutputFunctions
        extends AbstractTestFunctions
{
    private static final String JSON_EXPRESSION = "\"$varchar_to_json\"('{\"key1\" : 1e0, \"key2\" : true, \"key3\" : null}', true)";
    private static final String OUTPUT = "{\"key1\":1.0,\"key2\":true,\"key3\":null}";

    @Test
    public void testJsonToVarchar()
    {
        assertFunction(
                "\"$json_to_varchar\"(" + JSON_EXPRESSION + ", TINYINT '1', true)",
                VARCHAR,
                OUTPUT);
    }

    @Test
    public void testJsonToVarbinaryUtf8()
    {
        byte[] bytes = OUTPUT.getBytes(UTF_8);
        SqlVarbinary varbinaryOutput = new SqlVarbinary(bytes);

        assertFunction(
                "\"$json_to_varbinary\"(" + JSON_EXPRESSION + ", TINYINT '1', true)",
                VARBINARY,
                varbinaryOutput);

        assertFunction(
                "\"$json_to_varbinary_utf8\"(" + JSON_EXPRESSION + ", TINYINT '1', true)",
                VARBINARY,
                varbinaryOutput);
    }

    @Test
    public void testJsonToVarbinaryUtf16()
    {
        byte[] bytes = OUTPUT.getBytes(UTF_16LE);
        SqlVarbinary varbinaryOutput = new SqlVarbinary(bytes);

        assertFunction(
                "\"$json_to_varbinary_utf16\"(" + JSON_EXPRESSION + ", TINYINT '1', true)",
                VARBINARY,
                varbinaryOutput);
    }

    @Test
    public void testJsonToVarbinaryUtf32()
    {
        byte[] bytes = OUTPUT.getBytes(Charset.forName("UTF-32LE"));
        SqlVarbinary varbinaryOutput = new SqlVarbinary(bytes);

        assertFunction(
                "\"$json_to_varbinary_utf32\"(" + JSON_EXPRESSION + ", TINYINT '1', true)",
                VARBINARY,
                varbinaryOutput);
    }

    @Test
    public void testQuotesBehavior()
    {
        String jsonScalarString = "\"$varchar_to_json\"('\"some_text\"', true)";

        // keep quotes on scalar string
        assertFunction(
                "\"$json_to_varchar\"(" + jsonScalarString + ", TINYINT '1', false)",
                VARCHAR,
                "\"some_text\"");

        // omit quotes on scalar string
        assertFunction(
                "\"$json_to_varchar\"(" + jsonScalarString + ", TINYINT '1', true)",
                VARCHAR,
                "some_text");

        // quotes behavior does not apply to nested string. the quotes are preserved
        assertFunction(
                "\"$json_to_varchar\"(\"$varchar_to_json\"('[\"some_text\"]', true), TINYINT '1', true)",
                VARCHAR,
                "[\"some_text\"]");
    }

    @Test
    public void testNullInput()
    {
        assertFunction(
                "\"$json_to_varchar\"(null, TINYINT '1', true)",
                VARCHAR,
                null);
    }
}
