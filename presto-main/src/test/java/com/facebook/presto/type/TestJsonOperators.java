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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.JsonType.JSON;

public class TestJsonOperators
        extends AbstractTestFunctions
{
    @Test
    public void testBigIntCasts()
    {
        assertFunction("cast(json_extract('{\"x\":999}', '$.x') as BIGINT)", BIGINT, 999);
        assertFunction("cast(cast(128 as JSON) as BIGINT)", BIGINT, 128);
        assertInvalidCast("cast(cast('{ \"x\" : 123}'as JSON) as BIGINT)");
        assertFunction("cast(128 as JSON)", JSON, "128");
    }

    @Test
    public void testDoubleCasts()
            throws Exception
    {
        assertFunction("cast(json_extract('{\"x\":1.23}', '$.x') as DOUBLE)", DOUBLE, 1.23);
        assertFunction("cast(cast(3.14 as JSON) as DOUBLE)", DOUBLE, 3.14);
        assertInvalidCast("cast(cast('{ \"x\" : 123}'as JSON) as DOUBLE)");
        assertFunction("cast(3.14 as JSON)", JSON, "3.14");
    }

    @Test
    public void testBooleanCasts()
    {
        assertFunction("cast(json_extract('{\"x\":true}', '$.x') as BOOLEAN)", BOOLEAN, true);
        assertFunction("cast(cast(TRUE as JSON) as VARCHAR)", VARCHAR, "true");
        assertInvalidCast("cast(cast('{ \"x\" : 123}'as JSON) as BOOLEAN)");
        assertFunction("cast(TRUE as JSON)", JSON, "true");
    }

    @Test
    public void testVarcharCasts()
    {
        assertFunction("cast(json_extract('{\"x\":\"y\"}', '$.x') as VARCHAR)", VARCHAR, "\"y\"");
        assertFunction("cast(cast('{\"x\":1, \"y\":null, \"z\":true, \"t\" : {\"a\" : \"b\"}}' as JSON) as VARCHAR)", VARCHAR, "{\"t\":{\"a\":\"b\"},\"x\":1,\"y\":null,\"z\":true}");
        assertInvalidCast("cast(cast('x' as JSON) as VARCHAR)");
    }
}
