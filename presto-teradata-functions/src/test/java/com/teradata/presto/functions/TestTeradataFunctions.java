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
package com.teradata.presto.functions;

import com.facebook.presto.spi.type.Type;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.facebook.presto.operator.scalar.FunctionAssertions;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestTeradataFunctions
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions().addScalarFunctions(TeradataFunctions.class);
    }

    @Test
    public void testIndex()
    {
        assertFunction("INDEX('high', 'ig')", BIGINT, 2);
        assertFunction("INDEX('high', 'igx')", BIGINT, 0);
        assertFunction("INDEX('Quadratically', 'a')", BIGINT, 3);
        assertFunction("INDEX('foobar', 'foobar')", BIGINT, 1);
        assertFunction("INDEX('foobar', 'obar')", BIGINT, 3);
        assertFunction("INDEX('zoo!', '!')", BIGINT, 4);
        assertFunction("INDEX('x', '')", BIGINT, 1);
        assertFunction("INDEX('', '')", BIGINT, 1);
        assertFunction("INDEX(NULL, '')", BIGINT, null);
        assertFunction("INDEX('', NULL)", BIGINT, null);
        assertFunction("INDEX(NULL, NULL)", BIGINT, null);
    }

    @Test
    public void testChar2HexInt()
    {
        assertFunction("CHAR2HEXINT('123')", VARCHAR, "003100320033");
        assertFunction("CHAR2HEXINT('One Ring')", VARCHAR, "004F006E0065002000520069006E0067");
    }

    @Test
    public void testChar2HexIntUtf8()
    {
        assertFunction("CHAR2HEXINT('ą')", VARCHAR, "0105");
        assertFunction("CHAR2HEXINT('ಠ')", VARCHAR, "0CA0");
        assertFunction("CHAR2HEXINT('ｱ')", VARCHAR, "FF71");
        assertFunction("CHAR2HEXINT('ಠ益ಠ')", VARCHAR, "0CA076CA0CA0");
        assertFunction("CHAR2HEXINT('(ノಠ益ಠ)ノ彡┻━┻')", VARCHAR, "002830CE0CA076CA0CA0002930CE5F61253B2501253B");
    }

    private void assertFunction(String projection, Type expectedType, Object expected)
    {
        functionAssertions.assertFunction(projection, expectedType, expected);
    }
}
