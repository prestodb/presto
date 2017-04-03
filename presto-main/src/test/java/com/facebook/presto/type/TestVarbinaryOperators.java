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
import com.facebook.presto.spi.type.SqlVarbinary;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;

public class TestVarbinaryOperators
        extends AbstractTestFunctions
{
    @Test
    public void testEqual()
            throws Exception
    {
        assertFunction("varbinary'37' = varbinary'37'", BOOLEAN, true);
        assertFunction("varbinary'37' = varbinary'17'", BOOLEAN, false);
    }

    @Test
    public void testNotEqual()
            throws Exception
    {
        assertFunction("varbinary'37' <> varbinary'37'", BOOLEAN, false);
        assertFunction("varbinary'37' <> varbinary'17'", BOOLEAN, true);
    }

    @Test
    public void testLessThan()
            throws Exception
    {
        assertFunction("varbinary'37' < varbinary'37'", BOOLEAN, false);
        assertFunction("varbinary'37' < varbinary'17'", BOOLEAN, false);
    }

    @Test
    public void testLessThanOrEqual()
            throws Exception
    {
        assertFunction("varbinary'37' <= varbinary'37'", BOOLEAN, true);
        assertFunction("varbinary'37' <= varbinary'17'", BOOLEAN, false);
    }

    @Test
    public void testGreaterThan()
            throws Exception
    {
        assertFunction("varbinary'37' > varbinary'37'", BOOLEAN, false);
        assertFunction("varbinary'37' > varbinary'17'", BOOLEAN, true);
    }

    @Test
    public void testGreaterThanOrEqual()
            throws Exception
    {
        assertFunction("varbinary'37' >= varbinary'37'", BOOLEAN, true);
        assertFunction("varbinary'37' >= varbinary'17'", BOOLEAN, true);
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertFunction("varbinary'37' BETWEEN varbinary'37' AND varbinary'37'", BOOLEAN, true);
        assertFunction("varbinary'37' BETWEEN varbinary'37' AND varbinary'17'", BOOLEAN, false);

        assertFunction("varbinary'37' BETWEEN varbinary'17' AND varbinary'37'", BOOLEAN, true);
        assertFunction("varbinary'37' BETWEEN varbinary'17' AND varbinary'17'", BOOLEAN, false);

        assertFunction("varbinary'17' BETWEEN varbinary'37' AND varbinary'37'", BOOLEAN, false);
        assertFunction("varbinary'17' BETWEEN varbinary'37' AND varbinary'17'", BOOLEAN, false);
        assertFunction("varbinary'17' BETWEEN varbinary'17' AND varbinary'37'", BOOLEAN, true);
    }

    @Test
    public void testCastFromVarchar()
            throws Exception
    {
        assertFunction("cast(cast('123' as varchar) as varbinary)", VARBINARY, new SqlVarbinary("123".getBytes()));
        assertFunction("cast(cast('abc' as varchar) as varbinary)", VARBINARY, new SqlVarbinary("abc".getBytes()));
    }

    @Test
    public void testCastFromChar()
            throws Exception
    {
        assertFunction("cast(cast('123' as char(3)) as varbinary)", VARBINARY, new SqlVarbinary("123".getBytes()));
        assertFunction("cast(cast('abc' as char(3)) as varbinary)", VARBINARY, new SqlVarbinary("abc".getBytes()));
        assertFunction("cast(cast('abc ' as char(4)) as varbinary)", VARBINARY, new SqlVarbinary("abc".getBytes()));
    }

    @Test
    public void testIsDistinctFrom()
            throws Exception
    {
        assertFunction("CAST(NULL AS varbinary) IS DISTINCT FROM CAST(NULL AS varbinary)", BOOLEAN, false);
        assertFunction("varbinary'37' IS DISTINCT FROM varbinary'37'", BOOLEAN, false);
        assertFunction("varbinary'37' IS DISTINCT FROM varbinary'38'", BOOLEAN, true);
        assertFunction("NULL IS DISTINCT FROM varbinary'37'", BOOLEAN, true);
        assertFunction("varbinary'37' IS DISTINCT FROM NULL", BOOLEAN, true);
    }
}
