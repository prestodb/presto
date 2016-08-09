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

import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

public class TestCharacterStringCasts
        extends AbstractTestFunctions
{
    @Test
    public void testVarcharToVarcharCast()
            throws Exception
    {
        assertFunction("cast('bar' as varchar(20))", createVarcharType(20), "bar");
        assertFunction("cast(cast('bar' as varchar(20)) as varchar(30))", createVarcharType(30), "bar");
        assertFunction("cast(cast('bar' as varchar(20)) as varchar)", VARCHAR, "bar");

        assertFunction("cast('banana' as varchar(3))", createVarcharType(3), "ban");
        assertFunction("cast(cast('banana' as varchar(20)) as varchar(3))", createVarcharType(3), "ban");
    }

    @Test
    public void testVarcharToCharCast()
    {
        assertFunction("cast('bar  ' as char(10))", createCharType(10), "bar       ");
        assertFunction("cast('bar' as char)", createCharType(1), "b");
        assertFunction("cast('   ' as char)", createCharType(1), " ");
    }

    @Test
    public void testCharToVarcharCast()
            throws Exception
    {
        assertFunction("cast(cast('bar' as char(5)) as varchar(10))", createVarcharType(10), "bar       ");
        assertFunction("cast(cast('bar' as char(5)) as varchar(1))", createVarcharType(1), "b");
        assertFunction("cast(cast('bar' as char(3)) as varchar(3))", createVarcharType(3), "bar");
    }
}
