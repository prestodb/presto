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
package com.facebook.presto.maxcompute.util;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestDataTypes
{
    private static final TypeManager TYPE_MANAGER = createTestFunctionAndTypeManager();

    @Test
    public void testIntType()
    {
        Type anInt = DataTypes.convertToPrestoType("INT", null);

        assertEquals(anInt, IntegerType.INTEGER);
    }

    @Test
    public void testVarchar()
    {
        Type type = DataTypes.convertToPrestoType("CHAR", null);
        assertEquals(type, VarcharType.VARCHAR);

        type = DataTypes.convertToPrestoType("CHAR(34)", null);
        assertEquals(type, VarcharType.createVarcharType(34));

        type = DataTypes.convertToPrestoType("VARCHAR", null);
        assertEquals(type, VarcharType.VARCHAR);

        type = DataTypes.convertToPrestoType("VARCHAR(34)", null);
        assertEquals(type, VarcharType.createVarcharType(34));

        type = DataTypes.convertToPrestoType("STRING", null);
        assertEquals(type, VarcharType.VARCHAR);
    }

    @Test
    public void testArray()
    {
        Type type = DataTypes.convertToPrestoType("ARRAY<BOOLEAN>", null);
        assertEquals(type, new ArrayType(BooleanType.BOOLEAN));

        type = DataTypes.convertToPrestoType("ARRAY<ARRAY<INT>>", null);
        assertEquals(type, new ArrayType(new ArrayType(IntegerType.INTEGER)));

        type = DataTypes.convertToPrestoType("ARRAY<MAP<STRING, INT>>", TYPE_MANAGER);
        assertEquals(type.toString(), "array(map(varchar,integer))");

        type = DataTypes.convertToPrestoType("ARRAY<STRUCT<a:TINYINT, B:DATE,C:ARRAY<VARCHAR(34)>>>", TYPE_MANAGER);
        assertEquals(type.toString(), "array(row(a tinyint,b date,c array(varchar(34))))");
    }

    @Test
    public void testMap()
    {
        Type type = DataTypes.convertToPrestoType("map<BOOLEAN, timestamp>", TYPE_MANAGER);
        assertEquals(type.toString(), "map(boolean,timestamp)");

        type = DataTypes.convertToPrestoType("map<BOOLEAN, ARRAY<smallint>>", TYPE_MANAGER);
        assertEquals(type.toString(), "map(boolean,array(smallint))");

        type = DataTypes.convertToPrestoType("map<BOOLEAN, struct<a:smallint, xxxxxx:Array<Map<int, char(345)>>>>", TYPE_MANAGER);
        assertEquals(type.toString(), "map(boolean,row(a smallint,xxxxxx array(map(integer,varchar(345)))))");
    }

    @Test
    public void testRow()
    {
        Type type = DataTypes.convertToPrestoType("struct<aaaa:Array<Map<int, datetime>>, vbb: struct<a:double, b:json>>", TYPE_MANAGER);
        assertEquals(type.getDisplayName(), "row(\"aaaa\" array(map(integer, timestamp)), \"vbb\" row(\"a\" double, \"b\" varchar))");
    }

    @Test
    public void notSupport()
    {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataTypes.convertToPrestoType("notSupport", TYPE_MANAGER));
        assertTrue(e.getMessage().contains("Can't convert to MPP type, columnType=notSupport"));
    }
}
