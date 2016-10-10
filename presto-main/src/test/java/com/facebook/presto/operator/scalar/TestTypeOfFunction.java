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

import com.facebook.presto.spi.type.VarcharType;
import org.testng.annotations.Test;

public class TestTypeOfFunction
        extends AbstractTestFunctions
{
    @Test
    public void testSimpleType()
            throws Exception
    {
        assertFunction("typeof(CAST(1 AS BIGINT))", VarcharType.VARCHAR, "bigint");
        assertFunction("typeof(CAST(1 AS INTEGER))", VarcharType.VARCHAR, "integer");
        assertFunction("typeof(CAST(1 AS VARCHAR))", VarcharType.VARCHAR, "varchar");
        assertFunction("typeof(CAST(1 AS DOUBLE))", VarcharType.VARCHAR, "double");
        assertFunction("typeof(123)", VarcharType.VARCHAR, "integer");
        assertFunction("typeof('cat')", VarcharType.VARCHAR, "varchar(3)");
        assertFunction("typeof(NULL)", VarcharType.VARCHAR, "unknown");
    }

    @Test
    public void testParametricType()
            throws Exception
    {
        assertFunction("typeof(CAST(NULL AS VARCHAR(10)))", VarcharType.VARCHAR, "varchar(10)");
        assertFunction("typeof(CAST(NULL AS DECIMAL(5,1)))", VarcharType.VARCHAR, "decimal(5,1)");
        assertFunction("typeof(CAST(NULL AS DECIMAL(1)))", VarcharType.VARCHAR, "decimal(1,0)");
        assertFunction("typeof(CAST(NULL AS DECIMAL))", VarcharType.VARCHAR, "decimal(38,0)");
        assertFunction("typeof(CAST(NULL AS ARRAY(INTEGER)))", VarcharType.VARCHAR, "array(integer)");
        assertFunction("typeof(CAST(NULL AS ARRAY(DECIMAL(5,1))))", VarcharType.VARCHAR, "array(decimal(5,1))");
    }

    @Test
    public void testNestedType()
            throws Exception
    {
        assertFunction("typeof(CAST(NULL AS ARRAY(ARRAY(ARRAY(INTEGER)))))", VarcharType.VARCHAR, "array(array(array(integer)))");
        assertFunction("typeof(CAST(NULL AS ARRAY(ARRAY(ARRAY(DECIMAL(5,1))))))", VarcharType.VARCHAR, "array(array(array(decimal(5,1))))");
    }

    @Test
    public void testComplex()
            throws Exception
    {
        assertFunction("typeof(CONCAT('ala','ma','kota'))", VarcharType.VARCHAR, "varchar");
        assertFunction("typeof(CONCAT(CONCAT('ala','ma','kota'), 'baz'))", VarcharType.VARCHAR, "varchar");
        assertFunction("typeof(ARRAY [CAST(1 AS INTEGER),CAST(2 AS INTEGER),CAST(3 AS INTEGER)])", VarcharType.VARCHAR, "array(integer)");
        assertFunction("typeof(sin(2))", VarcharType.VARCHAR, "double");
        assertFunction("typeof(2+sin(2)+2.3)", VarcharType.VARCHAR, "double");
    }
}
