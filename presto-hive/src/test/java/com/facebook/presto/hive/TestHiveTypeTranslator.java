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
package com.facebook.presto.hive;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.testing.Assertions.assertContains;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestHiveTypeTranslator
{
    private final Map<String, HiveType> typeTranslationMap;

    public TestHiveTypeTranslator()
    {
        typeTranslationMap = ImmutableMap.<String, HiveType>builder()
                .put("bigint", HiveType.HIVE_LONG)
                .put("integer", HiveType.HIVE_INT)
                .put("smallint", HiveType.HIVE_SHORT)
                .put("tinyint", HiveType.HIVE_BYTE)
                .put("double", HiveType.HIVE_DOUBLE)
                .put("varchar", HiveType.HIVE_STRING)
                .put("date", HiveType.HIVE_DATE)
                .put("timestamp", HiveType.HIVE_TIMESTAMP)
                .put("decimal(5,3)", HiveType.valueOf("decimal(5,3)"))
                .put("varbinary", HiveType.HIVE_BINARY)
                .put("array(timestamp)", HiveType.valueOf("array<timestamp>"))
                .put("map(boolean,varbinary)", HiveType.valueOf("map<boolean,binary>"))
                .put("row(col0 integer,col1 varbinary)", HiveType.valueOf("struct<col0:int,col1:binary>"))
                .build();
    }

    @Test
    public void testTypeTranslator()
    {
        for (boolean useUnboundedVarchar : new boolean[] {true, false}) {
            assertTypeTranslation(typeTranslationMap, useUnboundedVarchar);

            assertInvalidTypeTranslation("row(integer,varbinary)", NOT_SUPPORTED.toErrorCode(), "Anonymous row type is not supported in Hive. Please give each field a name: row(integer,varbinary)", useUnboundedVarchar);
        }

        // verify bounded varchars.
        Map<String, HiveType> boundedTypes = ImmutableMap.of(
                "varchar(3)",
                HiveType.valueOf("varchar(3)"),
                "array(varchar(23))",
                HiveType.valueOf("array<varchar(23)>"),
                "map(varchar(10), varchar(37))",
                HiveType.valueOf("map<varchar(10),varchar(37)>"),
                "row(col0 array(varchar(1)),col1 map(varchar(2), bigint))",
                HiveType.valueOf("struct<col0:array<varchar(1)>,col1:map<varchar(2),bigint>>"));
        assertTypeTranslation(boundedTypes, false);

        // verify UnBounded types
        Map<String, HiveType> unboundedTypes = ImmutableMap.of(
                "varchar(3)",
                HiveType.HIVE_STRING,
                "array(varchar(23))",
                HiveType.valueOf("array<string>"),
                "map(varchar(10), varchar(37))",
                HiveType.valueOf("map<string,string>"),
                "row(col0 array(varchar(1)),col1 map(varchar(2), bigint))",
                HiveType.valueOf("struct<col0:array<string>,col1:map<string,bigint>>"));

        assertTypeTranslation(unboundedTypes, true);
    }

    private void assertTypeTranslation(Map<String, HiveType> typeTranslationMap, boolean useUnboundedVarchar)
    {
        for (Map.Entry<String, HiveType> entry : typeTranslationMap.entrySet()) {
            assertTypeTranslation(entry.getKey(), entry.getValue(), useUnboundedVarchar);
        }
    }

    private void assertTypeTranslation(String typeName, HiveType hiveType, boolean useUnboundedVarchar)
    {
        Type type = FUNCTION_AND_TYPE_MANAGER.getType(parseTypeSignature(typeName));
        assertEquals(HiveType.toHiveType(type, useUnboundedVarchar), hiveType, "Type does not match for " + useUnboundedVarchar);
    }

    private void assertInvalidTypeTranslation(String typeName, ErrorCode errorCode, String message, boolean useUnboundedVarchar)
    {
        Type type = FUNCTION_AND_TYPE_MANAGER.getType(parseTypeSignature(typeName));
        try {
            HiveType.toHiveType(type, useUnboundedVarchar);
            fail("expected exception");
        }
        catch (PrestoException e) {
            try {
                assertEquals(e.getErrorCode(), errorCode);
                assertContains(e.getMessage(), message);
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }
    }
}
