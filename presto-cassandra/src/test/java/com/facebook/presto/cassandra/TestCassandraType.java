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
package com.facebook.presto.cassandra;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCassandraType
{
    @Test
    public void testGetCassandraTypeForStringTypes()
    {
        // VARCHAR is an alias of TEXT in driver 4.x, so DataTypes.TEXT resolves to TEXT
        assertEquals(CassandraType.getCassandraType(DataTypes.TEXT), CassandraType.TEXT);
        assertEquals(CassandraType.getCassandraType(DataTypes.ASCII), CassandraType.ASCII);
    }

    @Test
    public void testJsonMapEncoding()
    {
        assertTrue(isValidJson(CassandraType.buildArrayValue(Lists.newArrayList("one", "two", "three\""), CassandraType.VARCHAR)));
        assertTrue(isValidJson(CassandraType.buildArrayValue(Lists.newArrayList(1, 2, 3), CassandraType.INT)));
        assertTrue(isValidJson(CassandraType.buildArrayValue(Lists.newArrayList(100000L, 200000000L, 3000000000L), CassandraType.BIGINT)));
        assertTrue(isValidJson(CassandraType.buildArrayValue(Lists.newArrayList(1.0, 2.0, 3.0), CassandraType.DOUBLE)));
        assertTrue(isValidJson(CassandraType.buildArrayValue(Lists.newArrayList((short) -32768, (short) 0, (short) 32767), CassandraType.SMALLINT)));
        assertTrue(isValidJson(CassandraType.buildArrayValue(Lists.newArrayList((byte) -128, (byte) 0, (byte) 127), CassandraType.TINYINT)));
        assertTrue(isValidJson(CassandraType.buildArrayValue(Lists.newArrayList("1970-01-01", "5555-06-15", "9999-12-31"), CassandraType.DATE)));
    }

    @Test
    public void testVectorTypeMapping()
    {
        // vector<float, 3> is exposed by the driver as a VectorType (which extends CustomType);
        // it must be classified as VECTOR, not CUSTOM.
        assertEquals(CassandraType.getCassandraType(DataTypes.vectorOf(DataTypes.FLOAT, 3)), CassandraType.VECTOR);
        assertEquals(CassandraType.VECTOR.getTypeArgumentSize(), 1);
    }

    @Test
    public void testVectorPrestoTypeIsArrayOfElement()
    {
        Type prestoType = CassandraType.getPrestoType(CassandraType.VECTOR, ImmutableList.of(CassandraType.FLOAT));
        assertTrue(prestoType instanceof ArrayType);
        assertEquals(((ArrayType) prestoType).getElementType(), RealType.REAL);
    }

    private static void continueWhileNotNull(JsonParser parser, JsonToken token)
            throws IOException
    {
        if (token != null) {
            continueWhileNotNull(parser, parser.nextToken());
        }
    }

    private static boolean isValidJson(String json)
    {
        boolean valid = false;
        try {
            JsonParser parser = new ObjectMapper().getFactory()
                    .createParser(json);
            continueWhileNotNull(parser, parser.nextToken());
            valid = true;
        }
        catch (IOException ignored) {
        }
        return valid;
    }
}
