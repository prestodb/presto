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
package com.facebook.presto.spi.session;

import com.facebook.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestSessionPropertyMetadata
{
    public static final JsonCodec<SessionPropertyMetadata> CODEC = JsonCodec.jsonCodec(SessionPropertyMetadata.class);

    @Test
    public void testBool()
    {
        String jsonString = "{\n  \"name\" : \"test\",\n  \"description\" : \"test description\",\n  \"typeSignature\" : \"boolean\",\n  \"defaultValue\" : \"false\",\n  \"hidden\" : false\n}";
        SessionPropertyMetadata actual = new SessionPropertyMetadata("test", "test description", BOOLEAN.getTypeSignature(), "false", false);
        SessionPropertyMetadata expected = CODEC.fromJson(CODEC.toJson(actual));
        assertEquals(expected, actual);
        assertEquals(CODEC.toJson(actual), jsonString);
    }

    @Test
    public void testInteger()
    {
        String jsonString = "{\n  \"name\" : \"test\",\n  \"description\" : \"test description\",\n  \"typeSignature\" : \"integer\",\n  \"defaultValue\" : \"0\",\n  \"hidden\" : false\n}";
        SessionPropertyMetadata actual = new SessionPropertyMetadata("test", "test description", INTEGER.getTypeSignature(), "0", false);
        SessionPropertyMetadata expected = CODEC.fromJson(CODEC.toJson(actual));
        assertEquals(expected, actual);
        assertEquals(CODEC.toJson(actual), jsonString);
    }

    @Test
    public void testLong()
    {
        String jsonString = "{\n  \"name\" : \"test\",\n  \"description\" : \"test description\",\n  \"typeSignature\" : \"bigint\",\n  \"defaultValue\" : \"987654321012345678\",\n  \"hidden\" : false\n}";
        SessionPropertyMetadata actual = new SessionPropertyMetadata("test", "test description", BIGINT.getTypeSignature(), "987654321012345678", false);
        SessionPropertyMetadata expected = CODEC.fromJson(CODEC.toJson(actual));
        assertEquals(expected, actual);
        assertEquals(CODEC.toJson(actual), jsonString);
    }

    @Test
    public void testDouble()
    {
        String jsonString = "{\n  \"name\" : \"test\",\n  \"description\" : \"test description\",\n  \"typeSignature\" : \"double\",\n  \"defaultValue\" : \"0.0\",\n  \"hidden\" : false\n}";
        SessionPropertyMetadata actual = new SessionPropertyMetadata("test", "test description", DOUBLE.getTypeSignature(), "0.0", false);
        SessionPropertyMetadata expected = CODEC.fromJson(CODEC.toJson(actual));
        assertEquals(expected, actual);
        assertEquals(CODEC.toJson(actual), jsonString);
    }

    @Test
    public void testString()
    {
        String jsonString = "{\n  \"name\" : \"test\",\n  \"description\" : \"test description\",\n  \"typeSignature\" : \"varchar\",\n  \"defaultValue\" : \"defaultValue\",\n  \"hidden\" : false\n}";
        SessionPropertyMetadata actual = new SessionPropertyMetadata("test", "test description", VARCHAR.getTypeSignature(), "defaultValue", false);
        SessionPropertyMetadata expected = CODEC.fromJson(CODEC.toJson(actual));
        assertEquals(expected, actual);
        assertEquals(CODEC.toJson(actual), jsonString);
    }

    @Test
    public void testDataSize()
    {
        String jsonString = "{\n  \"name\" : \"test\",\n  \"description\" : \"test description\",\n  \"typeSignature\" : \"varchar\",\n  \"defaultValue\" : \"1MB\",\n  \"hidden\" : false\n}";
        SessionPropertyMetadata actual = new SessionPropertyMetadata("test", "test description", VARCHAR.getTypeSignature(), "1MB", false);
        SessionPropertyMetadata expected = CODEC.fromJson(CODEC.toJson(actual));
        assertEquals(expected, actual);
        assertEquals(CODEC.toJson(actual), jsonString);
    }
}
