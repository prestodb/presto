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
package com.facebook.plugin.arrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class ArrowTableHandleTest
{
    @Test
    public void testConstructorAndGetters()
    {
        ArrowTableHandle handle = new ArrowTableHandle("test_schema", "test_table");
        assertEquals(handle.getSchema(), "test_schema");
        assertEquals(handle.getTable(), "test_table");
    }

    @Test
    public void testToString()
    {
        ArrowTableHandle handle = new ArrowTableHandle("test_schema", "test_table");
        assertEquals(handle.toString(), "test_schema:test_table");
    }

    @Test
    public void testJsonSerialization() throws Exception
    {
        ObjectMapper objectMapper = new ObjectMapper();

        ArrowTableHandle handle = new ArrowTableHandle("test_schema", "test_table");
        String json = objectMapper.writeValueAsString(handle);

        assertNotNull(json);

        ArrowTableHandle deserialized = objectMapper.readValue(json, ArrowTableHandle.class);
        assertEquals(deserialized.getSchema(), handle.getSchema());
        assertEquals(deserialized.getTable(), handle.getTable());
    }
}
