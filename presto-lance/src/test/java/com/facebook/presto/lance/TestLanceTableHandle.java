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
package com.facebook.presto.lance;

import com.facebook.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLanceTableHandle
{
    private final LanceTableHandle tableHandle = new LanceTableHandle("default", "test_table");

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<LanceTableHandle> codec = jsonCodec(LanceTableHandle.class);
        String json = codec.toJson(tableHandle);
        LanceTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
        assertFalse(copy.getDatasetVersion().isPresent());
        assertFalse(copy.hasLimit());
    }

    @Test
    public void testJsonRoundTripWithVersion()
    {
        JsonCodec<LanceTableHandle> codec = jsonCodec(LanceTableHandle.class);
        LanceTableHandle handleWithVersion = new LanceTableHandle("default", "test_table", Optional.of(42L));
        String json = codec.toJson(handleWithVersion);
        LanceTableHandle copy = codec.fromJson(json);
        assertEquals(copy, handleWithVersion);
        assertTrue(copy.getDatasetVersion().isPresent());
        assertEquals(copy.getDatasetVersion().get(), Long.valueOf(42L));
    }

    @Test
    public void testDefaultHasNoLimit()
    {
        assertEquals(tableHandle.getLimit(), OptionalLong.empty());
        assertFalse(tableHandle.hasLimit());
    }

    @Test
    public void testWithLimit()
    {
        LanceTableHandle withLimit = tableHandle.withLimit(10);
        assertTrue(withLimit.hasLimit());
        assertEquals(withLimit.getLimit(), OptionalLong.of(10));
        assertFalse(tableHandle.hasLimit());
        assertEquals(withLimit.getSchemaName(), "default");
        assertEquals(withLimit.getTableName(), "test_table");
    }

    @Test
    public void testJsonRoundTripWithLimit()
    {
        JsonCodec<LanceTableHandle> codec = jsonCodec(LanceTableHandle.class);
        LanceTableHandle withLimit = tableHandle.withLimit(25);
        LanceTableHandle copy = codec.fromJson(codec.toJson(withLimit));
        assertEquals(copy, withLimit);
        assertEquals(copy.getLimit(), OptionalLong.of(25));
    }
}
