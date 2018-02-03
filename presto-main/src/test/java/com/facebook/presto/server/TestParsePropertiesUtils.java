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
package com.facebook.presto.server;

import com.facebook.presto.server.ParsePropertiesUtils.SystemAndCatalogProperties;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import javax.ws.rs.WebApplicationException;

import java.util.Map;

import static com.facebook.presto.server.ParsePropertiesUtils.parseSessionProperties;
import static org.testng.Assert.assertEquals;

public class TestParsePropertiesUtils
{
    @Test
    public void testParseSessionPropertiesEmpty()
    {
        SystemAndCatalogProperties actual = parseSessionProperties(ImmutableMap.of(), "error");
        assertEquals(actual.getSystemProperties(), ImmutableMap.of());
        assertEquals(actual.getCatalogProperties(), ImmutableMap.of());
    }

    @Test
    public void testParseSessionPropertiesNotEmpty()
    {
        Map<String, String> input = ImmutableMap.of(
                "key1", "value1",
                "catalog1.key2", "value2",
                "key3", "value3",
                "catalog2.key4", "value4");
        SystemAndCatalogProperties actual = parseSessionProperties(input, "error");
        assertEquals(actual.getSystemProperties(), ImmutableMap.of(
                "key1", "value1",
                "key3", "value3"));
        assertEquals(actual.getCatalogProperties(), ImmutableMap.of(
                "catalog1", ImmutableMap.of("key2", "value2"),
                "catalog2", ImmutableMap.of("key4", "value4")));
    }

    @Test(expectedExceptions = WebApplicationException.class)
    public void testParseSessionPropertiesInvalid()
    {
        Map<String, String> input = ImmutableMap.of("first.second.third", "value");
        parseSessionProperties(input, "error");
    }
}
