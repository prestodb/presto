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
package com.facebook.presto.spark;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamWriteConstraints;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPrestoSparkServiceFactory
{
    private static final int LONG_NAME_LENGTH = 60_000;
    private static final int NESTING_DEPTH = 1_500;

    private StreamReadConstraints originalReadConstraints;
    private StreamWriteConstraints originalWriteConstraints;

    @BeforeMethod
    public void setUp()
    {
        // Capture the JVM-global Jackson defaults, then start each test from the stock limits.
        originalReadConstraints = StreamReadConstraints.defaults();
        originalWriteConstraints = StreamWriteConstraints.defaults();
        StreamReadConstraints.overrideDefaultStreamReadConstraints(StreamReadConstraints.builder().build());
        StreamWriteConstraints.overrideDefaultStreamWriteConstraints(StreamWriteConstraints.builder().build());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        StreamReadConstraints.overrideDefaultStreamReadConstraints(originalReadConstraints);
        StreamWriteConstraints.overrideDefaultStreamWriteConstraints(originalWriteConstraints);
    }

    @Test
    public void testOverrideRelaxesJsonPropertyNameLimit()
    {
        Map<String, String> value = ImmutableMap.of(Strings.repeat("a", LONG_NAME_LENGTH), "v");
        PrestoSparkServiceFactory.overrideJacksonStreamConstraints();

        JsonCodec<Map<String, String>> codec = new JsonCodecFactory().mapJsonCodec(String.class, String.class);
        assertEquals(codec.fromJson(codec.toJsonBytes(value)), value);
    }

    @Test
    public void testOverrideRelaxesJsonNestingDepthLimit()
    {
        Object nested = "v";
        for (int i = 0; i < NESTING_DEPTH; i++) {
            nested = Collections.singletonList(nested);
        }
        PrestoSparkServiceFactory.overrideJacksonStreamConstraints();

        JsonCodec<Object> codec = new JsonCodecFactory().jsonCodec(Object.class);
        assertTrue(codec.toJsonBytes(nested).length > 0);
    }
}
