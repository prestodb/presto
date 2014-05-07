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
package com.facebook.presto.execution;

import com.facebook.presto.spi.SerializableNativeValue;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestSimpleMarker
{
    private static final JsonCodec<SimpleMarker> codec = JsonCodec.jsonCodec(SimpleMarker.class);

    @Test
    public void testRoundTrip()
    {
        SimpleMarker expected = new SimpleMarker(true, new SerializableNativeValue(Long.class, new Long(10)));
        assertEquals(codec.fromJson(codec.toJson(expected)), expected);

        expected = new SimpleMarker(false, new SerializableNativeValue(Double.class, new Double(10)));
        assertEquals(codec.fromJson(codec.toJson(expected)), expected);

        expected = new SimpleMarker(true, new SerializableNativeValue(Boolean.class, new Boolean(true)));
        assertEquals(codec.fromJson(codec.toJson(expected)), expected);

        expected = new SimpleMarker(true, new SerializableNativeValue(String.class, new String("123")));
        assertEquals(codec.fromJson(codec.toJson(expected)), expected);
    }
}
