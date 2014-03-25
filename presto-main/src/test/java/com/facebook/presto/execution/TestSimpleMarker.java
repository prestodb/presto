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

import com.facebook.presto.spi.Marker;
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
        Marker marker = new Marker(new SerializableNativeValue(Long.class, 10L), Marker.Bound.BELOW);
        SimpleMarker expected = SimpleMarker.fromMarker(marker);
        assertEquals(codec.fromJson(codec.toJson(expected)), expected);

        marker = new Marker(new SerializableNativeValue(Double.class, 11.12), Marker.Bound.BELOW);
        expected = SimpleMarker.fromMarker(marker);
        assertEquals(codec.fromJson(codec.toJson(expected)), expected);

        marker = new Marker(new SerializableNativeValue(String.class, "23rsdg"), Marker.Bound.BELOW);
        expected = SimpleMarker.fromMarker(marker);
        assertEquals(codec.fromJson(codec.toJson(expected)), expected);

        marker = new Marker(new SerializableNativeValue(Boolean.class, true), Marker.Bound.BELOW);
        expected = SimpleMarker.fromMarker(marker);
        assertEquals(codec.fromJson(codec.toJson(expected)), expected);
    }
}
