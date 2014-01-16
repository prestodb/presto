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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestSimpleDomain
{
    private static final JsonCodec<SimpleDomain> codec = JsonCodec.jsonCodec(SimpleDomain.class);

    @Test
    public void testRoundTrip()
    {
        SimpleMarker low = new SimpleMarker(true, new SerializableNativeValue(Long.class, new Long(10)));
        SimpleMarker high = new SimpleMarker(false, new SerializableNativeValue(Long.class, new Long(100)));
        List<SimpleRange> ranges = ImmutableList.of(new SimpleRange(Optional.fromNullable(low), Optional.fromNullable(high)));
        SimpleDomain expected = new SimpleDomain(true, Optional.fromNullable(ranges));

        String json = codec.toJson(expected);
        SimpleDomain actual = codec.fromJson(json);

        assertEquals(actual, expected);
    }
}
