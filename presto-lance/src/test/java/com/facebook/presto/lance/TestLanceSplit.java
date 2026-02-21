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
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestLanceSplit
{
    @Test
    public void testJsonRoundTrip()
    {
        LanceSplit split = new LanceSplit(ImmutableList.of(0, 1, 2));
        JsonCodec<LanceSplit> codec = jsonCodec(LanceSplit.class);
        String json = codec.toJson(split);
        LanceSplit copy = codec.fromJson(json);
        assertEquals(copy.getFragments(), ImmutableList.of(0, 1, 2));
    }
}
