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

public class TestLanceCommitTaskData
{
    @Test
    public void testJsonRoundTrip()
    {
        LanceCommitTaskData data = new LanceCommitTaskData(
                ImmutableList.of("frag1", "frag2"),
                1024L,
                100L);
        JsonCodec<LanceCommitTaskData> codec = jsonCodec(LanceCommitTaskData.class);
        String json = codec.toJson(data);
        LanceCommitTaskData copy = codec.fromJson(json);
        assertEquals(copy.getFragmentsJson(), ImmutableList.of("frag1", "frag2"));
        assertEquals(copy.getWrittenBytes(), 1024L);
        assertEquals(copy.getRowCount(), 100L);
    }
}
