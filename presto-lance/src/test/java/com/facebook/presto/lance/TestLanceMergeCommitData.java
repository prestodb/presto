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

public class TestLanceMergeCommitData
{
    @Test
    public void testJsonRoundTrip()
    {
        LanceMergeCommitData data = new LanceMergeCommitData(
                ImmutableList.of(
                        new FragmentDeletion(0, ImmutableList.of(1, 2, 5)),
                        new FragmentDeletion(3, ImmutableList.of(7))),
                "[{\"id\":4}]",
                42L,
                2048L,
                4L);
        JsonCodec<LanceMergeCommitData> codec = jsonCodec(LanceMergeCommitData.class);
        LanceMergeCommitData copy = codec.fromJson(codec.toJson(data));

        assertEquals(copy.getDeletions().size(), 2);
        assertEquals(copy.getDeletions().get(0).getFragmentId(), 0);
        assertEquals(copy.getDeletions().get(0).getRowIndexes(), ImmutableList.of(1, 2, 5));
        assertEquals(copy.getDeletions().get(1).getFragmentId(), 3);
        assertEquals(copy.getDeletions().get(1).getRowIndexes(), ImmutableList.of(7));
        assertEquals(copy.getNewFragmentsJson(), "[{\"id\":4}]");
        assertEquals(copy.getReadVersion(), 42L);
        assertEquals(copy.getWrittenBytes(), 2048L);
        assertEquals(copy.getRowCount(), 4L);
    }

    @Test
    public void testEmptyDeletions()
    {
        LanceMergeCommitData data = new LanceMergeCommitData(
                ImmutableList.of(),
                "[]",
                1L,
                0L,
                0L);
        JsonCodec<LanceMergeCommitData> codec = jsonCodec(LanceMergeCommitData.class);
        LanceMergeCommitData copy = codec.fromJson(codec.toJson(data));

        assertEquals(copy.getDeletions().size(), 0);
        assertEquals(copy.getNewFragmentsJson(), "[]");
        assertEquals(copy.getReadVersion(), 1L);
    }
}
