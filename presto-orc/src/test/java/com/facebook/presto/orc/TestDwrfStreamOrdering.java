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

package com.facebook.presto.orc;

import com.facebook.presto.orc.proto.DwrfProto.KeyInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class TestDwrfStreamOrdering
{
    @Test
    public void testDwrfStreamOrder()
    {
        ImmutableMap<Integer, List<KeyInfo>> columnIdToFlatMapKeyIds =
                ImmutableMap.of(
                        1, ImmutableList.of(KeyInfo.newBuilder().setIntKey(1).build(), KeyInfo.newBuilder().setIntKey(2).build()),
                        2, ImmutableList.of(KeyInfo.newBuilder().setIntKey(3).build()),
                        3, ImmutableList.of(KeyInfo.newBuilder().setIntKey(5).build(), KeyInfo.newBuilder().setIntKey(4).build()));

        ImmutableMap<Integer, Set<KeyInfo>> expectedResult =
                ImmutableMap.of(
                        1, ImmutableSet.of(KeyInfo.newBuilder().setIntKey(1).build(), KeyInfo.newBuilder().setIntKey(2).build()),
                        2, ImmutableSet.of(KeyInfo.newBuilder().setIntKey(3).build()),
                        3, ImmutableSet.of(KeyInfo.newBuilder().setIntKey(5).build(), KeyInfo.newBuilder().setIntKey(4).build()));

        DwrfStreamOrderingConfig config = new DwrfStreamOrderingConfig(columnIdToFlatMapKeyIds);
        assertEquals(expectedResult, config.getStreamOrdering());
    }
}
