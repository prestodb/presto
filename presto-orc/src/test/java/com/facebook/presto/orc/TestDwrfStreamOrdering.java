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

import com.facebook.presto.orc.proto.DwrfProto;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestDwrfStreamOrdering
{
    @Test
    public void testDwrfStreamOrder()
    {
        ImmutableMap<Integer, List<DwrfProto.KeyInfo>> columnIdToFlatMapKeyIds =
                ImmutableMap.of(
                        1, Collections.singletonList(DwrfProto.KeyInfo.newBuilder().setIntKey(1).build()),
                        2, Collections.singletonList(DwrfProto.KeyInfo.newBuilder().setIntKey(1).build()),
                        3, Collections.singletonList(DwrfProto.KeyInfo.newBuilder().setIntKey(1).build()));
        DwrfStreamOrderingConfig config = new DwrfStreamOrderingConfig(columnIdToFlatMapKeyIds);
        assertEquals(config.getStreamOrdering(), columnIdToFlatMapKeyIds);
    }
}
