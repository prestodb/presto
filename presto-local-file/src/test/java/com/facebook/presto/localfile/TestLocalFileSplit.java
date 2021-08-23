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
package com.facebook.presto.localfile;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.HostAddress;
import com.facebook.presto.common.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static org.testng.Assert.assertEquals;

public class TestLocalFileSplit
{
    private final HostAddress address = HostAddress.fromParts("localhost", 1234);
    private final LocalFileSplit split = new LocalFileSplit(address, LocalFileTables.HttpRequestLogTable.getSchemaTableName(), TupleDomain.all());

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<LocalFileSplit> codec = jsonCodec(LocalFileSplit.class);
        String json = codec.toJson(split);
        LocalFileSplit copy = codec.fromJson(json);

        assertEquals(copy.getAddress(), split.getAddress());
        assertEquals(copy.getTableName(), split.getTableName());
        assertEquals(copy.getEffectivePredicate(), split.getEffectivePredicate());

        assertEquals(copy.getAddresses(), ImmutableList.of(address));
        assertEquals(copy.getNodeSelectionStrategy(), HARD_AFFINITY);
    }
}
