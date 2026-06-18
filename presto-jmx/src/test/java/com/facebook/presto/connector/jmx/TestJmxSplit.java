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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.connector.jmx.MetadataUtil.SPLIT_CODEC;
import static com.facebook.presto.connector.jmx.TestJmxTableHandle.TABLE;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class TestJmxSplit
{
    private static final ImmutableList<HostAddress> ADDRESSES = ImmutableList.of(HostAddress.fromString("test:1234"));
    private static final JmxSplit SPLIT = new JmxSplit(TABLE, ADDRESSES);

    @Test
    public void testSplit()
    {
        assertEquals(SPLIT.getTableHandle(), TABLE);
        assertEquals(SPLIT.getAddresses(), ADDRESSES);
        assertSame(SPLIT.getInfo(), SPLIT);
        assertEquals(SPLIT.getNodeSelectionStrategy(), HARD_AFFINITY);
    }

    @Test
    public void testJsonRoundTrip()
    {
        String json = SPLIT_CODEC.toJson(SPLIT);
        JmxSplit copy = SPLIT_CODEC.fromJson(json);

        assertEquals(copy.getTableHandle(), SPLIT.getTableHandle());
        assertEquals(copy.getAddresses(), SPLIT.getAddresses());
        assertSame(copy.getInfo(), copy);
        assertEquals(copy.getNodeSelectionStrategy(), HARD_AFFINITY);
    }
}
