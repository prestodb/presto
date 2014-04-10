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
package com.facebook.presto.split;

import com.facebook.presto.connector.system.SystemSplit;
import com.facebook.presto.connector.system.SystemTableHandle;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestInternalSplit
{
    @Test
    public void testSerialization()
            throws Exception
    {
        SystemTableHandle tableHandle = new SystemTableHandle("xyz", "foo");
        SystemSplit expected = new SystemSplit(tableHandle, ImmutableList.of(HostAddress.fromParts("127.0.0.1", 0)));

        JsonCodec<SystemSplit> codec = jsonCodec(SystemSplit.class);
        SystemSplit actual = codec.fromJson(codec.toJson(expected));

        assertEquals(actual.getTableHandle(), expected.getTableHandle());
        assertEquals(actual.getAddresses(), expected.getAddresses());
    }
}
