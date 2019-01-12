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
package io.prestosql.plugin.atop;

import io.airlift.json.JsonCodec;
import io.prestosql.spi.HostAddress;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.testng.Assert.assertEquals;

public class TestAtopSplit
{
    @Test
    public void testSerialization()
    {
        JsonCodec<AtopSplit> codec = JsonCodec.jsonCodec(AtopSplit.class);
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("+01:23"));
        AtopSplit split = new AtopSplit(AtopTable.DISKS, HostAddress.fromParts("localhost", 123), now.toEpochSecond(), now.getZone());
        AtopSplit decoded = codec.fromJson(codec.toJson(split));
        assertEquals(decoded.getTable(), split.getTable());
        assertEquals(decoded.getHost(), split.getHost());
        assertEquals(decoded.getDate(), split.getDate());
        assertEquals(decoded.getEpochSeconds(), split.getEpochSeconds());
        assertEquals(decoded.getTimeZone(), split.getTimeZone());
    }
}
