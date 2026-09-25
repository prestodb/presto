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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static org.testng.Assert.assertEquals;

public class TestElasticsearchQueryBuilder
{
    // 2001-09-09T01:46:40Z
    private static final long EPOCH_MILLIS = 1_000_000_000_000L;

    @Test
    public void testTimestampValueNonLegacy()
    {
        ConnectorSession session = TestingConnectorSession.SESSION;
        String value = ElasticsearchQueryBuilder.getValue(session, TIMESTAMP, EPOCH_MILLIS).stringValue();
        // Non-legacy: millis are true UTC, formatted in UTC
        assertEquals(value, "2001-09-09T01:46:40");
    }

    @Test
    public void testTimestampValueLegacy()
    {
        ConnectorSession session = new TestingConnectorSession(
                "user",
                new ConnectorIdentity("user", Optional.empty(), Optional.empty()),
                Optional.of("test"),
                Optional.empty(),
                TimeZoneKey.getTimeZoneKey("America/Los_Angeles"),
                Locale.ENGLISH,
                System.currentTimeMillis(),
                ImmutableList.of(),
                ImmutableMap.of(),
                true,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                ImmutableMap.of());
        String value = ElasticsearchQueryBuilder.getValue(session, TIMESTAMP, EPOCH_MILLIS).stringValue();
        // Legacy: millis are interpreted in the session timezone (America/Los_Angeles, PDT = UTC-7)
        assertEquals(value, "2001-09-08T18:46:40");
    }
}
