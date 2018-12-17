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
package com.facebook.presto.raptorx.storage;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestLocalCleanerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LocalCleanerConfig.class)
                .setLocalCleanerInterval(new Duration(1, HOURS))
                .setLocalCleanTime(new Duration(4, HOURS))
                .setThreads(2));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("raptor.local-cleaner-interval", "31m")
                .put("raptor.local-clean-time", "32m")
                .put("raptor.local-clean-threads", "32")
                .build();

        LocalCleanerConfig expected = new LocalCleanerConfig()
                .setLocalCleanerInterval(new Duration(31, MINUTES))
                .setLocalCleanTime(new Duration(32, MINUTES))
                .setThreads(32);

        assertFullMapping(properties, expected);
    }
}
