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
package com.facebook.presto.raptor.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestMetadataConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(MetadataConfig.class)
                .setStartupGracePeriod(new Duration(5, MINUTES))
                .setReassignmentDelay(new Duration(0, MINUTES))
                .setReassignmentInterval(new Duration(0, MINUTES))
                .setMinimumNodeCount(0));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("raptor.startup-grace-period", "42m")
                .put("raptor.reassignment-delay", "6m")
                .put("raptor.reassignment-interval", "7m")
                .put("raptor.minimum-node-count", "39")
                .build();

        MetadataConfig expected = new MetadataConfig()
                .setStartupGracePeriod(new Duration(42, MINUTES))
                .setReassignmentDelay(new Duration(6, MINUTES))
                .setReassignmentInterval(new Duration(7, MINUTES))
                .setMinimumNodeCount(39);

        assertFullMapping(properties, expected);
    }
}
