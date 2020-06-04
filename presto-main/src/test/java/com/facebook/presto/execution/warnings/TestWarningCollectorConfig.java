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
package com.facebook.presto.execution.warnings;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestWarningCollectorConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(WarningCollectorConfig.class)
                .setMaxWarnings(Integer.MAX_VALUE)
                .setWarningHandlingLevel(WarningHandlingLevel.NORMAL));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("warning-collector.max-warnings", "5")
                .put("warning-collector.warning-handling", "SUPPRESS")
                .build();

        WarningCollectorConfig expected = new WarningCollectorConfig()
                .setMaxWarnings(5)
                .setWarningHandlingLevel(WarningHandlingLevel.SUPPRESS);

        assertFullMapping(properties, expected);
    }
}
