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
package com.facebook.presto.delta;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

public class TestDeltaConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(DeltaConfig.class)
                .setMaxSplitsBatchSize(200)
                .setParquetDereferencePushdownEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("delta.max-splits-batch-size", "400")
                .put("delta.parquet-dereference-pushdown-enabled", "false")
                .build();

        DeltaConfig expected = new DeltaConfig()
                .setMaxSplitsBatchSize(400)
                .setParquetDereferencePushdownEnabled(false);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
