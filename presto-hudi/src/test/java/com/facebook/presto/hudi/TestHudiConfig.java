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

package com.facebook.presto.hudi;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestHudiConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HudiConfig.class)
                .setMetadataTableEnabled(false)
                .setSizeBasedSplitWeightsEnabled(true)
                .setStandardSplitWeightSize(new DataSize(128, MEGABYTE))
                .setMinimumAssignedSplitWeight(0.05));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hudi.metadata-table-enabled", "true")
                .put("hudi.size-based-split-weights-enabled", "false")
                .put("hudi.standard-split-weight-size", "500MB")
                .put("hudi.minimum-assigned-split-weight", "0.1")
                .build();

        HudiConfig expected = new HudiConfig()
                .setMetadataTableEnabled(true)
                .setSizeBasedSplitWeightsEnabled(false)
                .setStandardSplitWeightSize(new DataSize(500, MEGABYTE))
                .setMinimumAssignedSplitWeight(0.1);

        assertFullMapping(properties, expected);
    }
}
