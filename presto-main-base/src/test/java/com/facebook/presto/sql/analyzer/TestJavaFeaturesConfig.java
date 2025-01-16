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
package com.facebook.presto.sql.analyzer;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestJavaFeaturesConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(JavaFeaturesConfig.class)
                .setAggregationSpillEnabled(true)
                .setDistinctAggregationSpillEnabled(true)
                .setDedupBasedDistinctAggregationSpillEnabled(false)
                .setDistinctAggregationLargeBlockSpillEnabled(false)
                .setDistinctAggregationLargeBlockSizeThreshold(DataSize.valueOf("50MB"))
                .setOrderByAggregationSpillEnabled(true)
                .setWindowSpillEnabled(true)
                .setOrderBySpillEnabled(true)
                .setTopNSpillEnabled(true)
                .setAggregationOperatorUnspillMemoryLimit(DataSize.valueOf("4MB"))
                .setTopNOperatorUnspillMemoryLimit(DataSize.valueOf("4MB")));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("experimental.aggregation-spill-enabled", "false")
                .put("experimental.distinct-aggregation-spill-enabled", "false")
                .put("experimental.dedup-based-distinct-aggregation-spill-enabled", "true")
                .put("experimental.distinct-aggregation-large-block-spill-enabled", "true")
                .put("experimental.distinct-aggregation-large-block-size-threshold", "10MB")
                .put("experimental.order-by-aggregation-spill-enabled", "false")
                .put("experimental.window-spill-enabled", "false")
                .put("experimental.order-by-spill-enabled", "false")
                .put("experimental.topn-spill-enabled", "false")
                .put("experimental.aggregation-operator-unspill-memory-limit", "100MB")
                .put("experimental.topn-operator-unspill-memory-limit", "100MB")
                .build();

        JavaFeaturesConfig expected = new JavaFeaturesConfig()
                .setAggregationSpillEnabled(false)
                .setDistinctAggregationSpillEnabled(false)
                .setDedupBasedDistinctAggregationSpillEnabled(true)
                .setDistinctAggregationLargeBlockSpillEnabled(true)
                .setDistinctAggregationLargeBlockSizeThreshold(DataSize.valueOf("10MB"))
                .setOrderByAggregationSpillEnabled(false)
                .setWindowSpillEnabled(false)
                .setOrderBySpillEnabled(false)
                .setTopNSpillEnabled(false)
                .setAggregationOperatorUnspillMemoryLimit(DataSize.valueOf("100MB"))
                .setTopNOperatorUnspillMemoryLimit(DataSize.valueOf("100MB"));
        assertFullMapping(properties, expected);
    }
}
