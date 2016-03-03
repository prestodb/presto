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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.sql.analyzer.FeaturesConfig.FILE_BASED_RESOURCE_GROUP_MANAGER;
import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestFeaturesConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(FeaturesConfig.class)
                .setExperimentalSyntaxEnabled(false)
                .setDistributedIndexJoinsEnabled(false)
                .setDistributedJoinsEnabled(true)
                .setRedistributeWrites(true)
                .setOptimizeMetadataQueries(false)
                .setOptimizeHashGeneration(true)
                .setOptimizeSingleDistinct(true)
                .setPushTableWriteThroughUnion(true)
                .setIntermediateAggregationsEnabled(false)
                .setColumnarProcessing(false)
                .setColumnarProcessingDictionary(false)
                .setDictionaryAggregation(false)
                .setResourceGroupManager(FILE_BASED_RESOURCE_GROUP_MANAGER));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> propertiesLegacy = new ImmutableMap.Builder<String, String>()
                .put("analyzer.experimental-syntax-enabled", "true")
                .put("distributed-index-joins-enabled", "true")
                .put("distributed-joins-enabled", "false")
                .put("redistribute-writes", "false")
                .put("optimizer.optimize-metadata-queries", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("optimizer.optimize-single-distinct", "false")
                .put("optimizer.push-table-write-through-union", "false")
                .put("optimizer.use-intermediate-aggregations", "true")
                .put("optimizer.columnar-processing", "true")
                .put("optimizer.columnar-processing-dictionary", "true")
                .put("optimizer.dictionary-aggregation", "true")
                .put("resource-group-manager", "test")
                .build();
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("experimental-syntax-enabled", "true")
                .put("distributed-index-joins-enabled", "true")
                .put("distributed-joins-enabled", "false")
                .put("redistribute-writes", "false")
                .put("optimizer.optimize-metadata-queries", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("optimizer.optimize-single-distinct", "false")
                .put("optimizer.push-table-write-through-union", "false")
                .put("optimizer.use-intermediate-aggregations", "true")
                .put("optimizer.columnar-processing", "true")
                .put("optimizer.columnar-processing-dictionary", "true")
                .put("optimizer.dictionary-aggregation", "true")
                .put("resource-group-manager", "test")
                .build();

        FeaturesConfig expected = new FeaturesConfig()
                .setExperimentalSyntaxEnabled(true)
                .setDistributedIndexJoinsEnabled(true)
                .setDistributedJoinsEnabled(false)
                .setRedistributeWrites(false)
                .setOptimizeMetadataQueries(true)
                .setOptimizeHashGeneration(false)
                .setOptimizeSingleDistinct(false)
                .setPushTableWriteThroughUnion(false)
                .setIntermediateAggregationsEnabled(true)
                .setColumnarProcessing(true)
                .setColumnarProcessingDictionary(true)
                .setDictionaryAggregation(true)
                .setResourceGroupManager("test");

        assertFullMapping(properties, expected);
        assertDeprecatedEquivalence(FeaturesConfig.class, properties, propertiesLegacy);
    }
}
