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
package com.facebook.presto.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveSplitSource.buildQueueKey;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestHiveSplitSourceQueueKey
{
    private static final Function<HivePartitionKey, String> IDENTITY_NORMALIZER =
            key -> key.getValue().orElse("");

    @Test
    public void testSinglePartitionKeyConsistency()
    {
        List<HivePartitionKey> partitionKeys = ImmutableList.of(
                new HivePartitionKey("ds", Optional.of("2024-01-01")));
        Map<String, String> partitionValues = ImmutableMap.of("ds", "2024-01-01");
        Map<String, String> columnNameMapping = ImmutableMap.of("ds", "ds");

        assertEquals(buildQueueKey(0, partitionKeys, columnNameMapping, IDENTITY_NORMALIZER), buildQueueKey(0, partitionValues));
        assertEquals(buildQueueKey(7, partitionKeys, columnNameMapping, IDENTITY_NORMALIZER), buildQueueKey(7, partitionValues));
    }

    @Test
    public void testMultiplePartitionKeysConsistency()
    {
        List<HivePartitionKey> partitionKeys = ImmutableList.of(
                new HivePartitionKey("hr", Optional.of("06")),
                new HivePartitionKey("ds", Optional.of("2024-01-02")));
        Map<String, String> partitionValues = ImmutableMap.of(
                "hr", "06",
                "ds", "2024-01-02");
        Map<String, String> columnNameMapping = ImmutableMap.of("ds", "ds", "hr", "hr");

        assertEquals(buildQueueKey(3, partitionKeys, columnNameMapping, IDENTITY_NORMALIZER), buildQueueKey(3, partitionValues));
    }

    @Test
    public void testDifferentBucketsProduceDifferentKeys()
    {
        List<HivePartitionKey> partitionKeys = ImmutableList.of(
                new HivePartitionKey("ds", Optional.of("2024-01-01")));
        Map<String, String> columnNameMapping = ImmutableMap.of("ds", "ds");

        assertNotEquals(buildQueueKey(0, partitionKeys, columnNameMapping, IDENTITY_NORMALIZER), buildQueueKey(1, partitionKeys, columnNameMapping, IDENTITY_NORMALIZER));
    }

    @Test
    public void testDifferentPartitionsProduceDifferentKeys()
    {
        Map<String, String> pv1 = ImmutableMap.of("ds", "2024-01-01");
        Map<String, String> pv2 = ImmutableMap.of("ds", "2024-01-02");

        assertNotEquals(buildQueueKey(0, pv1), buildQueueKey(0, pv2));
    }

    @Test
    public void testEmptyMapping()
    {
        List<HivePartitionKey> partitionKeys = ImmutableList.of(
                new HivePartitionKey("ds", Optional.of("2024-01-01")));
        Map<String, String> partitionValues = ImmutableMap.of();
        Map<String, String> columnNameMapping = ImmutableMap.of();

        assertEquals(buildQueueKey(5, partitionKeys, columnNameMapping, IDENTITY_NORMALIZER), buildQueueKey(5, partitionValues));
        assertEquals(buildQueueKey(5, partitionKeys, columnNameMapping, IDENTITY_NORMALIZER), "5|");
    }

    @Test
    public void testThreePartitionColumns()
    {
        List<HivePartitionKey> partitionKeys = ImmutableList.of(
                new HivePartitionKey("region", Optional.of("us")),
                new HivePartitionKey("ds", Optional.of("2024-01-01")),
                new HivePartitionKey("hr", Optional.of("12")));
        Map<String, String> partitionValues = ImmutableMap.of(
                "region", "us",
                "ds", "2024-01-01",
                "hr", "12");
        Map<String, String> columnNameMapping = ImmutableMap.of("ds", "ds", "hr", "hr", "region", "region");

        assertEquals(buildQueueKey(2, partitionKeys, columnNameMapping, IDENTITY_NORMALIZER), buildQueueKey(2, partitionValues));
    }

    @Test
    public void testNullPartitionValueConsistency()
    {
        List<HivePartitionKey> partitionKeys = ImmutableList.of(
                new HivePartitionKey("ds", Optional.empty()));
        Map<String, String> columnNameMapping = ImmutableMap.of("ds", "ds");

        Map<String, String> partitionValues = new java.util.HashMap<>();
        partitionValues.put("ds", null);

        assertEquals(buildQueueKey(0, partitionKeys, columnNameMapping, IDENTITY_NORMALIZER), buildQueueKey(0, partitionValues));
        assertEquals(buildQueueKey(0, partitionKeys, columnNameMapping, IDENTITY_NORMALIZER), "0|ds=|");
    }

    @Test
    public void testPartialPartitionKeyFiltering()
    {
        List<HivePartitionKey> partitionKeys = ImmutableList.of(
                new HivePartitionKey("ds", Optional.of("2024-01-01")),
                new HivePartitionKey("hr", Optional.of("06")));
        Map<String, String> partitionValues = ImmutableMap.of("ds", "2024-01-01");
        Map<String, String> columnNameMapping = ImmutableMap.of("ds", "ds");

        assertEquals(buildQueueKey(0, partitionKeys, columnNameMapping, IDENTITY_NORMALIZER), buildQueueKey(0, partitionValues));
    }

    @Test
    public void testPartialKeyDifferentHrSameQueue()
    {
        List<HivePartitionKey> split1Keys = ImmutableList.of(
                new HivePartitionKey("ds", Optional.of("2024-01-01")),
                new HivePartitionKey("hr", Optional.of("00")));
        List<HivePartitionKey> split2Keys = ImmutableList.of(
                new HivePartitionKey("ds", Optional.of("2024-01-01")),
                new HivePartitionKey("hr", Optional.of("12")));
        Map<String, String> columnNameMapping = ImmutableMap.of("ds", "ds");

        assertEquals(buildQueueKey(0, split1Keys, columnNameMapping, IDENTITY_NORMALIZER), buildQueueKey(0, split2Keys, columnNameMapping, IDENTITY_NORMALIZER));
    }

    @Test
    public void testColumnNameMapping()
    {
        List<HivePartitionKey> t1Keys = ImmutableList.of(
                new HivePartitionKey("ts", Optional.of("2024-01-01")));
        List<HivePartitionKey> t2Keys = ImmutableList.of(
                new HivePartitionKey("ds", Optional.of("2024-01-01")));

        Map<String, String> t1Mapping = ImmutableMap.of("ts", "ds");
        Map<String, String> t2Mapping = ImmutableMap.of("ds", "ds");

        assertEquals(buildQueueKey(0, t1Keys, t1Mapping, IDENTITY_NORMALIZER), buildQueueKey(0, t2Keys, t2Mapping, IDENTITY_NORMALIZER));
    }

    @Test
    public void testColumnNameMappingMultipleColumns()
    {
        List<HivePartitionKey> t1Keys = ImmutableList.of(
                new HivePartitionKey("ts", Optional.of("2024-01-01")),
                new HivePartitionKey("region", Optional.of("us")));
        List<HivePartitionKey> t2Keys = ImmutableList.of(
                new HivePartitionKey("ds", Optional.of("2024-01-01")),
                new HivePartitionKey("area", Optional.of("us")));

        Map<String, String> t1Mapping = ImmutableMap.of("ts", "ds", "region", "area");
        Map<String, String> t2Mapping = ImmutableMap.of("ds", "ds", "area", "area");

        assertEquals(buildQueueKey(0, t1Keys, t1Mapping, IDENTITY_NORMALIZER), buildQueueKey(0, t2Keys, t2Mapping, IDENTITY_NORMALIZER));
    }

    @Test
    public void testHiveDefaultPartitionValueConsistency()
    {
        String hiveDefault = "__HIVE_DEFAULT_PARTITION__";

        List<HivePartitionKey> partitionKeys = ImmutableList.of(
                new HivePartitionKey("ds", Optional.of(hiveDefault)));
        Map<String, String> partitionValues = ImmutableMap.of("ds", hiveDefault);
        Map<String, String> columnNameMapping = ImmutableMap.of("ds", "ds");

        assertEquals(buildQueueKey(0, partitionKeys, columnNameMapping, IDENTITY_NORMALIZER), buildQueueKey(0, partitionValues));
    }
}
