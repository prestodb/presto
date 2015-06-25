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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.Histogram.NAME;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestHistogram
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testDuplicateKeysValues()
            throws Exception
    {
        MapType mapType = new MapType(VARCHAR, BIGINT);
        InternalAggregationFunction aggFunc = metadata.getExactFunction(new Signature(NAME, mapType.getTypeSignature().toString(), StandardTypes.VARCHAR)).getAggregationFunction();
        assertAggregation(
                aggFunc,
                1.0,
                ImmutableMap.of("a", 1L, "b", 1L, "c", 1L),
                new Page(createStringsBlock("a", "b", "c")));

        mapType = new MapType(VARCHAR, BIGINT);
        aggFunc = metadata.getExactFunction(new Signature(NAME, mapType.getTypeSignature().toString(), StandardTypes.VARCHAR)).getAggregationFunction();
        assertAggregation(
                aggFunc,
                1.0,
                ImmutableMap.of("a", 2L, "b", 1L),
                new Page(createStringsBlock("a", "b", "a")));
    }
}
