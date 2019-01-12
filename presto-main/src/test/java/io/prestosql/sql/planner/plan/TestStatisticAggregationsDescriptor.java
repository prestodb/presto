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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.statistics.TableStatisticType.ROW_COUNT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.plan.StatisticAggregationsDescriptor.ColumnStatisticMetadataKeyDeserializer.deserialize;
import static com.facebook.presto.sql.planner.plan.StatisticAggregationsDescriptor.ColumnStatisticMetadataKeySerializer.serialize;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestStatisticAggregationsDescriptor
{
    private static final ImmutableList<String> COLUMNS = ImmutableList.of("", "col1", "$:###:;", "abc+dddd___");

    @Test
    public void testColumnStatisticMetadataKeySerializationRoundTrip()
    {
        for (String column : COLUMNS) {
            for (ColumnStatisticType type : ColumnStatisticType.values()) {
                ColumnStatisticMetadata expected = new ColumnStatisticMetadata(column, type);
                assertEquals(deserialize(serialize(expected)), expected);
            }
        }
    }

    @Test
    public void testSerializationRoundTrip()
    {
        JsonCodec<StatisticAggregationsDescriptor<Symbol>> codec = JsonCodec.jsonCodec(new TypeToken<StatisticAggregationsDescriptor<Symbol>>() {});
        assertSerializationRoundTrip(codec, StatisticAggregationsDescriptor.<Symbol>builder().build());
        assertSerializationRoundTrip(codec, createTestDescriptor());
    }

    private static void assertSerializationRoundTrip(JsonCodec<StatisticAggregationsDescriptor<Symbol>> codec, StatisticAggregationsDescriptor<Symbol> descriptor)
    {
        assertEquals(codec.fromJson(codec.toJson(descriptor)), descriptor);
    }

    private static StatisticAggregationsDescriptor<Symbol> createTestDescriptor()
    {
        StatisticAggregationsDescriptor.Builder<Symbol> builder = StatisticAggregationsDescriptor.builder();
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        for (String column : COLUMNS) {
            for (ColumnStatisticType type : ColumnStatisticType.values()) {
                builder.addColumnStatistic(new ColumnStatisticMetadata(column, type), testSymbol(symbolAllocator));
            }
            builder.addGrouping(column, testSymbol(symbolAllocator));
        }
        builder.addTableStatistic(ROW_COUNT, testSymbol(symbolAllocator));
        return builder.build();
    }

    private static Symbol testSymbol(SymbolAllocator allocator)
    {
        return allocator.newSymbol("test_symbol", BIGINT);
    }
}
