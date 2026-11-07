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
package com.facebook.presto.ducklake.split;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.SplitWeight;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDuckLakeSplit
{
    private static final JsonCodec<DuckLakeSplit> CODEC = JsonCodec.jsonCodec(DuckLakeSplit.class);

    @Test
    public void testParquetSplitJsonRoundTrip()
    {
        DeleteFile delete = new DeleteFile("/data/orders/deletes/d1.parquet", "PARQUET", 25L, 44L, OptionalLong.of(25));

        Map<Integer, Optional<String>> partitionKeys = ImmutableMap.of(
                0, Optional.of("2026-09"),
                1, Optional.empty());

        DuckLakeSplit expected = DuckLakeSplit.parquetSplit(
                "/data/orders/f1.parquet",
                0,
                1024L,
                "PARQUET",
                1024L,
                partitionKeys,
                ImmutableList.of(delete),
                OptionalLong.of(6),
                OptionalLong.of(44),
                10L,
                SplitWeight.fromProportion(0.5));

        String json = CODEC.toJson(expected);
        DuckLakeSplit actual = CODEC.fromJson(json);

        assertEquals(actual, expected);
        assertEquals(actual.getKind(), DuckLakeSplitKind.PARQUET);
        assertEquals(actual.getPath(), "/data/orders/f1.parquet");
        assertEquals(actual.getFileFormat(), "PARQUET");
        assertEquals(actual.getFileSize(), 1024L);
        assertEquals(actual.getPartitionKeys(), partitionKeys);
        assertEquals(actual.getDeletes(), ImmutableList.of(delete));
        assertEquals(actual.getRowIdStart(), OptionalLong.of(6));
        assertEquals(actual.getPartialMax(), OptionalLong.of(44));
        assertEquals(actual.getSnapshotId(), 10L);
        assertEquals(actual.getInlinedTableName(), Optional.empty());
        assertEquals(actual.getSplitWeight(), SplitWeight.fromProportion(0.5));

        for (String property : new String[] {
                "path", "start", "length", "fileFormat", "fileSize", "partitionKeys", "deletes", "nodeSelectionStrategy", "splitWeight"}) {
            assertTrue(json.contains("\"" + property + "\""), "expected JSON property " + property + " in " + json);
        }
    }

    @Test
    public void testInlinedSplitJsonRoundTrip()
    {
        DuckLakeSplit expected = DuckLakeSplit.inlinedSplit("ducklake_inlined_data_21_21", 12L, SplitWeight.standard());

        String json = CODEC.toJson(expected);
        DuckLakeSplit actual = CODEC.fromJson(json);

        assertEquals(actual, expected);
        assertEquals(actual.getKind(), DuckLakeSplitKind.INLINED);
        assertEquals(actual.getInlinedTableName(), Optional.of("ducklake_inlined_data_21_21"));
        assertEquals(actual.getSnapshotId(), 12L);
        assertEquals(actual.getPath(), "");
        assertEquals(actual.getSplitWeight(), SplitWeight.standard());
    }
}
