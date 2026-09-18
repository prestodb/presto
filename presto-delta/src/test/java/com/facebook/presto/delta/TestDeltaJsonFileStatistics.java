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

import com.facebook.presto.common.type.TypeSignature;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link DeltaJsonFileStatistics}.
 */
public class TestDeltaJsonFileStatistics
{
    @Test
    public void testCreateReturnsEmptyForNull()
    {
        assertFalse(DeltaJsonFileStatistics.create(null).isPresent());
    }

    @Test
    public void testCreateReturnsEmptyForBlank()
    {
        assertFalse(DeltaJsonFileStatistics.create("").isPresent());
        assertFalse(DeltaJsonFileStatistics.create("   ").isPresent());
    }

    @Test
    public void testCreateReturnsEmptyForNullLiteral()
    {
        assertFalse(DeltaJsonFileStatistics.create("null").isPresent());
        assertFalse(DeltaJsonFileStatistics.create("  null  ").isPresent());
    }

    @Test
    public void testCreateReturnsEmptyForMalformedJson()
    {
        assertFalse(DeltaJsonFileStatistics.create("{not valid json}").isPresent());
    }

    @Test
    public void testCreateParsesFullStats()
    {
        String json = "{\"numRecords\":10,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":10},\"nullCount\":{\"id\":0}}";
        Optional<DeltaJsonFileStatistics> result = DeltaJsonFileStatistics.create(json);
        assertTrue(result.isPresent());

        DeltaJsonFileStatistics stats = result.get();
        assertEquals(stats.getNumRecords(), Optional.of(10L));
        assertEquals(stats.getNullCount("id"), Optional.of(0L));
    }

    @Test
    public void testCreateIgnoresUnknownFields()
    {
        String json = "{\"numRecords\":5,\"tightBounds\":true,\"nullCount\":{\"id\":0}}";
        assertTrue(DeltaJsonFileStatistics.create(json).isPresent());
    }

    @Test
    public void testGetNumRecordsPresent()
    {
        DeltaJsonFileStatistics stats = parse("{\"numRecords\":42}");
        assertEquals(stats.getNumRecords(), Optional.of(42L));
    }

    @Test
    public void testGetNumRecordsMissing()
    {
        DeltaJsonFileStatistics stats = parse("{\"minValues\":{\"id\":1}}");
        assertFalse(stats.getNumRecords().isPresent());
    }

    @Test
    public void testGetNullCountInteger()
    {
        DeltaJsonFileStatistics stats = parse("{\"numRecords\":5,\"nullCount\":{\"id\":3}}");
        assertEquals(stats.getNullCount("id"), Optional.of(3L));
    }

    @Test
    public void testGetNullCountFloatRepresentation()
    {
        DeltaJsonFileStatistics stats = parse("{\"numRecords\":5,\"nullCount\":{\"id\":0.0}}");
        assertEquals(stats.getNullCount("id"), Optional.of(0L));
    }

    @Test
    public void testGetNullCountMissingColumn()
    {
        DeltaJsonFileStatistics stats = parse("{\"numRecords\":5,\"nullCount\":{\"other\":0}}");
        assertFalse(stats.getNullCount("id").isPresent());
    }

    @Test
    public void testGetNullCountMissingMap()
    {
        DeltaJsonFileStatistics stats = parse("{\"numRecords\":5}");
        assertFalse(stats.getNullCount("id").isPresent());
    }

    @Test
    public void testGetNullCountCaseInsensitive()
    {
        DeltaJsonFileStatistics stats = parse("{\"numRecords\":2,\"nullCount\":{\"MyCol\":1}}");
        assertEquals(stats.getNullCount("mycol"), Optional.of(1L));
        assertEquals(stats.getNullCount("MYCOL"), Optional.of(1L));
    }

    @Test
    public void testGetMinMaxPresent()
    {
        DeltaJsonFileStatistics stats = parse("{\"numRecords\":5,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":99}}");
        DeltaColumnHandle handle = handle("id");

        assertTrue(stats.getMinColumnValue(handle).isPresent());
        assertEquals(((Number) stats.getMinColumnValue(handle).get()).intValue(), 1);

        assertTrue(stats.getMaxColumnValue(handle).isPresent());
        assertEquals(((Number) stats.getMaxColumnValue(handle).get()).intValue(), 99);
    }

    @Test
    public void testGetMinMaxMissingColumn()
    {
        DeltaJsonFileStatistics stats = parse("{\"numRecords\":5,\"minValues\":{\"other\":1},\"maxValues\":{\"other\":99}}");
        DeltaColumnHandle handle = handle("id");

        assertFalse(stats.getMinColumnValue(handle).isPresent());
        assertFalse(stats.getMaxColumnValue(handle).isPresent());
    }

    @Test
    public void testGetMinMaxSkipsComplexTypes()
    {
        DeltaJsonFileStatistics stats = parse("{\"numRecords\":5,\"minValues\":{\"arr\":[1,2,3]}}");
        DeltaColumnHandle handle = handle("arr");
        assertFalse(stats.getMinColumnValue(handle).isPresent());
    }

    @Test
    public void testGetMinMaxUsesPhysicalNameWhenPresent()
    {
        DeltaJsonFileStatistics stats = parse("{\"numRecords\":5,\"minValues\":{\"uuid-abc\":10}}");
        DeltaColumnHandle handle = handleWithPhysical("id", "uuid-abc");
        assertTrue(stats.getMinColumnValue(handle).isPresent());
        assertEquals(((Number) stats.getMinColumnValue(handle).get()).intValue(), 10);
    }

    private static DeltaJsonFileStatistics parse(String json)
    {
        return DeltaJsonFileStatistics.create(json)
                .orElseThrow(() -> new AssertionError("Expected stats to parse successfully but got empty for: " + json));
    }

    private static DeltaColumnHandle handle(String logicalName)
    {
        return new DeltaColumnHandle(
                null,
                null,
                logicalName,
                TypeSignature.parseTypeSignature("bigint"),
                DeltaColumnHandle.ColumnType.REGULAR,
                Optional.empty());
    }

    private static DeltaColumnHandle handleWithPhysical(String logicalName, String physicalName)
    {
        return new DeltaColumnHandle(
                null,
                physicalName,
                logicalName,
                TypeSignature.parseTypeSignature("bigint"),
                DeltaColumnHandle.ColumnType.REGULAR,
                Optional.empty());
    }
}
