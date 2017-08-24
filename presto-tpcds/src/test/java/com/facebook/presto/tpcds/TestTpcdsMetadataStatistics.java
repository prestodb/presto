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

package com.facebook.presto.tpcds;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.RangeColumnStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.primitives.Primitives;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.CallCenterColumn;
import com.teradata.tpcds.column.WebSiteColumn;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.spi.Constraint.alwaysTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestTpcdsMetadataStatistics
{
    private static final EstimateAssertion estimateAssertion = new EstimateAssertion(0.01);
    private static final ConnectorSession session = null;
    private final TpcdsMetadata metadata = new TpcdsMetadata();

    @Test
    public void testNoTableStatsForNotSupportedSchema()
    {
        Stream.of("sf0.001", "sf0.1", "sf10")
                .forEach(schemaName -> Table.getBaseTables()
                        .forEach(table -> {
                            SchemaTableName schemaTableName = new SchemaTableName(schemaName, table.getName());
                            ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName);
                            TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, alwaysTrue());
                            assertTrue(tableStatistics.getRowCount().isValueUnknown());
                            assertTrue(tableStatistics.getColumnStatistics().isEmpty());
                        }));
    }

    @Test
    public void testTableStatsExistenceSupportedSchema()
    {
        Stream.of("sf0.01", "tiny", "sf1", "sf1.000")
                .forEach(schemaName -> Table.getBaseTables()
                        .forEach(table -> {
                            SchemaTableName schemaTableName = new SchemaTableName(schemaName, table.getName());
                            ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName);
                            TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, alwaysTrue());
                            assertFalse(tableStatistics.getRowCount().isValueUnknown());
                            for (ColumnHandle column : metadata.getColumnHandles(session, tableHandle).values()) {
                                assertTrue(tableStatistics.getColumnStatistics().containsKey(column));
                                assertNotNull(tableStatistics.getColumnStatistics().get(column));

                                TpcdsColumnHandle tpcdsColumn = (TpcdsColumnHandle) column;
                                Optional<Object> low = tableStatistics.getColumnStatistics().get(column).getOnlyRangeColumnStatistics().getLowValue();
                                if (low.isPresent()) {
                                    assertEquals(low.get().getClass(), Primitives.wrap(tpcdsColumn.getType().getJavaType()));
                                }
                                Optional<Object> high = tableStatistics.getColumnStatistics().get(column).getOnlyRangeColumnStatistics().getLowValue();
                                if (high.isPresent()) {
                                    assertEquals(high.get().getClass(), Primitives.wrap(tpcdsColumn.getType().getJavaType()));
                                }
                            }
                        }));
    }

    @Test
    public void testTableStatsDetails()
    {
        SchemaTableName schemaTableName = new SchemaTableName("sf1", Table.CALL_CENTER.getName());
        ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName);
        TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, alwaysTrue());

        estimateAssertion.assertClose(tableStatistics.getRowCount(), new Estimate(6), "Row count does not match");

        // all columns have stats
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        for (ColumnHandle column : columnHandles.values()) {
            assertTrue(tableStatistics.getColumnStatistics().containsKey(column));
            assertNotNull(tableStatistics.getColumnStatistics().get(column));
        }

        // identifier
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_CALL_CENTER_SK.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(0))
                        .addRange(range -> range
                                .setFraction(new Estimate(1.0))
                                .setDistinctValuesCount(new Estimate(6))
                                .setLowValue(Optional.of(1L))
                                .setHighValue(Optional.of(6L))
                                .build())
                        .build());

        // varchar
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_CALL_CENTER_ID.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(0))
                        .addRange(range -> range
                                .setFraction(new Estimate(1.0))
                                .setDistinctValuesCount(new Estimate(3))
                                .setLowValue(Optional.of(Slices.utf8Slice("AAAAAAAABAAAAAAA")))
                                .setHighValue(Optional.of(Slices.utf8Slice("AAAAAAAAEAAAAAAA")))
                                .build())
                        .build());

        // char
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_ZIP.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(0))
                        .addRange(range -> range
                                .setFraction(new Estimate(1.0))
                                .setDistinctValuesCount(new Estimate(1))
                                .setLowValue(Optional.of(Slices.utf8Slice("31904")))
                                .setHighValue(Optional.of(Slices.utf8Slice("31904")))
                                .build())
                        .build());

        // decimal
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_GMT_OFFSET.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(0))
                        .addRange(range -> range
                                .setFraction(new Estimate(1.0))
                                .setDistinctValuesCount(new Estimate(1))
                                .setLowValue(Optional.of(-500L))
                                .setHighValue(Optional.of(-500L))
                                .build())
                        .build());

        // date
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_REC_START_DATE.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(0))
                        .addRange(range -> range
                                .setFraction(new Estimate(1))
                                .setDistinctValuesCount(new Estimate(4))
                                .setLowValue(Optional.of(10227L))
                                .setHighValue(Optional.of(11688L))
                                .build())
                        .build());

        // only null values
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(CallCenterColumn.CC_CLOSED_DATE_SK.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(1))
                        .addRange(range -> range
                                .setFraction(new Estimate(0))
                                .setDistinctValuesCount(new Estimate(0))
                                .setLowValue(Optional.empty())
                                .setHighValue(Optional.empty())
                                .build())
                        .build());
    }

    @Test
    public void testNullFraction()
    {
        SchemaTableName schemaTableName = new SchemaTableName("sf1", Table.WEB_SITE.getName());
        ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName);
        TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, alwaysTrue());

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);

        // some null values
        assertColumnStatistics(
                tableStatistics.getColumnStatistics().get(columnHandles.get(WebSiteColumn.WEB_REC_END_DATE.getName())),
                ColumnStatistics.builder()
                        .setNullsFraction(new Estimate(0.5))
                        .addRange(range -> range
                                .setFraction(new Estimate(0.5))
                                .setDistinctValuesCount(new Estimate(3))
                                .setLowValue(Optional.of(10819L))
                                .setHighValue(Optional.of(11549L))
                                .build())
                        .build());
    }

    private void assertColumnStatistics(ColumnStatistics actual, ColumnStatistics expected)
    {
        estimateAssertion.assertClose(actual.getNullsFraction(), expected.getNullsFraction(), "Null fraction does not match");

        RangeColumnStatistics actualRange = actual.getOnlyRangeColumnStatistics();
        RangeColumnStatistics expectedRange = expected.getOnlyRangeColumnStatistics();
        if (expectedRange.getFraction().isValueUnknown()) {
            assertTrue(actualRange.getFraction().isValueUnknown());
        }
        else {
            estimateAssertion.assertClose(actualRange.getFraction(), expectedRange.getFraction(), "Fraction does not match");
        }
        if (expectedRange.getDataSize().isValueUnknown()) {
            assertTrue(actualRange.getDataSize().isValueUnknown());
        }
        else {
            estimateAssertion.assertClose(actualRange.getDataSize(), expectedRange.getDataSize(), "Data size does not match");
        }
        if (expectedRange.getDistinctValuesCount().isValueUnknown()) {
            assertTrue(actualRange.getDistinctValuesCount().isValueUnknown());
        }
        else {
            estimateAssertion.assertClose(actualRange.getDistinctValuesCount(), expectedRange.getDistinctValuesCount(), "Distinct values count does not match");
        }
        assertEquals(actualRange.getLowValue(), expectedRange.getLowValue());
        assertEquals(actualRange.getHighValue(), expectedRange.getHighValue());
    }
}
