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
package com.facebook.presto.orc.metadata.statistics;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.OrcType;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcTester.arrayType;
import static com.facebook.presto.orc.OrcTester.mapType;
import static com.facebook.presto.orc.OrcTester.rowType;
import static com.facebook.presto.orc.metadata.statistics.StatisticsBuilders.createEmptyColumnStatistics;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestStatisticsBuilders
{
    @Test
    public void testCreateStatisticsBuilder()
    {
        assertTrue(createStatisticsBuilder(TINYINT) instanceof CountStatisticsBuilder);
        assertTrue(createStatisticsBuilder(SMALLINT) instanceof IntegerStatisticsBuilder);
        assertTrue(createStatisticsBuilder(INTEGER) instanceof IntegerStatisticsBuilder);
        assertTrue(createStatisticsBuilder(BIGINT) instanceof IntegerStatisticsBuilder);
        assertTrue(createStatisticsBuilder(VARCHAR) instanceof StringStatisticsBuilder);
        assertTrue(createStatisticsBuilder(VARBINARY) instanceof BinaryStatisticsBuilder);
        assertTrue(createStatisticsBuilder(BOOLEAN) instanceof BooleanStatisticsBuilder);
        assertTrue(createStatisticsBuilder(REAL) instanceof DoubleStatisticsBuilder);
        assertTrue(createStatisticsBuilder(DOUBLE) instanceof DoubleStatisticsBuilder);
        assertTrue(createStatisticsBuilder(TIMESTAMP) instanceof CountStatisticsBuilder);
        assertTrue(createStatisticsBuilder(TIMESTAMP_MICROSECONDS) instanceof CountStatisticsBuilder);
        assertTrue(createStatisticsBuilder(mapType(INTEGER, INTEGER)) instanceof CountStatisticsBuilder);
        assertTrue(createStatisticsBuilder(arrayType(INTEGER)) instanceof CountStatisticsBuilder);
        assertTrue(createStatisticsBuilder(rowType(INTEGER)) instanceof CountStatisticsBuilder);
    }

    @Test
    public void testCreateStatisticsBuilderInvalidType()
    {
        assertThrows(IllegalArgumentException.class, () -> createStatisticsBuilder(DATE));
    }

    @Test
    public void testCreateEmptyColumnStatistics()
    {
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder().setCompressionKind(CompressionKind.ZSTD).build();
        Type rootType = rowType(// node index 0
                TINYINT, // 1
                mapType(TINYINT, SMALLINT), // 2-4,
                REAL, // 5
                mapType(INTEGER, arrayType(BIGINT)), // 6-9
                DOUBLE, // 10
                arrayType(VARCHAR), // 11-12
                TIMESTAMP, // 13
                rowType(VARBINARY, rowType(BOOLEAN))); // 14-17
        List<OrcType> orcTypes = OrcType.toOrcType(0, rootType);

        // all CountStatisticsBuilders would usually return ColumnStatistics when no values have been provided
        Map<Integer, ColumnStatistics> columnStatistics = createEmptyColumnStatistics(orcTypes, 0, columnWriterOptions);
        assertEquals(columnStatistics.size(), 18);
        for (int i = 0; i < 18; i++) {
            ColumnStatistics emptyColumnStatistics = columnStatistics.get(i);
            assertNotNull(emptyColumnStatistics);
            assertEquals(emptyColumnStatistics.getNumberOfValues(), 0);
        }
    }

    private static StatisticsBuilder createStatisticsBuilder(Type type)
    {
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder().setCompressionKind(CompressionKind.ZSTD).build();
        OrcType orcType = OrcType.toOrcType(0, type).get(0);
        Supplier<? extends StatisticsBuilder> supplier = StatisticsBuilders.createStatisticsBuilderSupplier(orcType, columnWriterOptions);
        return supplier.get();
    }
}
