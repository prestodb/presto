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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.predicate.TupleDomain.fromFixedValues;
import static com.facebook.presto.tpch.TpchRecordSet.convertToPredicate;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.tpch.OrderColumn.ORDER_STATUS;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.PART;
import static io.airlift.tpch.TpchTable.PART_SUPPLIER;
import static io.airlift.tpch.TpchTable.REGION;
import static io.airlift.tpch.TpchTable.SUPPLIER;
import static java.util.Arrays.stream;
import static org.testng.Assert.assertEquals;

public class TestTpchMetadata
{
    private static final double TOLERANCE = 0.01;

    private final TpchMetadata tpchMetadata = new TpchMetadata("tpch");
    private final ConnectorSession session = null;

    @Test
    public void testTableStats()
    {
        TpchMetadata.SCHEMA_NAMES.forEach(schema -> {
            double scaleFactor = TpchMetadata.schemaNameToScaleFactor(schema);

            testTableStats(schema, REGION, 5);
            testTableStats(schema, NATION, 25);
            testTableStats(schema, SUPPLIER, 10_000 * scaleFactor);
            testTableStats(schema, CUSTOMER, 150_000 * scaleFactor);
            testTableStats(schema, PART, 200_000 * scaleFactor);
            testTableStats(schema, PART_SUPPLIER, 800_000 * scaleFactor);
            testTableStats(schema, ORDERS, 1_500_000 * scaleFactor);
            testTableStats(schema, LINE_ITEM, 6_000_000 * scaleFactor);
        });
    }

    @Test
    public void testTableStatsWithConstraints()
    {
        TpchMetadata.SCHEMA_NAMES.forEach(schema -> {
            double scaleFactor = TpchMetadata.schemaNameToScaleFactor(schema);

            testTableStats(schema, ORDERS, Constraint.alwaysFalse(), 0);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "NO SUCH STATUS"), 0);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "F"), 730_400 * scaleFactor);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "O"), 733_300 * scaleFactor);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "P"), 38_543 * scaleFactor);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "F", "NO SUCH STATUS"), 730_400 * scaleFactor);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "F", "O", "P"), 1_500_000 * scaleFactor);
        });
    }

    private void testTableStats(String schema, TpchTable<?> table, double expectedRowCount)
    {
        testTableStats(schema, table, Constraint.alwaysTrue(), expectedRowCount);
    }

    private void testTableStats(String schema, TpchTable<?> table, Constraint<ColumnHandle> constraint, double expectedRowCount)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName(schema, table.getTableName()));
        TableStatistics tableStatistics = tpchMetadata.getTableStatistics(session, tableHandle, constraint);

        double actualRowCountValue = tableStatistics.getRowCount().getValue();
        assertEquals(tableStatistics.getTableStatistics(), ImmutableMap.of("row_count", new Estimate(actualRowCountValue)));
        assertEquals(actualRowCountValue, expectedRowCount, expectedRowCount * TOLERANCE);
    }

    private Constraint<ColumnHandle> constraint(TpchColumn<?> column, String... values)
    {
        List<TupleDomain<ColumnHandle>> valueDomains = stream(values)
                .map(value -> fromFixedValues(valueBinding(column, value)))
                .collect(Collectors.toList());
        TupleDomain<ColumnHandle> domain = TupleDomain.columnWiseUnion(valueDomains);
        return new Constraint<>(domain, convertToPredicate(domain));
    }

    private ImmutableMap<ColumnHandle, NullableValue> valueBinding(TpchColumn<?> column, String value)
    {
        return ImmutableMap.of(
                tpchMetadata.toColumnHandle(column),
                new NullableValue(TpchMetadata.getPrestoType(column), utf8Slice(value)));
    }
}
