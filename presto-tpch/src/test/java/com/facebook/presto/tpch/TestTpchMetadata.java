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
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.Constraint.alwaysFalse;
import static com.facebook.presto.spi.Constraint.alwaysTrue;
import static com.facebook.presto.spi.predicate.TupleDomain.fromFixedValues;
import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static com.facebook.presto.spi.statistics.Estimate.zeroValue;
import static com.facebook.presto.tpch.TpchRecordSet.convertToPredicate;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.tpch.CustomerColumn.ADDRESS;
import static io.airlift.tpch.CustomerColumn.CUSTOMER_KEY;
import static io.airlift.tpch.CustomerColumn.MARKET_SEGMENT;
import static io.airlift.tpch.CustomerColumn.NAME;
import static io.airlift.tpch.LineItemColumn.COMMIT_DATE;
import static io.airlift.tpch.LineItemColumn.DISCOUNT;
import static io.airlift.tpch.LineItemColumn.EXTENDED_PRICE;
import static io.airlift.tpch.LineItemColumn.LINE_NUMBER;
import static io.airlift.tpch.LineItemColumn.QUANTITY;
import static io.airlift.tpch.LineItemColumn.RECEIPT_DATE;
import static io.airlift.tpch.LineItemColumn.RETURN_FLAG;
import static io.airlift.tpch.LineItemColumn.SHIP_DATE;
import static io.airlift.tpch.LineItemColumn.SHIP_INSTRUCTIONS;
import static io.airlift.tpch.LineItemColumn.SHIP_MODE;
import static io.airlift.tpch.LineItemColumn.STATUS;
import static io.airlift.tpch.LineItemColumn.TAX;
import static io.airlift.tpch.NationColumn.NATION_KEY;
import static io.airlift.tpch.OrderColumn.CLERK;
import static io.airlift.tpch.OrderColumn.ORDER_DATE;
import static io.airlift.tpch.OrderColumn.ORDER_KEY;
import static io.airlift.tpch.OrderColumn.ORDER_PRIORITY;
import static io.airlift.tpch.OrderColumn.ORDER_STATUS;
import static io.airlift.tpch.OrderColumn.SHIP_PRIORITY;
import static io.airlift.tpch.OrderColumn.TOTAL_PRICE;
import static io.airlift.tpch.PartColumn.BRAND;
import static io.airlift.tpch.PartColumn.CONTAINER;
import static io.airlift.tpch.PartColumn.MANUFACTURER;
import static io.airlift.tpch.PartColumn.PART_KEY;
import static io.airlift.tpch.PartColumn.RETAIL_PRICE;
import static io.airlift.tpch.PartColumn.SIZE;
import static io.airlift.tpch.PartColumn.TYPE;
import static io.airlift.tpch.PartSupplierColumn.AVAILABLE_QUANTITY;
import static io.airlift.tpch.PartSupplierColumn.COMMENT;
import static io.airlift.tpch.RegionColumn.REGION_KEY;
import static io.airlift.tpch.SupplierColumn.ACCOUNT_BALANCE;
import static io.airlift.tpch.SupplierColumn.SUPPLIER_KEY;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.PART;
import static io.airlift.tpch.TpchTable.PART_SUPPLIER;
import static io.airlift.tpch.TpchTable.REGION;
import static io.airlift.tpch.TpchTable.SUPPLIER;
import static java.util.Arrays.stream;
import static java.util.Optional.empty;
import static java.util.stream.Collectors.toList;
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

            testTableStats(schema, ORDERS, alwaysFalse(), 0);
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
        testTableStats(schema, table, alwaysTrue(), expectedRowCount);
    }

    private void testTableStats(String schema, TpchTable<?> table, Constraint<ColumnHandle> constraint, double expectedRowCount)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName(schema, table.getTableName()));
        TableStatistics tableStatistics = tpchMetadata.getTableStatistics(session, tableHandle, constraint);

        double actualRowCountValue = tableStatistics.getRowCount().getValue();
        assertEquals(tableStatistics.getTableStatistics(), ImmutableMap.of("row_count", new Estimate(actualRowCountValue)));
        assertEquals(actualRowCountValue, expectedRowCount, expectedRowCount * TOLERANCE);
    }

    @Test
    public void testColumnStats()
    {
        TpchMetadata.SCHEMA_NAMES.forEach(schema -> {
            double scaleFactor = TpchMetadata.schemaNameToScaleFactor(schema);

            //id columns
            testColumnStats(schema, REGION, REGION_KEY, columnStatistics(5, 0, 4));
            testColumnStats(schema, NATION, NATION_KEY, columnStatistics(25, 0, 24));
            testColumnStats(schema, SUPPLIER, SUPPLIER_KEY, columnStatistics(10_000 * scaleFactor, 1, 10_000 * scaleFactor));
            testColumnStats(schema, CUSTOMER, CUSTOMER_KEY, columnStatistics(150_000 * scaleFactor, 1, 150_000 * scaleFactor));
            testColumnStats(schema, PART, PART_KEY, columnStatistics(200_000 * scaleFactor, 1, 200_000 * scaleFactor));
            testColumnStats(schema, ORDERS, ORDER_KEY, columnStatistics(1_500_000 * scaleFactor, 1, 6_000_000 * scaleFactor));

            //foreign keys to dictionary identifier columns
            testColumnStats(schema, NATION, REGION_KEY, columnStatistics(5, 0, 4));
            testColumnStats(schema, SUPPLIER, NATION_KEY, columnStatistics(25, 0, 24));

            //foreign keys to scalable identifier columns
            testColumnStats(schema, PART_SUPPLIER, SUPPLIER_KEY, columnStatistics(10_000 * scaleFactor, 1, 10_000 * scaleFactor));
            testColumnStats(schema, PART_SUPPLIER, PART_KEY, columnStatistics(200_000 * scaleFactor, 1, 200_000 * scaleFactor));

            //semi-uniquely valued varchar columns
            testColumnStats(schema, PART_SUPPLIER, COMMENT, columnStatistics(800_000 * scaleFactor));
            testColumnStats(schema, CUSTOMER, NAME, columnStatistics(150_000 * scaleFactor));
            testColumnStats(schema, CUSTOMER, ADDRESS, columnStatistics(150_000 * scaleFactor));
            testColumnStats(schema, CUSTOMER, COMMENT, columnStatistics(150_000 * scaleFactor));

            //non-scalable columns:
            //dictionaries:
            testColumnStats(schema, CUSTOMER, MARKET_SEGMENT, columnStatistics(5, "AUTOMOBILE", "MACHINERY"));
            testColumnStats(schema, ORDERS, CLERK, columnStatistics(1000, "Clerk#000000001", "Clerk#000001000"));
            testColumnStats(schema, ORDERS, ORDER_STATUS, columnStatistics(3, "F", "P"));
            testColumnStats(schema, ORDERS, ORDER_PRIORITY, columnStatistics(5, "1-URGENT", "5-LOW"));
            testColumnStats(schema, PART, BRAND, columnStatistics(25, "Brand#11", "Brand#55"));
            testColumnStats(schema, PART, CONTAINER, columnStatistics(40, "JUMBO BAG", "WRAP PKG"));
            testColumnStats(schema, PART, MANUFACTURER, columnStatistics(5, "Manufacturer#1", "Manufacturer#5"));
            testColumnStats(schema, PART, SIZE, columnStatistics(50, 1, 50));
            testColumnStats(schema, PART, TYPE, columnStatistics(150, "ECONOMY ANODIZED BRASS", "STANDARD POLISHED TIN"));
            testColumnStats(schema, LINE_ITEM, RETURN_FLAG, columnStatistics(3, "A", "R"));
            testColumnStats(schema, LINE_ITEM, SHIP_INSTRUCTIONS, columnStatistics(4, "COLLECT COD", "TAKE BACK RETURN"));
            testColumnStats(schema, LINE_ITEM, SHIP_MODE, columnStatistics(7, "AIR", "TRUCK"));
            testColumnStats(schema, LINE_ITEM, STATUS, columnStatistics(2, "F", "O"));

            //low-valued numeric columns
            testColumnStats(schema, ORDERS, SHIP_PRIORITY, columnStatistics(1, 0, 0));
            testColumnStats(schema, LINE_ITEM, LINE_NUMBER, columnStatistics(7, 1, 7));
            testColumnStats(schema, LINE_ITEM, QUANTITY, columnStatistics(50, 1, 50));
            testColumnStats(schema, LINE_ITEM, DISCOUNT, columnStatistics(11, 0, 0.1));
            testColumnStats(schema, LINE_ITEM, TAX, columnStatistics(9, 0, 0.08));

            //dates:
            testColumnStats(schema, ORDERS, ORDER_DATE, columnStatistics(2_400, 8_035, 10_440));
            testColumnStats(schema, LINE_ITEM, COMMIT_DATE, columnStatistics(2_450, 8_035, 10_500));
            testColumnStats(schema, LINE_ITEM, SHIP_DATE, columnStatistics(2_525, 8_035, 10_500));
            testColumnStats(schema, LINE_ITEM, RECEIPT_DATE, columnStatistics(2_550, 8_035, 10_500));

            //AVAILABLE_QUANTITY and all money-related columns have quite visible non-scalable min and max
            //but their ndv reaches a plateau for bigger SFs because of the data type used
            //for this reason, those can't be estimated easily
            testColumnStats(schema, PART_SUPPLIER, AVAILABLE_QUANTITY, unknownStatistics());
            testColumnStats(schema, PART, RETAIL_PRICE, unknownStatistics());
            testColumnStats(schema, LINE_ITEM, EXTENDED_PRICE, unknownStatistics());
            testColumnStats(schema, ORDERS, TOTAL_PRICE, unknownStatistics());
            testColumnStats(schema, SUPPLIER, ACCOUNT_BALANCE, unknownStatistics());
            testColumnStats(schema, CUSTOMER, ACCOUNT_BALANCE, unknownStatistics());
        });
    }

    @Test
    public void testColumnStatsWithConstraints()
    {
        TpchMetadata.SCHEMA_NAMES.forEach(schema -> {
            double scaleFactor = TpchMetadata.schemaNameToScaleFactor(schema);

            //value count, min and max are supported for the constrained column
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "F"), columnStatistics(1, "F", "F"));
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "O"), columnStatistics(1, "O", "O"));
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "P"), columnStatistics(1, "P", "P"));

            //only min and max values for non-scaling columns can be estimated for non-constrained columns
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "F"), rangeStatistics(3, 6_000_000 * scaleFactor));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "O"), rangeStatistics(1, 6_000_000 * scaleFactor));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "P"), rangeStatistics(65, 6_000_000 * scaleFactor));
            testColumnStats(schema, ORDERS, CLERK, constraint(ORDER_STATUS, "O"), rangeStatistics("Clerk#000000001", "Clerk#000001000"));
            testColumnStats(schema, ORDERS, COMMENT, constraint(ORDER_STATUS, "O"), unknownStatistics());

            //nothing can be said for always false constraints
            testColumnStats(schema, ORDERS, ORDER_STATUS, alwaysFalse(), columnStatistics(0));
            testColumnStats(schema, ORDERS, ORDER_KEY, alwaysFalse(), columnStatistics(0));
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "NO SUCH STATUS"), columnStatistics(0));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "NO SUCH STATUS"), columnStatistics(0));

            //unmodified stats are returned for the always true constraint
            testColumnStats(schema, ORDERS, ORDER_STATUS, alwaysTrue(), columnStatistics(3, "F", "P"));
            testColumnStats(schema, ORDERS, ORDER_KEY, alwaysTrue(), columnStatistics(1_500_000 * scaleFactor, 1, 6_000_000 * scaleFactor));

            //constraints on columns other than ORDER_STATUS are not supported and are ignored
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(CLERK, "NO SUCH CLERK"), columnStatistics(3, "F", "P"));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(CLERK, "Clerk#000000001"), columnStatistics(1_500_000 * scaleFactor, 1, 6_000_000 * scaleFactor));

            //compound constraints are supported
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "F", "NO SUCH STATUS"), columnStatistics(1, "F", "F"));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "F", "NO SUCH STATUS"), rangeStatistics(3, 6_000_000 * scaleFactor));

            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "F", "O"), columnStatistics(2, "F", "O"));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "F", "O"), rangeStatistics(1, 6_000_000 * scaleFactor));

            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "F", "O", "P"), columnStatistics(3, "F", "P"));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "F", "O", "P"), columnStatistics(1_500_000 * scaleFactor, 1, 6_000_000 * scaleFactor));
        });
    }

    private void testColumnStats(String schema, TpchTable<?> table, TpchColumn<?> column, ColumnStatistics expectedStatistics)
    {
        testColumnStats(schema, table, column, alwaysTrue(), expectedStatistics);
    }

    private void testColumnStats(String schema, TpchTable<?> table, TpchColumn<?> column, Constraint<ColumnHandle> constraint, ColumnStatistics expected)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName(schema, table.getTableName()));
        TableStatistics tableStatistics = tpchMetadata.getTableStatistics(session, tableHandle, constraint);
        ColumnHandle columnHandle = tpchMetadata.getColumnHandles(session, tableHandle).get(column.getSimplifiedColumnName());

        ColumnStatistics actual = tableStatistics.getColumnStatistics().get(columnHandle);

        EstimateAssertion estimateAssertion = new EstimateAssertion(TOLERANCE);

        estimateAssertion.assertClose(
                actual.getOnlyRangeColumnStatistics().getDistinctValuesCount(),
                expected.getOnlyRangeColumnStatistics().getDistinctValuesCount(),
                "distinctValuesCount-s differ");
        estimateAssertion.assertClose(
                actual.getOnlyRangeColumnStatistics().getDataSize(),
                expected.getOnlyRangeColumnStatistics().getDataSize(),
                "dataSize-s differ");
        estimateAssertion.assertClose(
                actual.getNullsFraction(),
                expected.getNullsFraction(),
                "nullsFraction-s differ");
        estimateAssertion.assertClose(
                actual.getOnlyRangeColumnStatistics().getLowValue(),
                expected.getOnlyRangeColumnStatistics().getLowValue(),
                "lowValue-s differ");
        estimateAssertion.assertClose(
                actual.getOnlyRangeColumnStatistics().getHighValue(),
                expected.getOnlyRangeColumnStatistics().getHighValue(),
                "highValue-s differ");
    }

    private Constraint<ColumnHandle> constraint(TpchColumn<?> column, String... values)
    {
        List<TupleDomain<ColumnHandle>> valueDomains = stream(values)
                .map(value -> fromFixedValues(valueBinding(column, value)))
                .collect(toList());
        TupleDomain<ColumnHandle> domain = TupleDomain.columnWiseUnion(valueDomains);
        return new Constraint<>(domain, convertToPredicate(domain));
    }

    private ImmutableMap<ColumnHandle, NullableValue> valueBinding(TpchColumn<?> column, String value)
    {
        return ImmutableMap.of(
                tpchMetadata.toColumnHandle(column),
                new NullableValue(TpchMetadata.getPrestoType(column), utf8Slice(value)));
    }

    private ColumnStatistics columnStatistics(double distinctValuesCount)
    {
        return createColumnStatistics(Optional.of(distinctValuesCount), empty(), empty());
    }

    private ColumnStatistics columnStatistics(double distinctValuesCount, String min, String max)
    {
        return createColumnStatistics(Optional.of(distinctValuesCount), Optional.of(utf8Slice(min)), Optional.of(utf8Slice(max)));
    }

    private ColumnStatistics columnStatistics(double distinctValuesCount, double min, double max)
    {
        return createColumnStatistics(Optional.of(distinctValuesCount), Optional.of(min), Optional.of(max));
    }

    private ColumnStatistics rangeStatistics(String min, String max)
    {
        return createColumnStatistics(empty(), Optional.of(utf8Slice(min)), Optional.of(utf8Slice(max)));
    }

    private ColumnStatistics rangeStatistics(double min, double max)
    {
        return createColumnStatistics(empty(), Optional.of(min), Optional.of(max));
    }

    private ColumnStatistics unknownStatistics()
    {
        return createColumnStatistics(empty(), empty(), empty());
    }

    private ColumnStatistics createColumnStatistics(Optional<Double> distinctValuesCount, Optional<Object> min, Optional<Object> max)
    {
        return ColumnStatistics.builder()
                .addRange(rb -> rb
                        .setDistinctValuesCount(distinctValuesCount.map(Estimate::new).orElse(unknownValue()))
                        .setLowValue(min)
                        .setHighValue(max)
                        .setFraction(new Estimate(1.0)))
                .setNullsFraction(zeroValue())
                .build();
    }
}
