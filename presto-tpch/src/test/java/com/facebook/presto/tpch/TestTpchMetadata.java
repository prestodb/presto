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

import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.tpch.util.PredicateUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.PartColumn;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.facebook.presto.spi.Constraint.alwaysFalse;
import static com.facebook.presto.spi.Constraint.alwaysTrue;
import static com.facebook.presto.tpch.TpchMetadata.getPrestoType;
import static com.facebook.presto.tpch.util.PredicateUtils.filterOutColumnFromPredicate;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.tpch.CustomerColumn.MARKET_SEGMENT;
import static io.airlift.tpch.CustomerColumn.NAME;
import static io.airlift.tpch.LineItemColumn.LINE_NUMBER;
import static io.airlift.tpch.NationColumn.NATION_KEY;
import static io.airlift.tpch.OrderColumn.CLERK;
import static io.airlift.tpch.OrderColumn.ORDER_DATE;
import static io.airlift.tpch.OrderColumn.ORDER_KEY;
import static io.airlift.tpch.OrderColumn.ORDER_STATUS;
import static io.airlift.tpch.PartColumn.PART_KEY;
import static io.airlift.tpch.PartColumn.RETAIL_PRICE;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.PART;
import static io.airlift.tpch.TpchTable.PART_SUPPLIER;
import static io.airlift.tpch.TpchTable.REGION;
import static io.airlift.tpch.TpchTable.SUPPLIER;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTpchMetadata
{
    private static final double TOLERANCE = 0.01;

    private static final List<String> SUPPORTED_SCHEMAS = ImmutableList.of("tiny", "sf1");

    private final TpchMetadata tpchMetadata = new TpchMetadata("tpch");
    private final ConnectorSession session = null;

    @Test
    public void testTableStats()
    {
        SUPPORTED_SCHEMAS.forEach(schema -> {
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
    public void testNoTableStats()
    {
        Stream.of("sf10").forEach(schema -> {
            testNoTableStats(schema, REGION);
            testNoTableStats(schema, NATION);
            testNoTableStats(schema, SUPPLIER);
            testNoTableStats(schema, CUSTOMER);
            testNoTableStats(schema, PART);
            testNoTableStats(schema, PART_SUPPLIER);
            testNoTableStats(schema, ORDERS);
            testNoTableStats(schema, LINE_ITEM);
        });
    }

    @Test
    public void testTableStatsWithConstraints()
    {
        SUPPORTED_SCHEMAS.forEach(schema -> {
            double scaleFactor = TpchMetadata.schemaNameToScaleFactor(schema);

            testTableStats(schema, ORDERS, alwaysFalse(), 0);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "NO SUCH STATUS"), 0);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "F"), 730_400 * scaleFactor);
            testTableStats(schema, ORDERS, constraint(ORDER_STATUS, "O"), 733_300 * scaleFactor);
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
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(tpchMetadata.getColumnHandles(session, tableHandle).values());
        TableStatistics tableStatistics = tpchMetadata.getTableStatistics(session, tableHandle, Optional.empty(), columnHandles, constraint);

        double actualRowCountValue = tableStatistics.getRowCount().getValue();
        assertEquals(tableStatistics.getRowCount(), Estimate.of(actualRowCountValue));
        assertEquals(actualRowCountValue, expectedRowCount, expectedRowCount * TOLERANCE);
    }

    private void testNoTableStats(String schema, TpchTable<?> table)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName(schema, table.getTableName()));
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(tpchMetadata.getColumnHandles(session, tableHandle).values());
        TableStatistics tableStatistics = tpchMetadata.getTableStatistics(session, tableHandle, Optional.empty(), columnHandles, alwaysTrue());
        assertTrue(tableStatistics.getRowCount().isUnknown());
    }

    @Test
    public void testColumnStats()
    {
        Stream.of("tiny", "sf1").forEach(schema -> {
            double scaleFactor = TpchMetadata.schemaNameToScaleFactor(schema);

            //id column
            testColumnStats(schema, NATION, NATION_KEY, columnStatistics(25, 0, 24));

            //foreign key to dictionary identifier columns
            testColumnStats(schema, SUPPLIER, NATION_KEY, columnStatistics(25, 0, 24));

            //foreign key to scalable identifier column
            testColumnStats(schema, PART_SUPPLIER, PART_KEY, columnStatistics(200_000 * scaleFactor, 1, 200_000 * scaleFactor));

            //low-valued numeric column
            testColumnStats(schema, LINE_ITEM, LINE_NUMBER, columnStatistics(7, 1, 7));

            //date
            testColumnStats(schema, ORDERS, ORDER_DATE, columnStatistics(2_400, 8_035, 10_440));

            //varchar and double columns
            if (schema.equals("tiny")) {
                testColumnStats(schema, CUSTOMER, MARKET_SEGMENT, columnStatistics(5, 13465));
                testColumnStats(schema, CUSTOMER, NAME, columnStatistics(150_000 * scaleFactor, 27000));
                testColumnStats(schema, PART, RETAIL_PRICE, columnStatistics(1_099, 901, 1900.99));
            }
            else if (schema.equals("sf1")) {
                testColumnStats(schema, CUSTOMER, NAME, columnStatistics(150_000 * scaleFactor, 2700000));
                testColumnStats(schema, PART, RETAIL_PRICE, columnStatistics(20899, 901, 2089.99));
                testColumnStats(schema, CUSTOMER, MARKET_SEGMENT, columnStatistics(5, 1349610));
            }
        });
    }

    @Test
    public void testColumnStatsWithConstraints()
    {
        SUPPORTED_SCHEMAS.forEach(schema -> {
            double scaleFactor = TpchMetadata.schemaNameToScaleFactor(schema);

            //Single constrained column has only one unique value
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "F"), columnStatistics(1), EnumSet.of(ColumnStatisticsFields.DistinctValuesCount));
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "O"), columnStatistics(1), EnumSet.of(ColumnStatisticsFields.DistinctValuesCount));
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "P"), columnStatistics(1), EnumSet.of(ColumnStatisticsFields.DistinctValuesCount));

            //only min and max values for non-scaling columns can be estimated for non-constrained columns
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "F"), rangeStatistics(3, 6_000_000 * scaleFactor));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "O"), rangeStatistics(1, 6_000_000 * scaleFactor));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "P"), rangeStatistics(65, 6_000_000 * scaleFactor));

            //nothing can be said for always false constraints
            testColumnStats(schema, ORDERS, ORDER_STATUS, alwaysFalse(), noColumnStatistics());
            testColumnStats(schema, ORDERS, ORDER_KEY, alwaysFalse(), noColumnStatistics());
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "NO SUCH STATUS"), noColumnStatistics());
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "NO SUCH STATUS"), noColumnStatistics());

            //unmodified stats are returned for the always true constraint
            testColumnStats(schema, ORDERS, ORDER_STATUS, alwaysTrue(), columnStatistics(3), EnumSet.of(ColumnStatisticsFields.DistinctValuesCount));
            testColumnStats(schema, ORDERS, ORDER_KEY, alwaysTrue(), columnStatistics(1_500_000 * scaleFactor, 1, 6_000_000 * scaleFactor));

            //constraints on columns other than ORDER_STATUS are not supported and are ignored
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(CLERK, "NO SUCH CLERK"), columnStatistics(3), EnumSet.of(ColumnStatisticsFields.DistinctValuesCount));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(CLERK, "Clerk#000000001"), columnStatistics(1_500_000 * scaleFactor, 1, 6_000_000 * scaleFactor));

            //compound constraints are supported
            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "F", "NO SUCH STATUS"), columnStatistics(1), EnumSet.of(ColumnStatisticsFields.DistinctValuesCount));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "F", "NO SUCH STATUS"), rangeStatistics(3, 6_000_000 * scaleFactor));

            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "F", "O"), columnStatistics(2), EnumSet.of(ColumnStatisticsFields.DistinctValuesCount));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "F", "O"), rangeStatistics(1, 6_000_000 * scaleFactor));

            testColumnStats(schema, ORDERS, ORDER_STATUS, constraint(ORDER_STATUS, "F", "O", "P"), columnStatistics(3), EnumSet.of(ColumnStatisticsFields.DistinctValuesCount));
            testColumnStats(schema, ORDERS, ORDER_KEY, constraint(ORDER_STATUS, "F", "O", "P"), columnStatistics(1_500_000 * scaleFactor, 1, 6_000_000 * scaleFactor));
        });
    }

    private void testColumnStats(String schema, TpchTable<?> table, TpchColumn<?> column, ColumnStatistics expectedStatistics)
    {
        testColumnStats(schema, table, column, alwaysTrue(), expectedStatistics, EnumSet.allOf(ColumnStatisticsFields.class));
    }

    private void testColumnStats(String schema, TpchTable<?> table, TpchColumn<?> column, Constraint<ColumnHandle> constraint, ColumnStatistics expectedStatistics)
    {
        testColumnStats(schema, table, column, constraint, expectedStatistics, EnumSet.allOf(ColumnStatisticsFields.class));
    }

    private void testColumnStats(String schema, TpchTable<?> table, TpchColumn<?> column, Constraint<ColumnHandle> constraint, ColumnStatistics expected,
            EnumSet<ColumnStatisticsFields> fieldsToAssertOn)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName(schema, table.getTableName()));
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(tpchMetadata.getColumnHandles(session, tableHandle).values());
        TableStatistics tableStatistics = tpchMetadata.getTableStatistics(session, tableHandle, Optional.empty(), columnHandles, constraint);
        ColumnHandle columnHandle = tpchMetadata.getColumnHandles(session, tableHandle).get(column.getSimplifiedColumnName());

        ColumnStatistics actual = tableStatistics.getColumnStatistics().get(columnHandle);

        EstimateAssertion estimateAssertion = new EstimateAssertion(TOLERANCE);

        if (fieldsToAssertOn.contains(ColumnStatisticsFields.DistinctValuesCount)) {
            estimateAssertion.assertClose(actual.getDistinctValuesCount(), expected.getDistinctValuesCount(), "distinctValuesCount");
        }
        if (fieldsToAssertOn.contains(ColumnStatisticsFields.DataSize)) {
            estimateAssertion.assertClose(actual.getDataSize(), expected.getDataSize(), "dataSize");
        }
        if (fieldsToAssertOn.contains(ColumnStatisticsFields.NullsFraction)) {
            estimateAssertion.assertClose(actual.getNullsFraction(), expected.getNullsFraction(), "nullsFraction");
        }
        if (fieldsToAssertOn.contains(ColumnStatisticsFields.Range)) {
            estimateAssertion.assertClose(actual.getRange(), expected.getRange(), "range");
        }
    }

    @Test
    public void testOrdersOrderStatusPredicatePushdown()
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName("sf1", ORDERS.getTableName()));

        TupleDomain<ColumnHandle> domain;
        ConnectorTableLayoutResult tableLayout;

        domain = fixedValueTupleDomain(tpchMetadata, ORDER_STATUS, utf8Slice("P"));
        tableLayout = getTableOnlyLayout(tpchMetadata, session, tableHandle, new Constraint<>(domain, convertToPredicate(domain, ORDER_STATUS)));
        assertTupleDomainEquals(tableLayout.getUnenforcedConstraint(), TupleDomain.all(), session);
        assertTupleDomainEquals(tableLayout.getTableLayout().getPredicate(), domain, session);

        domain = fixedValueTupleDomain(tpchMetadata, ORDER_KEY, 42L);
        tableLayout = getTableOnlyLayout(tpchMetadata, session, tableHandle, new Constraint<>(domain, convertToPredicate(domain, ORDER_STATUS)));
        assertTupleDomainEquals(tableLayout.getUnenforcedConstraint(), domain, session);
        assertTupleDomainEquals(
                tableLayout.getTableLayout().getPredicate(),
                // The most important thing about the expected value that it is NOT TupleDomain.none() (or equivalent).
                // Using concrete expected value instead of checking TupleDomain::isNone to make sure the test doesn't pass on some other wrong value.
                TupleDomain.columnWiseUnion(
                        fixedValueTupleDomain(tpchMetadata, ORDER_STATUS, utf8Slice("F")),
                        fixedValueTupleDomain(tpchMetadata, ORDER_STATUS, utf8Slice("O")),
                        fixedValueTupleDomain(tpchMetadata, ORDER_STATUS, utf8Slice("P"))),
                session);
    }

    @Test
    public void testPartTypeAndPartContainerPredicatePushdown()
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(session, new SchemaTableName("sf1", PART.getTableName()));

        TupleDomain<ColumnHandle> domain;
        ConnectorTableLayoutResult tableLayout;

        domain = fixedValueTupleDomain(tpchMetadata, PartColumn.TYPE, utf8Slice("SMALL BRUSHED COPPER"));
        tableLayout = getTableOnlyLayout(tpchMetadata, session, tableHandle, new Constraint<>(domain, convertToPredicate(domain, PartColumn.TYPE)));
        assertTupleDomainEquals(tableLayout.getUnenforcedConstraint(), TupleDomain.all(), session);
        assertTupleDomainEquals(
                filterOutColumnFromPredicate(tableLayout.getTableLayout().getPredicate(), tpchMetadata.toColumnHandle(PartColumn.CONTAINER)),
                domain,
                session);

        domain = fixedValueTupleDomain(tpchMetadata, PartColumn.TYPE, utf8Slice("UNKNOWN"));
        tableLayout = getTableOnlyLayout(tpchMetadata, session, tableHandle, new Constraint<>(domain, convertToPredicate(domain, PartColumn.TYPE)));
        assertTupleDomainEquals(tableLayout.getUnenforcedConstraint(), TupleDomain.all(), session);
        assertTupleDomainEquals(tableLayout.getTableLayout().getPredicate(), TupleDomain.none(), session);

        domain = fixedValueTupleDomain(tpchMetadata, PartColumn.CONTAINER, utf8Slice("SM BAG"));
        tableLayout = getTableOnlyLayout(tpchMetadata, session, tableHandle, new Constraint<>(domain, convertToPredicate(domain, PartColumn.CONTAINER)));
        assertTupleDomainEquals(tableLayout.getUnenforcedConstraint(), TupleDomain.all(), session);
        assertTupleDomainEquals(
                filterOutColumnFromPredicate(tableLayout.getTableLayout().getPredicate(), tpchMetadata.toColumnHandle(PartColumn.TYPE)),
                domain,
                session);

        domain = fixedValueTupleDomain(tpchMetadata, PartColumn.CONTAINER, utf8Slice("UNKNOWN"));
        tableLayout = getTableOnlyLayout(tpchMetadata, session, tableHandle, new Constraint<>(domain, convertToPredicate(domain, PartColumn.CONTAINER)));
        assertTupleDomainEquals(tableLayout.getUnenforcedConstraint(), TupleDomain.all(), session);
        assertTupleDomainEquals(tableLayout.getTableLayout().getPredicate(), TupleDomain.none(), session);

        domain = fixedValueTupleDomain(tpchMetadata, PartColumn.TYPE, utf8Slice("SMALL BRUSHED COPPER"), PartColumn.CONTAINER, utf8Slice("SM BAG"));
        tableLayout = getTableOnlyLayout(tpchMetadata, session, tableHandle, new Constraint<>(domain, convertToPredicate(domain, PartColumn.CONTAINER)));
        assertTupleDomainEquals(tableLayout.getUnenforcedConstraint(), TupleDomain.all(), session);
        assertTupleDomainEquals(tableLayout.getTableLayout().getPredicate(), domain, session);

        domain = fixedValueTupleDomain(tpchMetadata, PartColumn.TYPE, utf8Slice("UNKNOWN"), PartColumn.CONTAINER, utf8Slice("UNKNOWN"));
        tableLayout = getTableOnlyLayout(tpchMetadata, session, tableHandle, new Constraint<>(domain, convertToPredicate(domain, PartColumn.TYPE, PartColumn.CONTAINER)));
        assertTupleDomainEquals(tableLayout.getUnenforcedConstraint(), TupleDomain.all(), session);
        assertTupleDomainEquals(tableLayout.getTableLayout().getPredicate(), TupleDomain.none(), session);
    }

    private Predicate<Map<ColumnHandle, NullableValue>> convertToPredicate(TupleDomain<ColumnHandle> domain, TpchColumn... columns)
    {
        Preconditions.checkArgument(columns.length > 0, "No columns given");
        return bindings -> {
            for (TpchColumn column : columns) {
                ColumnHandle columnHandle = tpchMetadata.toColumnHandle(column);
                if (bindings.containsKey(columnHandle)) {
                    NullableValue nullableValue = requireNonNull(bindings.get(columnHandle), "binding is null");
                    if (!PredicateUtils.convertToPredicate(domain, tpchMetadata.toColumnHandle(column)).test(nullableValue)) {
                        return false;
                    }
                }
            }
            return true;
        };
    }

    private void assertTupleDomainEquals(TupleDomain<?> actual, TupleDomain<?> expected, ConnectorSession session)
    {
        if (!Objects.equals(actual, expected)) {
            fail(format("expected [%s] but found [%s]", expected.toString(session.getSqlFunctionProperties()), actual.toString(session.getSqlFunctionProperties())));
        }
    }

    private Constraint<ColumnHandle> constraint(TpchColumn<?> column, String... values)
    {
        List<TupleDomain<ColumnHandle>> valueDomains = stream(values)
                .map(value -> fixedValueTupleDomain(tpchMetadata, column, utf8Slice(value)))
                .collect(toList());
        TupleDomain<ColumnHandle> domain = TupleDomain.columnWiseUnion(valueDomains);
        return new Constraint<>(domain, convertToPredicate(domain, column));
    }

    private static TupleDomain<ColumnHandle> fixedValueTupleDomain(TpchMetadata tpchMetadata, TpchColumn<?> column, Object value)
    {
        requireNonNull(column, "column is null");
        requireNonNull(value, "value is null");
        return TupleDomain.fromFixedValues(
                ImmutableMap.of(tpchMetadata.toColumnHandle(column), new NullableValue(getPrestoType(column), value)));
    }

    private static TupleDomain<ColumnHandle> fixedValueTupleDomain(TpchMetadata tpchMetadata, TpchColumn<?> column1, Object value1, TpchColumn<?> column2, Object value2)
    {
        return TupleDomain.fromFixedValues(
                ImmutableMap.of(
                        tpchMetadata.toColumnHandle(column1), new NullableValue(getPrestoType(column1), value1),
                        tpchMetadata.toColumnHandle(column2), new NullableValue(getPrestoType(column2), value2)));
    }

    private static ConnectorTableLayoutResult getTableOnlyLayout(TpchMetadata tpchMetadata, ConnectorSession session, ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint)
    {
        return tpchMetadata.getTableLayoutForConstraint(session, tableHandle, constraint, Optional.empty());
    }

    private ColumnStatistics noColumnStatistics()
    {
        return createColumnStatistics(Optional.of(0.0), Optional.empty(), Optional.of(0.0));
    }

    private ColumnStatistics columnStatistics(double distinctValuesCount)
    {
        return createColumnStatistics(Optional.of(distinctValuesCount), Optional.empty(), Optional.empty());
    }

    private ColumnStatistics columnStatistics(double distinctValuesCount, double dataSize)
    {
        return createColumnStatistics(Optional.of(distinctValuesCount), Optional.empty(), Optional.of(dataSize));
    }

    private ColumnStatistics columnStatistics(double distinctValuesCount, double min, double max)
    {
        return createColumnStatistics(Optional.of(distinctValuesCount), Optional.of(new DoubleRange(min, max)), Optional.empty());
    }

    private ColumnStatistics rangeStatistics(double min, double max)
    {
        return createColumnStatistics(Optional.empty(), Optional.of(new DoubleRange(min, max)), Optional.empty());
    }

    private static ColumnStatistics createColumnStatistics(Optional<Double> distinctValuesCount, Optional<DoubleRange> range, Optional<Double> dataSize)
    {
        return ColumnStatistics.builder()
                .setNullsFraction(Estimate.zero())
                .setDistinctValuesCount(toEstimate(distinctValuesCount))
                .setRange(range)
                .setDataSize(toEstimate(dataSize))
                .build();
    }

    private static Estimate toEstimate(Optional<Double> value)
    {
        return value
                .map(Estimate::of)
                .orElse(Estimate.unknown());
    }

    private enum ColumnStatisticsFields
    {
        DistinctValuesCount,
        DataSize,
        NullsFraction,
        Range
    }
}
