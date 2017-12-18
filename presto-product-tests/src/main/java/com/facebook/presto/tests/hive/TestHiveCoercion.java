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
package com.facebook.presto.tests.hive;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.Requires;
import com.teradata.tempto.assertions.QueryAssert;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.table.MutableTableRequirement;
import com.teradata.tempto.fulfillment.table.MutableTablesState;
import com.teradata.tempto.fulfillment.table.TableDefinition;
import com.teradata.tempto.fulfillment.table.TableHandle;
import com.teradata.tempto.fulfillment.table.TableInstance;
import com.teradata.tempto.fulfillment.table.hive.HiveTableDefinition;
import com.teradata.tempto.query.QueryExecutor;
import com.teradata.tempto.query.QueryResult;
import com.teradata.tempto.query.QueryType;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.tests.TestGroups.HIVE_COERCION;
import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.usingTeradataJdbcDriver;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.context.ThreadLocalTestContextHolder.testContext;
import static com.teradata.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static com.teradata.tempto.fulfillment.table.TableHandle.tableHandle;
import static com.teradata.tempto.query.QueryExecutor.defaultQueryExecutor;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.LONGNVARCHAR;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARBINARY;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class TestHiveCoercion
        extends ProductTest
{
    private static String tableNameFormat = "%s_hive_coercion";

    private static final List<TestTuple> TEST_TUPLES = ImmutableList.<TestTuple>builder()
            .add(new TestTuple("tinyint", "smallint",
                    asList("-1", "1"),
                    asList(-1, 1)))
            .add(new TestTuple("tinyint", "int",
                    asList("2", "-2"),
                    asList(2, -2)))
            .add(new TestTuple("tinyint", "bigint",
                    asList("-3"),
                    asList(-3L)))
            .add(new TestTuple("smallint", "int",
                    asList("100", "-100"),
                    asList(100, -100)))
            .add(new TestTuple("smallint", "bigint",
                    asList("-101", "101"),
                    asList(-101L, 101L)))
            .add(new TestTuple("int", "bigint",
                    asList("2323", "-2323"),
                    asList(2323L, -2323L)))
            .add(new TestTuple("bigint", "string",
                    asList("12345", "-12345"),
                    asList("12345", "-12345")))
            .add(new TestTuple("float", "double",
                    asList("0.5", "-1.5"),
                    asList(0.5, -1.5)))
            .add(new TestTuple("decimal(11,5)", "decimal(8,3)",
                    asList("12345.123",
                            "12345.12350",
                            "123456.12345",
                            "99999.99999"),
                    asList(new BigDecimal("12345.123"),
                            new BigDecimal("12345.124"),
                            null,
                            null)))
            .add(new TestTuple("decimal(21,15)", "decimal(8,3)",
                    asList("12345.123",
                            "12345.123999999999999",
                            "123456.123999999999999"),
                    asList(new BigDecimal("12345.123"),
                            new BigDecimal("12345.124"),
                            null)))
            .add(new TestTuple("decimal(27,21)", "decimal(20,15)",
                    asList("12345.123456789012345",
                            "12345.123456789012345678901",
                            "123456.123999999999999999999"),
                    asList(new BigDecimal("12345.123456789012345"),
                            new BigDecimal("12345.123456789012346"),
                            null)))
            .add(new TestTuple("decimal(8,3)", "decimal(20,15)",
                    asList("12345.123"),
                    asList(new BigDecimal("12345.123"))))
            .add(new TestTuple("decimal(8,3)", "float",
                    asList("12345.123"),
                    asList(12345.123f)))
            .add(new TestTuple("decimal(25,5)", "float",
                    asList("12345678901234567890.12345"),
                    asList(12345678901234567890.12345f)))
            .add(new TestTuple("decimal(8,3)", "double",
                    asList("12345.123"),
                    asList(12345.123)))
            .add(new TestTuple("decimal(25,5)", "double",
                    asList("12345678901234567890.12345"),
                    asList(12345678901234567890.12345)))
            .build();

    public static final HiveTableDefinition HIVE_COERCION_TEXTFILE = tableDefinitionBuilder("TEXTFILE", Optional.empty(), Optional.of("DELIMITED FIELDS TERMINATED BY '|'"));
    public static final HiveTableDefinition HIVE_COERCION_PARQUET = tableDefinitionBuilder("PARQUET", Optional.empty(), Optional.empty());
    public static final HiveTableDefinition HIVE_COERCION_ORC = tableDefinitionBuilder("ORC", Optional.empty(), Optional.empty());
    public static final HiveTableDefinition HIVE_COERCION_RCTEXT = tableDefinitionBuilder("RCFILE", Optional.of("RCTEXT"), Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'"));
    public static final HiveTableDefinition HIVE_COERCION_RCBINARY = tableDefinitionBuilder("RCFILE", Optional.of("RCBINARY"), Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'"));

    private static HiveTableDefinition tableDefinitionBuilder(String fileFormat, Optional<String> recommendTableName, Optional<String> rowFormat)
    {
        String tableName = format(tableNameFormat, recommendTableName.orElse(fileFormat).toLowerCase(Locale.ENGLISH));
        List<TestTuple> testTuples = getTestTuples(fileFormat.equals("PARQUET"));
        StringBuilder template = new StringBuilder();

        template.append("CREATE TABLE %NAME% (");
        template.append(Joiner.on(",").join(
                testTuples.stream()
                        .map(testTuple -> format(" %s %s", testTuple.columnName(), testTuple.getFromType()))
                        .collect(toList())));
        template.append(") PARTITIONED BY (id BIGINT)");
        template.append((rowFormat.map(format -> " ROW FORMAT " + format).orElse("")));
        template.append(" STORED AS " + fileFormat);

        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate(template.toString())
                .setNoData()
                .build();
    }

    private static List<TestTuple> getTestTuples(boolean forParquet)
    {
        List<TestTuple> testTuples = TEST_TUPLES;
        if (forParquet) {
            testTuples = testTuples.stream()
                    .filter(TestTuple::validForParquet)
                    .collect(toList());
        }
        return testTuples;
    }

    public static final class TextRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_TEXTFILE).withState(CREATED).build();
        }
    }

    public static final class OrcRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_ORC).withState(CREATED).build();
        }
    }

    public static final class RcTextRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_RCTEXT).withState(CREATED).build();
        }
    }

    public static final class RcBinaryRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_RCBINARY).withState(CREATED).build();
        }
    }

    public static final class ParquetRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(HIVE_COERCION_PARQUET).withState(CREATED).build();
        }
    }

    @Requires(TextRequirements.class)
    @Test(groups = {HIVE_COERCION, HIVE_CONNECTOR})
    public void testHiveCoercionTextFile()
            throws SQLException
    {
        doTestHiveCoercion(HIVE_COERCION_TEXTFILE);
    }

    @Requires(OrcRequirements.class)
    @Test(groups = {HIVE_COERCION, HIVE_CONNECTOR})
    public void testHiveCoercionOrc()
            throws SQLException
    {
        doTestHiveCoercion(HIVE_COERCION_ORC);
    }

    @Requires(RcTextRequirements.class)
    @Test(groups = {HIVE_COERCION, HIVE_CONNECTOR})
    public void testHiveCoercionRcText()
            throws SQLException
    {
        doTestHiveCoercion(HIVE_COERCION_RCTEXT);
    }

    @Requires(RcBinaryRequirements.class)
    @Test(groups = {HIVE_COERCION, HIVE_CONNECTOR})
    public void testHiveCoercionRcBinary()
            throws SQLException
    {
        doTestHiveCoercion(HIVE_COERCION_RCBINARY);
    }

    @Requires(ParquetRequirements.class)
    @Test(groups = {HIVE_COERCION, HIVE_CONNECTOR})
    public void testHiveCoercionParquet()
            throws SQLException
    {
        doTestHiveCoercion(HIVE_COERCION_PARQUET);
    }

    private void doTestHiveCoercion(HiveTableDefinition tableDefinition)
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(tableDefinition).getNameInDatabase();

        List<TestTuple> testTuples = getTestTuples(tableDefinition == HIVE_COERCION_PARQUET);
        int rowCount = getRowCount(testTuples);

        StringBuilder insertQuery = new StringBuilder("INSERT INTO TABLE " + tableName + " ");
        insertQuery.append("PARTITION (id=1) ");
        insertQuery.append("VALUES ");
        insertQuery.append(Joiner.on(",").join(range(0, rowCount)
                .mapToObj(row -> valuesForRow(testTuples, row))
                .collect(toList())));
        executeHiveQuery(insertQuery.toString());

        alterTableColumnTypes(tableName, testTuples);
        assertProperAlteredTableSchema(tableName, testTuples);

        QueryResult queryResult = query(format("SELECT * FROM %s", tableName));
        assertColumnTypes(queryResult, testTuples);
        assertThat(queryResult).containsOnly(range(0, rowCount)
                .mapToObj(
                        row -> {
                            List<Object> expectedValues = new ArrayList<>();
                            expectedValues.addAll(testTuples.stream()
                                    .map(testTuple -> getOrNull(testTuple.getExpectedValues(), row))
                                    .collect(toList()));
                            expectedValues.add(1L); // partitioning column
                            return row(expectedValues.toArray());
                        })
                .collect(toList()));
    }

    private String valuesForRow(List<TestTuple> testTuples, int row)
    {
        return "(" + Joiner.on(",").join(testTuples.stream()
                .map(testTuple -> getOrNull(testTuple.getInsertedValues(), row))
                .map(value -> value == null ? "null" : value)
                .collect(toList())) + ")";
    }

    private int getRowCount(List<TestTuple> testTuples)
    {
        return testTuples.stream().mapToInt(testTuple -> testTuple.getExpectedValues().size()).max().orElse(0);
    }

    private <T> T getOrNull(List<T> list, int index)
    {
        if (index < list.size()) {
            return list.get(index);
        }
        else {
            return null;
        }
    }

    private void assertProperAlteredTableSchema(String tableName, List<TestTuple> testTuples)
    {
        List<QueryAssert.Row> expectedResult = new ArrayList<>();
        expectedResult.addAll(testTuples.stream()
                .map(testTuple -> row(testTuple.columnName(), testTuple.getPrestoToType()))
                .collect(toList()));
        expectedResult.add(row("id", "bigint")); // partitioning column
        assertThat(query("SHOW COLUMNS FROM " + tableName, QueryType.SELECT).project(1, 2)).containsExactly(expectedResult);
    }

    private void assertColumnTypes(QueryResult queryResult, List<TestTuple> testTuples)
    {
        Connection connection = defaultQueryExecutor().getConnection();
        List<JDBCType> expectedTypes = new ArrayList<>();
        expectedTypes.addAll(testTuples.stream()
                .map(testTuple -> testTuple.getJdbcToType(usingTeradataJdbcDriver(connection)))
                .collect(toList()));
        expectedTypes.add(BIGINT); // partitioning column
        assertThat(queryResult).hasColumns(
                expectedTypes);
    }

    private void alterTableColumnTypes(String tableName, List<TestTuple> testTuples)
    {
        testTuples.forEach(testTuple -> executeHiveQuery(
                format("ALTER TABLE %s CHANGE COLUMN %s %s %s", tableName, testTuple.columnName(), testTuple.columnName(), testTuple.getToType())));
    }

    private static TableInstance mutableTableInstanceOf(TableDefinition tableDefinition)
    {
        if (tableDefinition.getDatabase().isPresent()) {
            return mutableTableInstanceOf(tableDefinition, tableDefinition.getDatabase().get());
        }
        else {
            return mutableTableInstanceOf(tableHandleInSchema(tableDefinition));
        }
    }

    private static TableInstance mutableTableInstanceOf(TableDefinition tableDefinition, String database)
    {
        return mutableTableInstanceOf(tableHandleInSchema(tableDefinition).inDatabase(database));
    }

    private static TableInstance mutableTableInstanceOf(TableHandle tableHandle)
    {
        return testContext().getDependency(MutableTablesState.class).get(tableHandle);
    }

    private static TableHandle tableHandleInSchema(TableDefinition tableDefinition)
    {
        TableHandle tableHandle = tableHandle(tableDefinition.getName());
        if (tableDefinition.getSchema().isPresent()) {
            tableHandle = tableHandle.inSchema(tableDefinition.getSchema().get());
        }
        return tableHandle;
    }

    private static QueryResult executeHiveQuery(String query)
    {
        return testContext().getDependency(QueryExecutor.class, "hive").executeQuery(query);
    }

    private static class TestTuple
    {
        private final String fromType;
        private final String toType;
        private final List<String> insertedValues;
        private final List<Object> expectedValues;

        public TestTuple(String fromType, String toType, List<String> insertedValues, List<Object> expectedValues)
        {
            Preconditions.checkArgument(insertedValues.size() == expectedValues.size(), "length of insertedValues does not match the length of expectedValues");
            this.fromType = fromType;
            this.toType = toType;
            this.insertedValues = new ArrayList<>(insertedValues);
            this.expectedValues = new ArrayList<>(expectedValues);
        }

        public String getFromType()
        {
            return fromType;
        }

        public String getToType()
        {
            return toType;
        }

        public List<String> getInsertedValues()
        {
            return insertedValues;
        }

        public List<Object> getExpectedValues()
        {
            return expectedValues;
        }

        public String columnName()
        {
            String fromTypeName = fromType.replaceAll("[^\\w]", "_");
            String toTypeName = toType.replaceAll("[^\\w]", "_");
            return format("%s_to_%s", fromTypeName, toTypeName);
        }

        boolean validForParquet()
        {
            return !fromType.equals("float") && !toType.equals("float");
        }

        public String getPrestoToType()
        {
            if (toType.startsWith("decimal")) {
                return toType;
            }
            switch (toType) {
                case "tinyint":
                case "smallint":
                case "bigint":
                case "double":
                    return toType;
                case "float":
                    return "real";
                case "int":
                    return "integer";
                case "string":
                    return "varchar";
                default:
                    throw new IllegalArgumentException("unknown mapping to Presto type for Hive type " + toType);
            }
        }

        public JDBCType getJdbcToType(boolean teradataDriver)
        {
            if (toType.startsWith("decimal")) {
                return DECIMAL;
            }
            switch (toType) {
                case "tinyint":
                    return TINYINT;
                case "smallint":
                    return SMALLINT;
                case "int":
                    return INTEGER;
                case "bigint":
                    return BIGINT;
                case "float":
                    return REAL;
                case "double":
                    return DOUBLE;
                case "string":
                    return teradataDriver ? VARBINARY : LONGNVARCHAR;
                default:
                    throw new IllegalArgumentException("unknown mapping to JDBC type for Hive type " + toType);
            }
        }
    }
}
