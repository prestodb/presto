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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.hive.HiveClientConfig.InsertExistingPartitionsBehavior;
import com.facebook.presto.metadata.InsertTableHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartialMergePushdownStrategy;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.planPrinter.IOPlanPrinter.ColumnConstraint;
import com.facebook.presto.sql.planner.planPrinter.IOPlanPrinter.FormattedDomain;
import com.facebook.presto.sql.planner.planPrinter.IOPlanPrinter.FormattedMarker;
import com.facebook.presto.sql.planner.planPrinter.IOPlanPrinter.FormattedRange;
import com.facebook.presto.sql.planner.planPrinter.IOPlanPrinter.IOPlan;
import com.facebook.presto.sql.planner.planPrinter.IOPlanPrinter.IOPlan.TableColumnInfo;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.QueryAssertions;
import com.facebook.presto.tests.ResultWithQueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.apache.hadoop.fs.Path;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.LongStream;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.SystemSessionProperties.COLOCATED_JOIN;
import static com.facebook.presto.SystemSessionProperties.CONCURRENT_LIFESPANS_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.INLINE_SQL_FUNCTIONS;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.LOG_INVOKED_FUNCTION_NAMES_ENABLED;
import static com.facebook.presto.SystemSessionProperties.PARTIAL_MERGE_PUSHDOWN_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.PARTITIONING_PROVIDER_CATALOG;
import static com.facebook.presto.common.predicate.Marker.Bound.EXACTLY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.FILE_SIZE_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.ROW_ID_COLUMN_NAME;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.TPCH_SCHEMA;
import static com.facebook.presto.hive.HiveQueryRunner.createBucketedSession;
import static com.facebook.presto.hive.HiveQueryRunner.createMaterializeExchangesSession;
import static com.facebook.presto.hive.HiveSessionProperties.FILE_RENAMING_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.MANIFEST_VERIFICATION_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.OPTIMIZED_PARTITION_UPDATE_SERIALIZATION_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.PARTIAL_AGGREGATION_PUSHDOWN_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.PARTIAL_AGGREGATION_PUSHDOWN_FOR_VARIABLE_LENGTH_DATATYPES_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.PREFER_MANIFESTS_TO_LIST_FILES;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.RCFILE_OPTIMIZED_WRITER_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.SORTED_WRITE_TEMP_PATH_SUBDIRECTORY_COUNT;
import static com.facebook.presto.hive.HiveSessionProperties.SORTED_WRITE_TO_TEMP_PATH_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.TEMPORARY_STAGING_DIRECTORY_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.getInsertExistingPartitionsBehavior;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.PAGEFILE;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.getHiveTableProperty;
import static com.facebook.presto.hive.HiveUtil.columnExtraInfo;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialMergePushdownStrategy.PUSH_THROUGH_LOW_MEMORY_OPERATORS;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textLogicalPlan;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Files.asCharSink;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.PART_SUPPLIER;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.FileAssert.assertFile;

public class TestHiveIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final String catalog;
    private final Session bucketedSession;
    private final Session materializeExchangesSession;
    private final TypeTranslator typeTranslator;

    @SuppressWarnings("unused")
    public TestHiveIntegrationSmokeTest()
    {
        this(createBucketedSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))),
                createMaterializeExchangesSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))),
                HIVE_CATALOG,
                new HiveTypeTranslator());
    }

    protected TestHiveIntegrationSmokeTest(
            Session bucketedSession,
            Session materializeExchangesSession,
            String catalog,
            TypeTranslator typeTranslator)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.bucketedSession = requireNonNull(bucketedSession, "bucketSession is null");
        this.materializeExchangesSession = requireNonNull(materializeExchangesSession, "materializeExchangesSession is null");
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(ORDERS, CUSTOMER, LINE_ITEM, PART_SUPPLIER, NATION);
    }

    private List<?> getPartitions(HiveTableLayoutHandle tableLayoutHandle)
    {
        return tableLayoutHandle.getPartitions().get();
    }

    @Test
    public void testSchemaOperations()
    {
        Session admin = Session.builder(getQueryRunner().getDefaultSession())
                .setIdentity(new Identity(
                        "hive",
                        Optional.empty(),
                        ImmutableMap.of("hive", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("admin"))),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty()))
                .build();

        assertUpdate(admin, "CREATE SCHEMA new_schema");

        assertUpdate(admin, "CREATE TABLE new_schema.test (x bigint)");

        assertQueryFails(admin, "DROP SCHEMA new_schema", "Schema not empty: new_schema");

        assertUpdate(admin, "DROP TABLE new_schema.test");

        assertUpdate(admin, "DROP SCHEMA new_schema");
    }

    @Test
    public void testArrayPredicate()
    {
        Session admin = Session.builder(getQueryRunner().getDefaultSession())
                .setIdentity(new Identity(
                        "hive",
                        Optional.empty(),
                        ImmutableMap.of("hive", new SelectedRole(SelectedRole.Type.ROLE, Optional.of("admin"))),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty()))
                .build();

        assertUpdate(admin, "CREATE SCHEMA new_schema");

        assertUpdate(admin, "CREATE TABLE new_schema.test (a array<varchar>)");

        assertUpdate(admin, "INSERT INTO new_schema.test (values array['hi'])", 1);

        assertQuery(admin, "SELECT * FROM new_schema.test where a <> array[]", "SELECT 'hi'");

        assertUpdate(admin, "DROP TABLE new_schema.test");

        assertUpdate(admin, "DROP SCHEMA new_schema");
    }

    @Test
    public void testCreateTableWithInvalidProperties()
    {
        // CSV
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 bigint) WITH (format = 'ORC', csv_separator = 'S')"))
                .hasMessageMatching("Cannot specify csv_separator table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'CSV', csv_separator = 'SS')"))
                .hasMessageMatching("csv_separator must be a single character string, but was: 'SS'");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 bigint) WITH (format = 'ORC', csv_quote = 'Q')"))
                .hasMessageMatching("Cannot specify csv_quote table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'CSV', csv_quote = 'QQ')"))
                .hasMessageMatching("csv_quote must be a single character string, but was: 'QQ'");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'ORC', csv_escape = 'E')"))
                .hasMessageMatching("Cannot specify csv_escape table property for storage format: ORC");
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE invalid_table (col1 varchar) WITH (format = 'CSV', csv_escape = 'EE')"))
                .hasMessageMatching("csv_escape must be a single character string, but was: 'EE'");
    }

    @Test
    public void testIOExplain()
    {
        // Test IO explain with small number of discrete components.
        computeActual("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['orderkey', 'processing']) AS SELECT custkey, orderkey, orderstatus = 'P' processing FROM orders WHERE orderkey < 3");

        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_orders SELECT custkey, orderkey, processing FROM test_orders where custkey <= 10");
        ImmutableSet.Builder<ColumnConstraint> expectedConstraints = ImmutableSet.builder();
        expectedConstraints.add(new ColumnConstraint(
                "orderkey",
                BIGINT.getTypeSignature(),
                new FormattedDomain(
                        false,
                        ImmutableSet.of(
                                new FormattedRange(
                                        new FormattedMarker(Optional.of("1"), EXACTLY),
                                        new FormattedMarker(Optional.of("1"), EXACTLY)),
                                new FormattedRange(
                                        new FormattedMarker(Optional.of("2"), EXACTLY),
                                        new FormattedMarker(Optional.of("2"), EXACTLY))))));
        expectedConstraints.add(new ColumnConstraint(
                "processing",
                BOOLEAN.getTypeSignature(),
                new FormattedDomain(
                        false,
                        ImmutableSet.of(
                                new FormattedRange(
                                        new FormattedMarker(Optional.of("false"), EXACTLY),
                                        new FormattedMarker(Optional.of("false"), EXACTLY))))));

        assertEquals(
                jsonCodec(IOPlan.class).fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IOPlan(
                        ImmutableSet.of(new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "test_orders"),
                                expectedConstraints.build())),
                        Optional.of(new CatalogSchemaTableName(catalog, "tpch", "test_orders"))));

        assertUpdate("DROP TABLE test_orders");

        // Test IO explain with large number of discrete components where Domain::simplify comes into play.
        computeActual("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['orderkey']) AS select custkey, orderkey FROM orders where orderkey < 200");

        expectedConstraints = ImmutableSet.builder();
        expectedConstraints.add(new ColumnConstraint(
                "orderkey",
                BIGINT.getTypeSignature(),
                new FormattedDomain(
                        false,
                        ImmutableSet.of(
                                new FormattedRange(
                                        new FormattedMarker(Optional.of("1"), EXACTLY),
                                        new FormattedMarker(Optional.of("199"), EXACTLY))))));

        result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_orders SELECT custkey, orderkey + 10 FROM test_orders where custkey <= 10");
        assertEquals(
                jsonCodec(IOPlan.class).fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IOPlan(
                        ImmutableSet.of(new TableColumnInfo(
                                new CatalogSchemaTableName(catalog, "tpch", "test_orders"),
                                expectedConstraints.build())),
                        Optional.of(new CatalogSchemaTableName(catalog, "tpch", "test_orders"))));

        assertUpdate("DROP TABLE test_orders");
    }

    @Test
    public void testReadNoColumns()
    {
        testWithAllStorageFormats(this::testReadNoColumns);
    }

    private void testReadNoColumns(Session session, HiveStorageFormat storageFormat)
    {
        if (!insertOperationsSupported(storageFormat)) {
            return;
        }
        assertUpdate(session, format("CREATE TABLE test_read_no_columns WITH (format = '%s') AS SELECT 0 x", storageFormat), 1);
        assertQuery(session, "SELECT count(*) FROM test_read_no_columns", "SELECT 1");
        assertUpdate(session, "DROP TABLE test_read_no_columns");
    }

    @Test
    public void createTableWithEveryType()
    {
        @Language("SQL") String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast('bar' as varbinary) _varbinary" +
                ", cast(1 as bigint) _bigint" +
                ", 2 _integer" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", CAST('bar' AS CHAR(10)) _char";

        assertUpdate(query, 1);

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_types_table").toTestTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 2);
        assertEquals(row.getField(4), 3.14);
        assertEquals(row.getField(5), true);
        assertEquals(row.getField(6), LocalDate.of(1980, 5, 7));
        assertEquals(row.getField(7), LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertEquals(row.getField(8), new BigDecimal("3.14"));
        assertEquals(row.getField(9), new BigDecimal("12345678901234567890.0123456789"));
        assertEquals(row.getField(10), "bar       ");
        assertUpdate("DROP TABLE test_types_table");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_types_table"));
    }

    @Test
    public void testCreatePartitionedTable()
    {
        testWithAllStorageFormats(this::testCreatePartitionedTable);
    }

    private void testCreatePartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        if (!insertOperationsSupported(storageFormat)) {
            return;
        }

        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table (" +
                "  _string VARCHAR" +
                ",  _varchar VARCHAR(65535)" +
                ", _char CHAR(10)" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _smallint SMALLINT" +
                ", _tinyint TINYINT" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ", _decimal_short DECIMAL(3,2)" +
                ", _decimal_long DECIMAL(30,10)" +
                ", _partition_string VARCHAR" +
                ", _partition_varchar VARCHAR(65535)" +
                ", _partition_char CHAR(10)" +
                ", _partition_tinyint TINYINT" +
                ", _partition_smallint SMALLINT" +
                ", _partition_integer INTEGER" +
                ", _partition_bigint BIGINT" +
                ", _partition_boolean BOOLEAN" +
                ", _partition_decimal_short DECIMAL(3,2)" +
                ", _partition_decimal_long DECIMAL(30,10)" +
                ", _partition_date DATE" +
                ", _partition_timestamp TIMESTAMP" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ '_partition_string', '_partition_varchar', '_partition_char', '_partition_tinyint', '_partition_smallint', '_partition_integer', '_partition_bigint', '_partition_boolean', '_partition_decimal_short', '_partition_decimal_long', '_partition_date', '_partition_timestamp']" +
                ") ";

        if (storageFormat == HiveStorageFormat.AVRO) {
            createTable = createTable.replace(" _smallint SMALLINT,", " _smallint INTEGER,");
            createTable = createTable.replace(" _tinyint TINYINT,", " _tinyint INTEGER,");
        }

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> partitionedBy = ImmutableList.of(
                "_partition_string",
                "_partition_varchar",
                "_partition_char",
                "_partition_tinyint",
                "_partition_smallint",
                "_partition_integer",
                "_partition_bigint",
                "_partition_boolean",
                "_partition_decimal_short",
                "_partition_decimal_long",
                "_partition_date",
                "_partition_timestamp");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            boolean partitionKey = partitionedBy.contains(columnMetadata.getName());
            assertEquals(columnMetadata.getExtraInfo().orElse(null), columnExtraInfo(partitionKey));
        }

        assertColumnType(tableMetadata, "_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_varchar", createVarcharType(65535));
        assertColumnType(tableMetadata, "_char", createCharType(10));
        assertColumnType(tableMetadata, "_partition_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_partition_varchar", createVarcharType(65535));

        MaterializedResult result = computeActual("SELECT * from test_partitioned_table");
        assertEquals(result.getRowCount(), 0);

        @Language("SQL") String select = "" +
                "SELECT" +
                " 'foo' _string" +
                ", 'bar' _varchar" +
                ", CAST('boo' AS CHAR(10)) _char" +
                ", CAST(1 AS BIGINT) _bigint" +
                ", 2 _integer" +
                ", CAST (3 AS SMALLINT) _smallint" +
                ", CAST (4 AS TINYINT) _tinyint" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", 'foo' _partition_string" +
                ", 'bar' _partition_varchar" +
                ", CAST('boo' AS CHAR(10)) _partition_char" +
                ", CAST(1 AS TINYINT) _partition_tinyint" +
                ", CAST(1 AS SMALLINT) _partition_smallint" +
                ", 1 _partition_integer" +
                ", CAST (1 AS BIGINT) _partition_bigint" +
                ", true _partition_boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _partition_decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _partition_decimal_long" +
                ", CAST('2017-05-01' AS DATE) _partition_date" +
                ", CAST('2017-05-01 10:12:34' AS TIMESTAMP) _partition_timestamp";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (3 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (4 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        assertUpdate(session, "INSERT INTO test_partitioned_table " + select, 1);
        assertQuery(session, "SELECT * from test_partitioned_table", select);
        assertQuery(session,
                "SELECT * from test_partitioned_table WHERE" +
                        " 'foo' = _partition_string" +
                        " AND 'bar' = _partition_varchar" +
                        " AND CAST('boo' AS CHAR(10)) = _partition_char" +
                        " AND CAST(1 AS TINYINT) = _partition_tinyint" +
                        " AND CAST(1 AS SMALLINT) = _partition_smallint" +
                        " AND 1 = _partition_integer" +
                        " AND CAST(1 AS BIGINT) = _partition_bigint" +
                        " AND true = _partition_boolean" +
                        " AND CAST('3.14' AS DECIMAL(3,2)) = _partition_decimal_short" +
                        " AND CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = _partition_decimal_long" +
                        " AND CAST('2017-05-01' AS DATE) = _partition_date" +
                        " AND CAST('2017-05-01 10:12:34' AS TIMESTAMP) = _partition_timestamp",
                select);

        assertUpdate(session, "DROP TABLE test_partitioned_table");

        assertFalse(getQueryRunner().tableExists(session, "test_partitioned_table"));
    }

    @Test
    public void createTableLike()
    {
        createTableLike("", false);
        createTableLike("EXCLUDING PROPERTIES", false);
        createTableLike("INCLUDING PROPERTIES", true);
    }

    private void createTableLike(String likeSuffix, boolean hasPartition)
    {
        // Create a non-partitioned table
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_table_original (" +
                "  tinyint_col tinyint " +
                ", smallint_col smallint" +
                ")";
        assertUpdate(createTable);

        // Verify the table is correctly created
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_table_original");
        assertColumnType(tableMetadata, "tinyint_col", TINYINT);
        assertColumnType(tableMetadata, "smallint_col", SMALLINT);

        // Create a partitioned table
        @Language("SQL") String createPartitionedTable = "" +
                "CREATE TABLE test_partitioned_table_original (" +
                "  string_col VARCHAR" +
                ", decimal_long_col DECIMAL(30,10)" +
                ", partition_bigint BIGINT" +
                ", partition_decimal_long DECIMAL(30,10)" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY['partition_bigint', 'partition_decimal_long']" +
                ")";
        assertUpdate(createPartitionedTable);

        // Verify the table is correctly created
        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_original");

        // Verify the partition keys are correctly created
        List<String> partitionedBy = ImmutableList.of("partition_bigint", "partition_decimal_long");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        // Create a table using only one LIKE
        @Language("SQL") String createTableSingleLike = "" +
                "CREATE TABLE test_partitioned_table_single_like (" +
                "LIKE test_partitioned_table_original " + likeSuffix +
                ")";
        assertUpdate(createTableSingleLike);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_single_like");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        @Language("SQL") String createTableLikeExtra = "" +
                "CREATE TABLE test_partitioned_table_like_extra (" +
                "  bigint_col BIGINT" +
                ", double_col DOUBLE" +
                ", LIKE test_partitioned_table_single_like " + likeSuffix +
                ")";
        assertUpdate(createTableLikeExtra);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_like_extra");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "bigint_col", BIGINT);
        assertColumnType(tableMetadata, "double_col", DOUBLE);
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        @Language("SQL") String createTableDoubleLike = "" +
                "CREATE TABLE test_partitioned_table_double_like (" +
                "  LIKE test_table_original " +
                ", LIKE test_partitioned_table_like_extra " + likeSuffix +
                ")";
        assertUpdate(createTableDoubleLike);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_double_like");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "tinyint_col", TINYINT);
        assertColumnType(tableMetadata, "smallint_col", SMALLINT);
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        assertUpdate("DROP TABLE test_table_original");
        assertUpdate("DROP TABLE test_partitioned_table_original");
        assertUpdate("DROP TABLE test_partitioned_table_single_like");
        assertUpdate("DROP TABLE test_partitioned_table_like_extra");
        assertUpdate("DROP TABLE test_partitioned_table_double_like");
    }

    @Test
    public void testCreateTableAs()
    {
        testWithAllStorageFormats(this::testCreateTableAs);
    }

    private void testCreateTableAs(Session session, HiveStorageFormat storageFormat)
    {
        if (!insertOperationsSupported(storageFormat)) {
            return;
        }

        @Language("SQL") String select = "SELECT" +
                " 'foo' _varchar" +
                ", CAST('bar' AS CHAR(10)) _char" +
                ", CAST (1 AS BIGINT) _bigint" +
                ", 2 _integer" +
                ", CAST (3 AS SMALLINT) _smallint" +
                ", CAST (4 AS TINYINT) _tinyint" +
                ", CAST ('123.45' as REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (3 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (4 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        String createTableAs = format("CREATE TABLE test_format_table WITH (format = '%s') AS %s", storageFormat, select);

        assertUpdate(session, createTableAs, 1);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertColumnType(tableMetadata, "_varchar", createVarcharType(3));
        assertColumnType(tableMetadata, "_char", createCharType(10));

        // assure reader supports basic column reordering and pruning
        assertQuery(session, "SELECT _integer, _varchar, _integer from test_format_table", "SELECT 2, 'foo', 2");

        assertQuery(session, "SELECT * from test_format_table", select);

        assertUpdate(session, "DROP TABLE test_format_table");

        assertFalse(getQueryRunner().tableExists(session, "test_format_table"));
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        testWithAllStorageFormats(this::testCreatePartitionedTableAs);
    }

    private void testCreatePartitionedTableAs(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(session, createTable, "SELECT count(*) from orders");

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_create_partitioned_table_as");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        List<?> partitions = getPartitions("test_create_partitioned_table_as");
        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * from test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE test_create_partitioned_table_as");

        assertFalse(getQueryRunner().tableExists(session, "test_create_partitioned_table_as"));
    }

    @Test
    public void testInlineRecursiveSqlFunctions()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(INLINE_SQL_FUNCTIONS, "false")
                .build();

        @Language("SQL") String createTable = "create table if not exists test_array_temp as select sequence(1, 10) x";
        @Language("SQL") String queryTable = "SELECT\n" +
                "\"array_sum\"(\"transform\"(x, (y) -> \"cardinality\"(x)))\n" +
                ", \"array_sum\"(\"transform\"(x, (y) -> \"array_sum\"(\"transform\"(x, (check) -> IF((NOT random(10) > 10), 1, 0)))))\n" +
                "FROM\n" +
                "test_array_temp";

        assertUpdate(session, createTable, 1);
        assertQuerySucceeds(session, queryTable);
    }

    @Test
    public void testCreatePartitionedTableAsShuffleOnPartitionColumns()
    {
        testCreatePartitionedTableAsShuffleOnPartitionColumns(
                Session.builder(getSession())
                        .setSystemProperty("task_writer_count", "1")
                        .setCatalogSessionProperty(catalog, "shuffle_partitioned_columns_for_table_write", "true")
                        .build(),
                ORC);
    }

    private void testCreatePartitionedTableAsShuffleOnPartitionColumns(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as_shuffle_on_partition_columns " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(session, createTable, "SELECT count(*) from orders");

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_create_partitioned_table_as_shuffle_on_partition_columns");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        List<?> partitions = getPartitions("test_create_partitioned_table_as_shuffle_on_partition_columns");
        assertEquals(partitions.size(), 3);

        assertQuery(
                session,
                "SELECT count(distinct \"$path\") from test_create_partitioned_table_as_shuffle_on_partition_columns",
                "SELECT 3");

        assertQuery(session, "SELECT * from test_create_partitioned_table_as_shuffle_on_partition_columns", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE test_create_partitioned_table_as_shuffle_on_partition_columns");

        assertFalse(getQueryRunner().tableExists(session, "test_create_partitioned_table_as_shuffle_on_partition_columns"));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition keys must be the last columns in the table and in the same order as the table properties.*")
    public void testCreatePartitionedTableInvalidColumnOrdering()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_invalid_column_ordering\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['apple'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition keys must be the last columns in the table and in the same order as the table properties.*")
    public void testCreatePartitionedTableAsInvalidColumnOrdering()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_as_invalid_column_ordering " +
                "WITH (partitioned_by = ARRAY['SHIP_PRIORITY', 'ORDER_STATUS']) " +
                "AS " +
                "SELECT shippriority AS ship_priority, orderkey AS order_key, orderstatus AS order_status " +
                "FROM tpch.tiny.orders");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Table contains only partition columns")
    public void testCreateTableOnlyPartitionColumns()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_only_partition_columns\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['grape', 'apple', 'orange', 'pear'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition columns .* not present in schema")
    public void testCreateTableNonExistentPartitionColumns()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_nonexistent_partition_columns\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['dragonfruit'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported type .* for partition: .*")
    public void testCreateTableUnsupportedPartitionType()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_unsupported_partition_type " +
                "(foo bigint, bar ARRAY(varchar)) " +
                "WITH (partitioned_by = ARRAY['bar'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported type .* for partition: a")
    public void testCreateTableUnsupportedPartitionTypeAs()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_unsupported_partition_type_as " +
                "WITH (partitioned_by = ARRAY['a']) " +
                "AS " +
                "SELECT 123 x, ARRAY ['foo'] a");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported Hive type: varchar\\(65536\\)\\. Supported VARCHAR types: VARCHAR\\(<=65535\\), VARCHAR\\.")
    public void testCreateTableNonSupportedVarcharColumn()
    {
        assertUpdate("CREATE TABLE test_create_table_non_supported_varchar_column (apple varchar(65536))");
    }

    @Test
    public void testCreatePartitionedBucketedTableAsFewRows()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats((session, format) -> testCreatePartitionedBucketedTableAsFewRows(session, format, false, false));
        testWithAllStorageFormats((session, format) -> testCreatePartitionedBucketedTableAsFewRows(session, format, false, true));
        // test with optimized PartitionUpdate serialization
        testWithAllStorageFormats((session, format) -> testCreatePartitionedBucketedTableAsFewRows(session, format, true, false));
        testWithAllStorageFormats((session, format) -> testCreatePartitionedBucketedTableAsFewRows(session, format, true, true));
    }

    private void testCreatePartitionedBucketedTableAsFewRows(
            Session session,
            HiveStorageFormat storageFormat,
            boolean optimizedPartitionUpdateSerializationEnabled,
            boolean createEmpty)
    {
        String tableName = "test_create_partitioned_bucketed_table_as_few_rows";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'partition_key' ], " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT * " +
                "FROM (" +
                "VALUES " +
                "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                "  ('aa', 'bb', 'cc'), " +
                "  ('aaa', 'bbb', 'ccc')" +
                ") t(bucket_key, col, partition_key)";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                Session.builder(getTableWriteTestingSession(optimizedPartitionUpdateSerializationEnabled))
                        .setCatalogSessionProperty(catalog, "create_empty_bucket_files", String.valueOf(createEmpty))
                        .build(),
                createTable,
                3);

        verifyPartitionedBucketedTableAsFewRows(storageFormat, tableName);

        assertThatThrownBy(() -> assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('a0', 'b0', 'c')", 1))
                .hasMessage(getExpectedErrorMessageForInsertExistingBucketedTable(
                        getInsertExistingPartitionsBehavior(getConnectorSession(session)),
                        "partition_key=c"));

        assertUpdate(session, "DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableAs()
    {
        testCreatePartitionedBucketedTableAs(HiveStorageFormat.RCBINARY, false);
        testCreatePartitionedBucketedTableAs(HiveStorageFormat.RCBINARY, true);
    }

    private void testCreatePartitionedBucketedTableAs(HiveStorageFormat storageFormat, boolean optimizedPartitionUpdateSerializationEnabled)
    {
        String tableName = "test_create_partitioned_bucketed_table_as";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getTableWriteTestingSession(optimizedPartitionUpdateSerializationEnabled),
                createTable,
                "SELECT count(*) from orders");

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableAsWithUnionAll()
    {
        testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat.RCBINARY, false);
        testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat.RCBINARY, true);
    }

    private void testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat storageFormat, boolean optimizedPartitionUpdateSerializationEnabled)
    {
        String tableName = "test_create_partitioned_bucketed_table_as_with_union_all";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders " +
                "WHERE length(comment) % 2 = 0 " +
                "UNION ALL " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders " +
                "WHERE length(comment) % 2 = 1";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getTableWriteTestingSession(optimizedPartitionUpdateSerializationEnabled),
                createTable,
                "SELECT count(*) from orders");

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    private void verifyPartitionedBucketedTable(HiveStorageFormat storageFormat, String tableName)
    {
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("orderstatus"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey", "custkey2"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        assertQuery("SELECT * from " + tableName, "SELECT custkey, custkey, comment, orderstatus FROM orders");

        for (int i = 1; i <= 30; i++) {
            assertQuery(
                    format("SELECT * from " + tableName + " where custkey = %d and custkey2 = %d", i, i),
                    format("SELECT custkey, custkey, comment, orderstatus FROM orders where custkey = %d", i));
        }

        assertThatThrownBy(() -> assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1, 'comment', 'O')", 1))
                .hasMessage(getExpectedErrorMessageForInsertExistingBucketedTable(
                        getInsertExistingPartitionsBehavior(getConnectorSession(getSession())),
                        "orderstatus=O"));
    }

    @Test
    public void testCreateInvalidBucketedTable()
    {
        testCreateInvalidBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testCreateInvalidBucketedTable(HiveStorageFormat storageFormat)
    {
        String tableName = "test_create_invalid_bucketed_table";

        try {
            computeActual("" +
                    "CREATE TABLE " + tableName + " (" +
                    "  a BIGINT," +
                    "  b DOUBLE," +
                    "  p VARCHAR" +
                    ") WITH (" +
                    "format = '" + storageFormat + "', " +
                    "partitioned_by = ARRAY[ 'p' ], " +
                    "bucketed_by = ARRAY[ 'a', 'c' ], " +
                    "bucket_count = 11 " +
                    ")");
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Bucketing columns [c] not present in schema");
        }

        try {
            computeActual("" +
                    "CREATE TABLE " + tableName + " " +
                    "WITH (" +
                    "format = '" + storageFormat + "', " +
                    "partitioned_by = ARRAY[ 'orderstatus' ], " +
                    "bucketed_by = ARRAY[ 'custkey', 'custkey3' ], " +
                    "bucket_count = 11 " +
                    ") " +
                    "AS " +
                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                    "FROM tpch.tiny.orders");
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Bucketing columns [custkey3] not present in schema");
        }

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testCreatePartitionedUnionAll()
    {
        assertUpdate("CREATE TABLE test_create_partitioned_union_all (a varchar, ds varchar) WITH (partitioned_by = ARRAY['ds'])");
        assertUpdate("INSERT INTO test_create_partitioned_union_all SELECT 'a', '2013-05-17' UNION ALL SELECT 'b', '2013-05-17'", 2);
        assertUpdate("DROP TABLE test_create_partitioned_union_all");
    }

    @Test
    public void testInsertPartitionedBucketedTableFewRows()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        testWithAllStorageFormats((session, format) -> testInsertPartitionedBucketedTableFewRows(session, format, false));
        // test with optimized PartitionUpdate serialization
        testWithAllStorageFormats((session, format) -> testInsertPartitionedBucketedTableFewRows(session, format, true));
    }

    private void testInsertPartitionedBucketedTableFewRows(Session session, HiveStorageFormat storageFormat, boolean optimizedPartitionUpdateSerializationEnabled)
    {
        String tableName = "test_insert_partitioned_bucketed_table_few_rows";

        assertUpdate(session, "" +
                "CREATE TABLE " + tableName + " (" +
                "  bucket_key varchar," +
                "  col varchar," +
                "  partition_key varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'partition_key' ], " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11)");

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getTableWriteTestingSession(optimizedPartitionUpdateSerializationEnabled),
                "INSERT INTO " + tableName + " " +
                        "VALUES " +
                        "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                        "  ('aa', 'bb', 'cc'), " +
                        "  ('aaa', 'bbb', 'ccc')",
                3);

        verifyPartitionedBucketedTableAsFewRows(storageFormat, tableName);

        assertThatThrownBy(() -> assertUpdate(session, "INSERT INTO test_insert_partitioned_bucketed_table_few_rows VALUES ('a0', 'b0', 'c')", 1))
                .hasMessage(getExpectedErrorMessageForInsertExistingBucketedTable(
                        getInsertExistingPartitionsBehavior(getConnectorSession(session)),
                        "partition_key=c"));

        assertUpdate(session, "DROP TABLE test_insert_partitioned_bucketed_table_few_rows");
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    private void verifyPartitionedBucketedTableAsFewRows(HiveStorageFormat storageFormat, String tableName)
    {
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("partition_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("bucket_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        MaterializedResult actual = computeActual("SELECT * from " + tableName);
        MaterializedResult expected = resultBuilder(getSession(), canonicalizeType(createUnboundedVarcharType()), canonicalizeType(createUnboundedVarcharType()), canonicalizeType(createUnboundedVarcharType()))
                .row("a", "b", "c")
                .row("aa", "bb", "cc")
                .row("aaa", "bbb", "ccc")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testCastNullToColumnTypes()
    {
        String tableName = "test_cast_null_to_column_types";
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "orc_optimized_writer_enabled", "true")
                .build();

        assertUpdate(session, "" +
                "CREATE TABLE " + tableName + " (" +
                "  col1 bigint," +
                "  col2 map(bigint, bigint)," +
                "  partition_key varchar)" +
                "WITH (" +
                "  format = 'ORC', " +
                "  partitioned_by = ARRAY[ 'partition_key' ] " +
                ")");

        assertUpdate(session, format("INSERT INTO %s (col1) VALUES (1), (2), (3)", tableName), 3);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateEmptyNonBucketedPartition()
    {
        testCreateEmptyNonBucketedPartition(false);
        testCreateEmptyNonBucketedPartition(true);
    }

    public void testCreateEmptyNonBucketedPartition(boolean optimizedPartitionUpdateSerializationEnabled)
    {
        String tableName = "test_insert_empty_partitioned_unbucketed_table";
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  dummy_col bigint," +
                "  part varchar)" +
                "WITH (" +
                "  format = 'ORC', " +
                "  partitioned_by = ARRAY[ 'part' ] " +
                ")");
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 0");

        // create an empty partition
        assertUpdate(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, OPTIMIZED_PARTITION_UPDATE_SERIALIZATION_ENABLED, optimizedPartitionUpdateSerializationEnabled + "")
                        .build(),
                format("CALL system.create_empty_partition('%s', '%s', ARRAY['part'], ARRAY['%s'])", TPCH_SCHEMA, tableName, "empty"));
        assertQuery(format("SELECT count(*) FROM \"%s$partitions\"", tableName), "SELECT 1");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateEmptyUnpartitionedBucketedTable()
    {
        String tableName = "test_create_empty_bucketed_table";
        assertUpdate("" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "   bucketed_by = ARRAY[ 'custkey' ], " +
                "   bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, comment " +
                "FROM customer " +
                "WHERE custkey < 0", 0);
        assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateEmptyUnpartitionedBucketedTableNoStaging()
    {
        String tableName = "test_create_empty_bucketed_table_no_staging";
        assertUpdate(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, TEMPORARY_STAGING_DIRECTORY_ENABLED, "false")
                        .build(),
                "" +
                        "CREATE TABLE " + tableName + " " +
                        "WITH (" +
                        "   bucketed_by = ARRAY[ 'custkey' ], " +
                        "   bucket_count = 11 " +
                        ") " +
                        "AS " +
                        "SELECT custkey, comment " +
                        "FROM customer " +
                        "WHERE custkey < 0", 0);
        assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateEmptyBucketedPartition()
    {
        for (HiveStorageFormat storageFormat : getSupportedHiveStorageFormats()) {
            testCreateEmptyBucketedPartition(storageFormat, false);
            testCreateEmptyBucketedPartition(storageFormat, true);
        }
    }

    public void testCreateEmptyBucketedPartition(HiveStorageFormat storageFormat, boolean optimizedPartitionUpdateSerializationEnabled)
    {
        String tableName = "test_insert_empty_partitioned_bucketed_table";
        createPartitionedBucketedTable(tableName, storageFormat);

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String sql = format("CALL system.create_empty_partition('%s', '%s', ARRAY['orderstatus'], ARRAY['%s'])", TPCH_SCHEMA, tableName, orderStatusList.get(i));
            assertUpdate(
                    Session.builder(getSession())
                            .setCatalogSessionProperty(catalog, OPTIMIZED_PARTITION_UPDATE_SERIALIZATION_ENABLED, optimizedPartitionUpdateSerializationEnabled + "")
                            .build(),
                    sql);
            assertQuery(
                    format("SELECT count(*) FROM \"%s$partitions\"", tableName),
                    "SELECT " + (i + 1));

            assertQueryFails(sql, "Partition already exists.*");
        }

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsertPartitionedBucketedTable()
    {
        testInsertPartitionedBucketedTable(HiveStorageFormat.RCBINARY, false);
        testInsertPartitionedBucketedTable(HiveStorageFormat.RCBINARY, true);
    }

    private void testInsertPartitionedBucketedTable(HiveStorageFormat storageFormat, boolean optimizedPartitionUpdateSerializationEnabled)
    {
        String tableName = "test_insert_partitioned_bucketed_table";
        createPartitionedBucketedTable(tableName, storageFormat);

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String orderStatus = orderStatusList.get(i);
            assertUpdate(
                    // make sure that we will get one file per bucket regardless of writer count configured
                    getTableWriteTestingSession(optimizedPartitionUpdateSerializationEnabled),
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s'",
                            orderStatus),
                    format("SELECT count(*) from orders where orderstatus = '%s'", orderStatus));
        }

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    private void createPartitionedBucketedTable(String tableName, HiveStorageFormat storageFormat)
    {
        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  custkey bigint," +
                "  custkey2 bigint," +
                "  comment varchar," +
                "  orderstatus varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11)");
    }

    @Test
    public void testInsertPartitionedBucketedTableWithUnionAll()
    {
        testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat.RCBINARY, false);
        testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat.RCBINARY, true);
    }

    private void testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat storageFormat, boolean optimizedPartitionUpdateSerializationEnabled)
    {
        String tableName = "test_insert_partitioned_bucketed_table_with_union_all";

        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  custkey bigint," +
                "  custkey2 bigint," +
                "  comment varchar," +
                "  orderstatus varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11)");

        List<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String orderStatus = orderStatusList.get(i);
            assertUpdate(
                    // make sure that we will get one file per bucket regardless of writer count configured
                    getTableWriteTestingSession(optimizedPartitionUpdateSerializationEnabled),
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s' and length(comment) %% 2 = 0 " +
                                    "UNION ALL " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s' and length(comment) %% 2 = 1",
                            orderStatus, orderStatus),
                    format("SELECT count(*) from orders where orderstatus = '%s'", orderStatus));
        }

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsert()
    {
        testWithAllStorageFormats(this::testInsert);
    }

    private void testInsert(Session session, HiveStorageFormat storageFormat)
    {
        if (!insertOperationsSupported(storageFormat)) {
            return;
        }
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_format_table " +
                "(" +
                "  _string VARCHAR," +
                "  _varchar VARCHAR(65535)," +
                "  _char CHAR(10)," +
                "  _bigint BIGINT," +
                "  _integer INTEGER," +
                "  _smallint SMALLINT," +
                "  _tinyint TINYINT," +
                "  _real REAL," +
                "  _double DOUBLE," +
                "  _boolean BOOLEAN," +
                "  _decimal_short DECIMAL(3,2)," +
                "  _decimal_long DECIMAL(30,10)" +
                ") " +
                "WITH (format = '" + storageFormat + "') ";

        if (storageFormat == HiveStorageFormat.AVRO) {
            createTable = createTable.replace(" _smallint SMALLINT,", " _smallint INTEGER,");
            createTable = createTable.replace(" _tinyint TINYINT,", " _tinyint INTEGER,");
        }

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_insert_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertColumnType(tableMetadata, "_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_varchar", createVarcharType(65535));
        assertColumnType(tableMetadata, "_char", createCharType(10));

        @Language("SQL") String select = "SELECT" +
                " 'foo' _string" +
                ", 'bar' _varchar" +
                ", CAST('boo' AS CHAR(10)) _char" +
                ", 1 _bigint" +
                ", CAST(42 AS INTEGER) _integer" +
                ", CAST(43 AS SMALLINT) _smallint" +
                ", CAST(44 AS TINYINT) _tinyint" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (43 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (44 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        assertUpdate(session, "INSERT INTO test_insert_format_table " + select, 1);

        assertQuery(session, "SELECT * from test_insert_format_table", select);

        assertUpdate(session, "INSERT INTO test_insert_format_table (_tinyint, _smallint, _integer, _bigint, _real, _double) SELECT CAST(1 AS TINYINT), CAST(2 AS SMALLINT), 3, 4, cast(14.3E0 as REAL), 14.3E0", 1);

        assertQuery(session, "SELECT * from test_insert_format_table where _bigint = 4", "SELECT null, null, null, 4, 3, 2, 1, 14.3, 14.3, null, null, null");

        assertQuery(session, "SELECT * from test_insert_format_table where _real = CAST(14.3 as REAL)", "SELECT null, null, null, 4, 3, 2, 1, 14.3, 14.3, null, null, null");

        assertUpdate(session, "INSERT INTO test_insert_format_table (_double, _bigint) SELECT 2.72E0, 3", 1);

        assertQuery(session, "SELECT * from test_insert_format_table where _bigint = 3", "SELECT null, null, null, 3, null, null, null, null, 2.72, null, null, null");

        assertUpdate(session, "INSERT INTO test_insert_format_table (_decimal_short, _decimal_long) SELECT DECIMAL '2.72', DECIMAL '98765432101234567890.0123456789'", 1);

        assertQuery(session, "SELECT * from test_insert_format_table where _decimal_long = DECIMAL '98765432101234567890.0123456789'", "SELECT null, null, null, null, null, null, null, null, null, null, 2.72, 98765432101234567890.0123456789");

        assertUpdate(session, "DROP TABLE test_insert_format_table");

        assertFalse(getQueryRunner().tableExists(session, "test_insert_format_table"));
    }

    @Test
    public void testInsertPartitionedTable()
    {
        testWithAllStorageFormats(this::testInsertPartitionedTable);
    }

    private void testInsertPartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_partitioned_table " +
                "(" +
                "  ORDER_KEY BIGINT," +
                "  SHIP_PRIORITY INTEGER," +
                "  ORDER_STATUS VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_insert_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        String partitionsTable = "\"test_insert_partitioned_table$partitions\"";

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "SELECT shippriority, orderstatus FROM orders LIMIT 0");

        // Hive will reorder the partition keys, so we must insert into the table assuming the partition keys have been moved to the end
        assertUpdate(
                session,
                "" +
                        "INSERT INTO test_insert_partitioned_table " +
                        "SELECT orderkey, shippriority, orderstatus " +
                        "FROM tpch.tiny.orders",
                "SELECT count(*) from orders");

        // verify the partitions
        List<?> partitions = getPartitions("test_insert_partitioned_table");
        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * from test_insert_partitioned_table", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "SELECT DISTINCT shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " ORDER BY order_status LIMIT 2",
                "SELECT DISTINCT shippriority, orderstatus FROM orders ORDER BY orderstatus LIMIT 2");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE order_status = 'O'",
                "SELECT DISTINCT shippriority, orderstatus FROM orders WHERE orderstatus = 'O'");

        assertQueryFails(session, "SELECT * FROM " + partitionsTable + " WHERE no_such_column = 1", "line \\S*: Column 'no_such_column' cannot be resolved");
        assertQueryFails(session, "SELECT * FROM " + partitionsTable + " WHERE orderkey = 1", "line \\S*: Column 'orderkey' cannot be resolved");

        assertUpdate(session, "DROP TABLE test_insert_partitioned_table");

        assertFalse(getQueryRunner().tableExists(session, "test_insert_partitioned_table"));
    }

    @Test
    public void testInsertPartitionedTableShuffleOnPartitionColumns()
    {
        testInsertPartitionedTableShuffleOnPartitionColumns(
                Session.builder(getSession())
                        .setSystemProperty("task_writer_count", "1")
                        .setCatalogSessionProperty(catalog, "shuffle_partitioned_columns_for_table_write", "true")
                        .build(),
                ORC);
    }

    public void testInsertPartitionedTableShuffleOnPartitionColumns(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_table_shuffle_on_partition_columns";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'order_status' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("order_status"));

        assertUpdate(
                session,
                "INSERT INTO " + tableName + " " +
                        "SELECT orderkey, comment, orderstatus " +
                        "FROM tpch.tiny.orders",
                "SELECT count(*) from orders");

        // verify the partitions
        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        assertQuery(
                session,
                "SELECT * from " + tableName,
                "SELECT orderkey, comment, orderstatus FROM orders");

        assertQuery(
                session,
                "SELECT count(distinct \"$path\") from " + tableName,
                "SELECT 3");

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testInsertPartitionedTableExistingPartition()
    {
        testWithAllStorageFormats(this::testInsertPartitionedTableExistingPartition);
    }

    private void testInsertPartitionedTableExistingPartition(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_table_existing_partition";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'order_status' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("order_status"));

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) from orders where orderkey %% 3 = %d", i));
        }

        // verify the partitions
        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        assertQuery(
                session,
                "SELECT * from " + tableName,
                "SELECT orderkey, comment, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testInsertPartitionedTableOverwriteExistingPartition()
    {
        testInsertPartitionedTableOverwriteExistingPartition(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "insert_existing_partitions_behavior", "OVERWRITE")
                        .build(),
                ORC);
    }

    private void testInsertPartitionedTableOverwriteExistingPartition(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_table_overwrite_existing_partition";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'order_status' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("order_status"));

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) from orders where orderkey %% 3 = %d", i));

            // verify the partitions
            List<?> partitions = getPartitions(tableName);
            assertEquals(partitions.size(), 3);

            assertQuery(
                    session,
                    "SELECT * from " + tableName,
                    format("SELECT orderkey, comment, orderstatus FROM orders where orderkey %% 3 = %d", i));
        }
        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testInsertPartitionedTableImmutableExistingPartition()
    {
        testInsertPartitionedTableImmutableExistingPartition(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "insert_existing_partitions_behavior", "ERROR")
                        .build(),
                ORC);
        testInsertPartitionedTableImmutableExistingPartition(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "insert_existing_partitions_behavior", "ERROR")
                        .setCatalogSessionProperty(catalog, "temporary_staging_directory_enabled", "false")
                        .build(),
                ORC);
        testInsertPartitionedTableImmutableExistingPartition(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "insert_existing_partitions_behavior", "ERROR")
                        .setCatalogSessionProperty(catalog, "fail_fast_on_insert_into_immutable_partitions_enabled", "false")
                        .build(),
                ORC);
    }

    public void testInsertPartitionedTableImmutableExistingPartition(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_table_immutable_existing_partition";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'order_status' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("order_status"));

        assertUpdate(
                session,
                format(
                        "INSERT INTO " + tableName + " " +
                                "SELECT orderkey, comment, orderstatus " +
                                "FROM tpch.tiny.orders " +
                                "WHERE orderkey %% 3 = %d",
                        0),
                format("SELECT count(*) from orders where orderkey %% 3 = %d", 0));

        // verify the partitions
        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        assertQuery(
                session,
                "SELECT * from " + tableName,
                format("SELECT orderkey, comment, orderstatus FROM orders where orderkey %% 3 = %d", 0));

        assertQueryFails(
                session,
                format(
                        "INSERT INTO " + tableName + " " +
                                "SELECT orderkey, comment, orderstatus " +
                                "FROM tpch.tiny.orders " +
                                "WHERE orderkey %% 3 = %d",
                        0),
                ".*Cannot insert into an existing partition of Hive table.*");

        partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        assertQuery(
                session,
                "SELECT * from " + tableName,
                format("SELECT orderkey, comment, orderstatus FROM orders where orderkey %% 3 = %d", 0));

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testNullPartitionValues()
    {
        assertUpdate("" +
                "CREATE TABLE test_null_partition (test VARCHAR, part VARCHAR)\n" +
                "WITH (partitioned_by = ARRAY['part'])");

        assertUpdate("INSERT INTO test_null_partition VALUES ('hello', 'test'), ('world', null)", 2);

        assertQuery(
                "SELECT * FROM test_null_partition",
                "VALUES ('hello', 'test'), ('world', null)");

        assertQuery(
                "SELECT * FROM \"test_null_partition$partitions\"",
                "VALUES 'test', null");

        assertUpdate("DROP TABLE test_null_partition");
    }

    @Test
    public void testPartitionPerScanLimit()
    {
        testPartitionPerScanLimit(getSession(), DWRF);
    }

    private void testPartitionPerScanLimit(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_partition_per_scan_limit";
        String partitionsTable = "\"" + tableName + "$partitions\"";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  foo VARCHAR," +
                "  part BIGINT" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'part' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("part"));

        // insert 1200 partitions
        for (int i = 0; i < 12; i++) {
            int partStart = i * 100;
            int partEnd = (i + 1) * 100 - 1;

            @Language("SQL") String insertPartitions = "" +
                    "INSERT INTO " + tableName + " " +
                    "SELECT 'bar' foo, part " +
                    "FROM UNNEST(SEQUENCE(" + partStart + ", " + partEnd + ")) AS TMP(part)";

            assertUpdate(session, insertPartitions, 100);
        }

        // we are not constrained by hive.max-partitions-per-scan when listing partitions
        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE part > 490 and part <= 500",
                "VALUES 491, 492, 493, 494, 495, 496, 497, 498, 499, 500");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE part < 0",
                "SELECT null WHERE false");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "VALUES " + LongStream.range(0, 1200)
                        .mapToObj(String::valueOf)
                        .collect(joining(",")));

        // verify can query 1000 partitions
        assertQuery(
                session,
                "SELECT count(foo) FROM " + tableName + " WHERE part < 1000",
                "SELECT 1000");

        // verify the rest 200 partitions are successfully inserted
        assertQuery(
                session,
                "SELECT count(foo) FROM " + tableName + " WHERE part >= 1000 AND part < 1200",
                "SELECT 200");

        // verify cannot query more than 1000 partitions
        assertQueryFails(
                session,
                "SELECT * from " + tableName + " WHERE part < 1001",
                format("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName));

        // verify cannot query all partitions
        assertQueryFails(
                session,
                "SELECT * from " + tableName,
                format("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName));

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testShowColumnsFromPartitions()
    {
        String tableName = "test_show_columns_from_partitions";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  foo VARCHAR," +
                "  part1 BIGINT," +
                "  part2 VARCHAR" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'part1', 'part2' ]" +
                ") ";

        assertUpdate(getSession(), createTable);

        assertQuery(
                getSession(),
                "SHOW COLUMNS FROM \"" + tableName + "$partitions\"",
                "VALUES ('part1', 'bigint', '', '', 19L, null, null), ('part2', 'varchar', '', '', null, null, 2147483647L)");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"$partitions\"",
                ".*Table '.*\\.tpch\\.\\$partitions' does not exist");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"orders$partitions\"",
                ".*Table '.*\\.tpch\\.orders\\$partitions' does not exist");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"blah$partitions\"",
                ".*Table '.*\\.tpch\\.blah\\$partitions' does not exist");
    }

    @Test
    public void testPartitionsTableInvalidAccess()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitions_invalid " +
                "(" +
                "  foo VARCHAR," +
                "  part1 BIGINT," +
                "  part2 VARCHAR" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'part1', 'part2' ]" +
                ") ";

        assertUpdate(getSession(), createTable);

        assertQueryFails(
                getSession(),
                "SELECT * FROM \"test_partitions_invalid$partitions$partitions\"",
                ".*Table .*\\.tpch\\.test_partitions_invalid\\$partitions\\$partitions does not exist");

        assertQueryFails(
                getSession(),
                "SELECT * FROM \"non_existent$partitions\"",
                ".*Table .*\\.tpch\\.non_existent\\$partitions does not exist");
    }

    @Test
    public void testInsertUnpartitionedTable()
    {
        testWithAllStorageFormats(this::testInsertUnpartitionedTable);
    }

    private void testInsertUnpartitionedTable(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_unpartitioned_table";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "'" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) from orders where orderkey %% 3 = %d", i));
        }

        assertQuery(
                session,
                "SELECT * from " + tableName,
                "SELECT orderkey, comment, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testDeleteFromUnpartitionedTable()
    {
        assertUpdate("CREATE TABLE test_delete_unpartitioned AS SELECT orderstatus FROM tpch.tiny.orders", "SELECT count(*) from orders");

        assertUpdate("DELETE FROM test_delete_unpartitioned");

        MaterializedResult result = computeActual("SELECT * from test_delete_unpartitioned");
        assertEquals(result.getRowCount(), 0);

        assertUpdate("DROP TABLE test_delete_unpartitioned");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_delete_unpartitioned"));
    }

    @Test
    public void testMetadataDelete()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_metadata_delete " +
                "(" +
                "  ORDER_KEY BIGINT," +
                "  LINE_NUMBER INTEGER," +
                "  LINE_STATUS VARCHAR" +
                ") " +
                "WITH (" +
                PARTITIONED_BY_PROPERTY + " = ARRAY[ 'LINE_NUMBER', 'LINE_STATUS' ]" +
                ") ";

        assertUpdate(createTable);

        assertUpdate("" +
                        "INSERT INTO test_metadata_delete " +
                        "SELECT orderkey, linenumber, linestatus " +
                        "FROM tpch.tiny.lineitem",
                "SELECT count(*) from lineitem");

        // Delete returns number of rows deleted, or null if obtaining the number is hard or impossible.
        // Currently, Hive implementation always returns null.
        assertUpdate("DELETE FROM test_metadata_delete WHERE LINE_STATUS='F' and LINE_NUMBER=CAST(3 AS INTEGER)");

        assertQuery("SELECT * from test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'F' or linenumber<>3");

        // TODO This use case can be supported
        try {
            getQueryRunner().execute("DELETE FROM test_metadata_delete WHERE lower(LINE_STATUS)='f' and LINE_NUMBER=CAST(4 AS INTEGER)");
            fail("expected exception");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "This connector only supports delete where one or more partitions are deleted entirely");
        }

        assertUpdate("DELETE FROM test_metadata_delete WHERE LINE_STATUS='O'");

        assertQuery("SELECT * from test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' and linenumber<>3");

        try {
            getQueryRunner().execute("DELETE FROM test_metadata_delete WHERE ORDER_KEY=1");
            fail("expected exception");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "This connector only supports delete where one or more partitions are deleted entirely");
        }

        assertQuery("SELECT * from test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' and linenumber<>3");

        assertUpdate("DROP TABLE test_metadata_delete");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_metadata_delete"));
    }

    private TableMetadata getTableMetadata(String catalog, String schema, String tableName)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    Optional<TableHandle> tableHandle = metadata.getMetadataResolver(transactionSession).getTableHandle(new QualifiedObjectName(catalog, schema, tableName));
                    assertTrue(tableHandle.isPresent());
                    return metadata.getTableMetadata(transactionSession, tableHandle.get());
                });
    }

    private List<?> getPartitions(String tableName)
    {
        return (List<?>) getHiveTableProperty(getQueryRunner(), getSession(), tableName, (HiveTableLayoutHandle table) -> getPartitions(table));
    }

    private int getBucketCount(String tableName)
    {
        return (int) getHiveTableProperty(getQueryRunner(), getSession(), tableName, (HiveTableLayoutHandle table) -> table.getBucketHandle().get().getTableBucketCount());
    }

    @Test
    public void testShowColumnsPartitionKey()
    {
        assertUpdate("" +
                "CREATE TABLE test_show_columns_partition_key\n" +
                "(grape bigint, orange bigint, pear varchar(65535), mango integer, lychee smallint, kiwi tinyint, apple varchar, pineapple varchar(65535))\n" +
                "WITH (partitioned_by = ARRAY['apple', 'pineapple'])");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM test_show_columns_partition_key");
        Type unboundedVarchar = canonicalizeType(VARCHAR);
        MaterializedResult expected = resultBuilder(getSession(), unboundedVarchar, unboundedVarchar, unboundedVarchar, unboundedVarchar, BIGINT, BIGINT, BIGINT)
                .row("grape", canonicalizeTypeName("bigint"), "", "", 19L, null, null)
                .row("orange", canonicalizeTypeName("bigint"), "", "", 19L, null, null)
                .row("pear", canonicalizeTypeName("varchar(65535)"), "", "", null, null, 65535L)
                .row("mango", canonicalizeTypeName("integer"), "", "", 10L, null, null)
                .row("lychee", canonicalizeTypeName("smallint"), "", "", 5L, null, null)
                .row("kiwi", canonicalizeTypeName("tinyint"), "", "", 3L, null, null)
                .row("apple", canonicalizeTypeName("varchar"), "partition key", "", null, null, 2147483647L)
                .row("pineapple", canonicalizeTypeName("varchar(65535)"), "partition key", "", null, null, 65535L)
                .build();
        assertEquals(actual, expected);
    }

    // TODO: These should be moved to another class, when more connectors support arrays
    @Test
    public void testArrays()
    {
        assertUpdate("CREATE TABLE tmp_array1 AS SELECT ARRAY[1, 2, NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array1", "SELECT 2");
        assertQuery("SELECT col[3] FROM tmp_array1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array2 AS SELECT ARRAY[1.0E0, 2.5E0, 3.5E0] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array2", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_array3 AS SELECT ARRAY['puppies', 'kittens', NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array3", "SELECT 'kittens'");
        assertQuery("SELECT col[3] FROM tmp_array3", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array4 AS SELECT ARRAY[TRUE, NULL] AS col", 1);
        assertQuery("SELECT col[1] FROM tmp_array4", "SELECT TRUE");
        assertQuery("SELECT col[2] FROM tmp_array4", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array5 AS SELECT ARRAY[ARRAY[1, 2], NULL, ARRAY[3, 4]] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array5", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array6 AS SELECT ARRAY[ARRAY['\"hi\"'], NULL, ARRAY['puppies']] AS col", 1);
        assertQuery("SELECT col[1][1] FROM tmp_array6", "SELECT '\"hi\"'");
        assertQuery("SELECT col[3][1] FROM tmp_array6", "SELECT 'puppies'");

        assertUpdate("CREATE TABLE tmp_array7 AS SELECT ARRAY[ARRAY[INTEGER'1', INTEGER'2'], NULL, ARRAY[INTEGER'3', INTEGER'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array7", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array8 AS SELECT ARRAY[ARRAY[SMALLINT'1', SMALLINT'2'], NULL, ARRAY[SMALLINT'3', SMALLINT'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array8", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array9 AS SELECT ARRAY[ARRAY[TINYINT'1', TINYINT'2'], NULL, ARRAY[TINYINT'3', TINYINT'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array9", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array10 AS SELECT ARRAY[ARRAY[DECIMAL '3.14']] AS col1, ARRAY[ARRAY[DECIMAL '12345678901234567890.0123456789']] AS col2", 1);
        assertQuery("SELECT col1[1][1] FROM tmp_array10", "SELECT 3.14");
        assertQuery("SELECT col2[1][1] FROM tmp_array10", "SELECT 12345678901234567890.0123456789");

        assertUpdate("CREATE TABLE tmp_array13 AS SELECT ARRAY[ARRAY[REAL'1.234', REAL'2.345'], NULL, ARRAY[REAL'3.456', REAL'4.567']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array13", "SELECT 2.345");
    }

    @Test
    public void testTemporalArrays()
    {
        assertUpdate("CREATE TABLE tmp_array11 AS SELECT ARRAY[DATE '2014-09-30'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array11");
        assertUpdate("CREATE TABLE tmp_array12 AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array12");
    }

    @Test
    public void testMaps()
    {
        assertUpdate("CREATE TABLE tmp_map1 AS SELECT MAP(ARRAY[0,1], ARRAY[2,NULL]) AS col", 1);
        assertQuery("SELECT col[0] FROM tmp_map1", "SELECT 2");
        assertQuery("SELECT col[1] FROM tmp_map1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_map2 AS SELECT MAP(ARRAY[INTEGER'1'], ARRAY[INTEGER'2']) AS col", 1);
        assertQuery("SELECT col[INTEGER'1'] FROM tmp_map2", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map3 AS SELECT MAP(ARRAY[SMALLINT'1'], ARRAY[SMALLINT'2']) AS col", 1);
        assertQuery("SELECT col[SMALLINT'1'] FROM tmp_map3", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map4 AS SELECT MAP(ARRAY[TINYINT'1'], ARRAY[TINYINT'2']) AS col", 1);
        assertQuery("SELECT col[TINYINT'1'] FROM tmp_map4", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map5 AS SELECT MAP(ARRAY[1.0], ARRAY[2.5]) AS col", 1);
        assertQuery("SELECT col[1.0] FROM tmp_map5", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_map6 AS SELECT MAP(ARRAY['puppies'], ARRAY['kittens']) AS col", 1);
        assertQuery("SELECT col['puppies'] FROM tmp_map6", "SELECT 'kittens'");

        assertUpdate("CREATE TABLE tmp_map7 AS SELECT MAP(ARRAY[TRUE], ARRAY[FALSE]) AS col", 1);
        assertQuery("SELECT col[TRUE] FROM tmp_map7", "SELECT FALSE");

        assertUpdate("CREATE TABLE tmp_map8 AS SELECT MAP(ARRAY[DATE '2014-09-30'], ARRAY[DATE '2014-09-29']) AS col", 1);
        assertOneNotNullResult("SELECT col[DATE '2014-09-30'] FROM tmp_map8");
        assertUpdate("CREATE TABLE tmp_map9 AS SELECT MAP(ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'], ARRAY[TIMESTAMP '2001-08-22 03:04:05.321']) AS col", 1);
        assertOneNotNullResult("SELECT col[TIMESTAMP '2001-08-22 03:04:05.321'] FROM tmp_map9");

        assertUpdate("CREATE TABLE tmp_map10 AS SELECT MAP(ARRAY[DECIMAL '3.14', DECIMAL '12345678901234567890.0123456789'], " +
                "ARRAY[DECIMAL '12345678901234567890.0123456789', DECIMAL '3.0123456789']) AS col", 1);
        assertQuery("SELECT col[DECIMAL '3.14'], col[DECIMAL '12345678901234567890.0123456789'] FROM tmp_map10", "SELECT 12345678901234567890.0123456789, 3.0123456789");

        assertUpdate("CREATE TABLE tmp_map11 AS SELECT MAP(ARRAY[REAL'1.234'], ARRAY[REAL'2.345']) AS col", 1);
        assertQuery("SELECT col[REAL'1.234'] FROM tmp_map11", "SELECT 2.345");

        assertUpdate("CREATE TABLE tmp_map12 AS SELECT MAP(ARRAY[1.0E0], ARRAY[ARRAY[1, 2]]) AS col", 1);
        assertQuery("SELECT col[1.0][2] FROM tmp_map12", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map13 AS SELECT MAP(ARRAY['puppies', 'kittens'], ARRAY['corgi', 'norwegianWood']) AS col", 1);
        assertQuery("SELECT col['puppies'] FROM tmp_map13", "SELECT 'corgi'");
        assertQuery("SELECT col['kittens'] FROM tmp_map13", "SELECT 'norwegianWood'");
    }

    @Test
    public void testRows()
    {
        assertUpdate("CREATE TABLE tmp_row1 AS SELECT cast(row(CAST(1 as BIGINT), CAST(NULL as BIGINT)) AS row(col0 bigint, col1 bigint)) AS a", 1);
        assertQuery(
                "SELECT a.col0, a.col1 FROM tmp_row1",
                "SELECT 1, cast(null as bigint)");
    }

    @Test
    public void testComplex()
    {
        assertUpdate("CREATE TABLE tmp_complex1 AS SELECT " +
                        "ARRAY [MAP(ARRAY['a', 'b'], ARRAY[2.0E0, 4.0E0]), MAP(ARRAY['c', 'd'], ARRAY[12.0E0, 14.0E0])] AS a",
                1);

        assertQuery(
                "SELECT a[1]['a'], a[2]['d'] FROM tmp_complex1",
                "SELECT 2.0, 14.0");
    }

    @Test
    public void testBucketedCatalog()
    {
        testBucketedCatalog(bucketedSession);
    }

    private void testBucketedCatalog(Session session)
    {
        String bucketedCatalog = session.getCatalog().get();
        String bucketedSchema = session.getSchema().get();

        TableMetadata ordersTableMetadata = getTableMetadata(bucketedCatalog, bucketedSchema, "orders");
        assertEquals(ordersTableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey"));
        assertEquals(ordersTableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        TableMetadata customerTableMetadata = getTableMetadata(bucketedCatalog, bucketedSchema, "customer");
        assertEquals(customerTableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey"));
        assertEquals(customerTableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);
    }

    @Test
    public void testBucketedExecution()
    {
        testBucketedExecution(bucketedSession);
    }

    private void testBucketedExecution(Session session)
    {
        assertQuery(session, "select count(*) a from orders t1 join orders t2 on t1.custkey=t2.custkey");
        assertQuery(session, "select count(*) a from orders t1 join customer t2 on t1.custkey=t2.custkey", "SELECT count(*) from orders");
        assertQuery(session, "select count(distinct custkey) from orders");

        assertQuery(
                Session.builder(session).setSystemProperty("task_writer_count", "1").build(),
                "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
        assertQuery(
                Session.builder(session).setSystemProperty("task_writer_count", "4").build(),
                "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testBucketPruning()
    {
        Session session = getSession();
        QueryRunner queryRunner = getQueryRunner();

        queryRunner.execute("CREATE TABLE orders_bucketed WITH (bucket_count = 11, bucketed_by = ARRAY['orderkey']) AS " +
                "SELECT * FROM orders");

        try {
            assertQuery(session, "SELECT * FROM orders_bucketed WHERE orderkey = 100", "SELECT * FROM orders WHERE orderkey = 100");

            assertQuery(session, "SELECT * FROM orders_bucketed WHERE orderkey = 100 OR orderkey = 101", "SELECT * FROM orders WHERE orderkey = 100 OR orderkey = 101");

            assertQuery(session, "SELECT * FROM orders_bucketed WHERE orderkey IN (100, 101, 133)", "SELECT * FROM orders WHERE orderkey IN (100, 101, 133)");

            assertQuery(session, "SELECT * FROM orders_bucketed", "SELECT * FROM orders");

            assertQuery(session, "SELECT * FROM orders_bucketed WHERE orderkey > 100", "SELECT * FROM orders WHERE orderkey > 100");

            assertQuery(session, "SELECT * FROM orders_bucketed WHERE orderkey != 100", "SELECT * FROM orders WHERE orderkey != 100");
        }
        finally {
            queryRunner.execute("DROP TABLE orders_bucketed");
        }

        queryRunner.execute("CREATE TABLE orders_bucketed WITH (bucket_count = 11, bucketed_by = ARRAY['orderkey', 'custkey']) AS " +
                "SELECT * FROM orders");

        try {
            assertQuery(session, "SELECT * FROM orders_bucketed WHERE orderkey = 101 AND custkey = 280", "SELECT * FROM orders WHERE orderkey = 101 AND custkey = 280");

            assertQuery(session, "SELECT * FROM orders_bucketed WHERE orderkey IN (101, 71) AND custkey = 280", "SELECT * FROM orders WHERE orderkey IN (101, 71) AND custkey = 280");

            assertQuery(session, "SELECT * FROM orders_bucketed WHERE orderkey IN (101, 71) AND custkey IN (280, 34)", "SELECT * FROM orders WHERE orderkey IN (101, 71) AND custkey IN (280, 34)");

            assertQuery(session, "SELECT * FROM orders_bucketed WHERE orderkey = 101 AND custkey = 280 AND orderstatus <> '0'", "SELECT * FROM orders WHERE orderkey = 101 AND custkey = 280 AND orderstatus <> '0'");

            assertQuery(session, "SELECT * FROM orders_bucketed WHERE orderkey = 101", "SELECT * FROM orders WHERE orderkey = 101");

            assertQuery(session, "SELECT * FROM orders_bucketed WHERE custkey = 280", "SELECT * FROM orders WHERE custkey = 280");

            assertQuery(session, "SELECT * FROM orders_bucketed WHERE orderkey = 101 AND custkey > 280", "SELECT * FROM orders WHERE orderkey = 101 AND custkey > 280");
        }
        finally {
            queryRunner.execute("DROP TABLE orders_bucketed");
        }
    }

    @Test
    public void testNullBucket()
    {
        Session session = getSession();
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.execute("CREATE TABLE table_with_null_buckets WITH (bucket_count=2, bucketed_by = ARRAY['key']) AS " +
                "SELECT 10 x, CAST(NULL AS INTEGER) AS key UNION ALL SELECT 20 x, 1 key");

        try {
            assertQuery(session, "SELECT COUNT() FROM table_with_null_buckets WHERE key IS NULL", "SELECT 1");
            assertQuery(session, "SELECT x FROM table_with_null_buckets WHERE key IS NULL", "SELECT 10");
            assertQuery(session, "SELECT key FROM table_with_null_buckets WHERE x = 10", "SELECT NULL");
            assertQuery(session, "SELECT x FROM table_with_null_buckets WHERE key = 1", "SELECT 20");
            assertQuery(session, "SELECT key FROM table_with_null_buckets WHERE key IS NULL AND x = 10", "SELECT NULL");
            assertQuery(session, "SELECT COUNT() FROM table_with_null_buckets WHERE key IS NULL AND x = 1", "SELECT 0");
            assertQuery(session, "SELECT COUNT() FROM table_with_null_buckets WHERE key = 10", "SELECT 0");
            assertQuery(session, "SELECT COUNT() FROM table_with_null_buckets WHERE key IN (NULL, 1)", "SELECT 1");
            assertQuery(session, "SELECT COUNT() FROM table_with_null_buckets WHERE key IS NULL OR key = 1", "SELECT 2");
        }
        finally {
            queryRunner.execute("DROP TABLE table_with_null_buckets");
        }
    }

    @Test
    public void testWriteSortedTable()
    {
        testWriteSortedTable(getSession());
        testWriteSortedTable(Session.builder(getSession())
                .setCatalogSessionProperty(catalog, SORTED_WRITE_TO_TEMP_PATH_ENABLED, "true")
                .setCatalogSessionProperty(catalog, SORTED_WRITE_TEMP_PATH_SUBDIRECTORY_COUNT, "10")
                .build());
    }

    private void testWriteSortedTable(Session session)
    {
        try {
            // create table
            assertUpdate(
                    session,
                    "CREATE TABLE create_partitioned_sorted_table (orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus)\n" +
                            "WITH (partitioned_by = ARRAY['orderstatus'], bucketed_by = ARRAY['custkey'], bucket_count = 11, sorted_by = ARRAY['orderkey']) AS\n" +
                            "SELECT orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus FROM tpch.tiny.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());
            assertQuery(
                    session,
                    "SELECT count(*) FROM create_partitioned_sorted_table",
                    "SELECT count(*) FROM orders");

            assertUpdate(
                    session,
                    "CREATE TABLE create_unpartitioned_sorted_table (orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus)\n" +
                            "WITH (bucketed_by = ARRAY['custkey'], bucket_count = 11, sorted_by = ARRAY['orderkey']) AS\n" +
                            "SELECT orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus FROM tpch.tiny.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());
            assertQuery(
                    session,
                    "SELECT count(*) FROM create_unpartitioned_sorted_table",
                    "SELECT count(*) FROM orders");

            // insert
            assertUpdate(
                    session,
                    "CREATE TABLE insert_partitioned_sorted_table (LIKE create_partitioned_sorted_table) " +
                            "WITH (partitioned_by = ARRAY['orderstatus'], bucketed_by = ARRAY['custkey'], bucket_count = 11, sorted_by = ARRAY['orderkey'])");

            assertUpdate(
                    session,
                    "INSERT INTO insert_partitioned_sorted_table\n" +
                            "SELECT orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus FROM tpch.tiny.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());
            assertQuery(
                    session,
                    "SELECT count(*) FROM insert_partitioned_sorted_table",
                    "SELECT count(*) FROM orders");
        }
        finally {
            assertUpdate(session, "DROP TABLE IF EXISTS create_partitioned_sorted_table");
            assertUpdate(session, "DROP TABLE IF EXISTS create_unpartitioned_sorted_table");
            assertUpdate(session, "DROP TABLE IF EXISTS insert_partitioned_sorted_table");
        }
    }

    @Test
    public void testWritePreferredOrderingTable()
    {
        testWritePreferredOrderingTable(getSession());
        testWritePreferredOrderingTable(Session.builder(getSession())
                .setCatalogSessionProperty(catalog, SORTED_WRITE_TO_TEMP_PATH_ENABLED, "true")
                .setCatalogSessionProperty(catalog, SORTED_WRITE_TEMP_PATH_SUBDIRECTORY_COUNT, "10")
                .build());
    }

    public void testWritePreferredOrderingTable(Session session)
    {
        try {
            // create table
            assertUpdate(
                    session,
                    "CREATE TABLE create_partitioned_ordering_table (orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus)\n" +
                            "WITH (partitioned_by = ARRAY['orderstatus'], preferred_ordering_columns = ARRAY['orderkey']) AS\n" +
                            "SELECT orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus FROM tpch.tiny.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());
            assertQuery(
                    session,
                    "SELECT count(*) FROM create_partitioned_ordering_table",
                    "SELECT count(*) FROM orders");

            assertUpdate(
                    session,
                    "CREATE TABLE create_unpartitioned_ordering_table (orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus)\n" +
                            "WITH (preferred_ordering_columns = ARRAY['orderkey']) AS\n" +
                            "SELECT orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus FROM tpch.tiny.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());
            assertQuery(
                    session,
                    "SELECT count(*) FROM create_unpartitioned_ordering_table",
                    "SELECT count(*) FROM orders");

            // insert
            assertUpdate(
                    session,
                    "CREATE TABLE insert_partitioned_ordering_table (LIKE create_partitioned_ordering_table) " +
                            "WITH (partitioned_by = ARRAY['orderstatus'], preferred_ordering_columns = ARRAY['orderkey'])");

            assertUpdate(
                    session,
                    "INSERT INTO insert_partitioned_ordering_table\n" +
                            "SELECT orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus FROM tpch.tiny.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());
            assertQuery(
                    session,
                    "SELECT count(*) FROM insert_partitioned_ordering_table",
                    "SELECT count(*) FROM orders");

            // invalid
            assertQueryFails(
                    session,
                    "CREATE TABLE invalid_bucketed_ordering_table (LIKE create_partitioned_ordering_table) " +
                            "WITH (preferred_ordering_columns = ARRAY['orderkey'], bucketed_by = ARRAY['totalprice'], bucket_count = 13)",
                    ".*preferred_ordering_columns must not be specified when bucketed_by is specified.*");
        }
        finally {
            assertUpdate(session, "DROP TABLE IF EXISTS create_partitioned_ordering_table");
            assertUpdate(session, "DROP TABLE IF EXISTS create_unpartitioned_ordering_table");
            assertUpdate(session, "DROP TABLE IF EXISTS insert_partitioned_ordering_table");
        }
    }

    @Test
    public void testScaleWriters()
    {
        try {
            // small table that will only have one writer
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("writer_min_size", "32MB")
                            .setSystemProperty("task_writer_count", "1")
                            .build(),
                    "CREATE TABLE scale_writers_small AS SELECT * FROM tpch.tiny.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());

            assertEquals(computeActual("SELECT count(DISTINCT \"$path\") FROM scale_writers_small").getOnlyValue(), 1L);

            // large table that will scale writers to multiple machines
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("writer_min_size", "1MB")
                            .setSystemProperty("task_writer_count", "1")
                            .build(),
                    "CREATE TABLE scale_writers_large WITH (format = 'RCBINARY') AS SELECT * FROM tpch.sf1.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.sf1.orders").getOnlyValue());

            long files = (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM scale_writers_large");
            long workers = (long) computeScalar("SELECT count(*) FROM system.runtime.nodes");
            assertThat(files).isBetween(2L, workers);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS scale_writers_large");
            assertUpdate("DROP TABLE IF EXISTS scale_writers_small");
        }
    }

    @Test
    public void testPreferManifestsToListFilesForPartitionedTable()
    {
        try {
            // Create partitioned table. Partitions are populated with list of fileNames,sizes
            assertUpdate(
                    Session.builder(getSession())
                            .setCatalogSessionProperty(catalog, FILE_RENAMING_ENABLED, "true")
                            .setCatalogSessionProperty(catalog, PREFER_MANIFESTS_TO_LIST_FILES, "true")
                            .setSystemProperty("scale_writers", "false")
                            .setSystemProperty("writer_min_size", "1MB")
                            .setSystemProperty("task_writer_count", "1")
                            .build(),
                    "CREATE TABLE partitioned_ordering_table (orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus)\n" +
                            "WITH (partitioned_by = ARRAY['orderstatus'], preferred_ordering_columns = ARRAY['orderkey']) AS\n" +
                            "SELECT orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus FROM tpch.tiny.orders where orderstatus = 'O'",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders where orderstatus = 'O'").getOnlyValue());

            // Assert that we are able to read data after disabling listFiles() call to storage
            assertQuery(
                    Session.builder(getSession())
                            .setCatalogSessionProperty(catalog, FILE_RENAMING_ENABLED, "true")
                            .setCatalogSessionProperty(catalog, PREFER_MANIFESTS_TO_LIST_FILES, "true")
                            .setCatalogSessionProperty(catalog, MANIFEST_VERIFICATION_ENABLED, "true")
                            .build(),
                    "SELECT orderkey, custkey, totalprice, orderdate FROM partitioned_ordering_table",
                    "SELECT orderkey, custkey, totalprice, orderdate FROM orders where orderstatus = 'O'");

            // Insert a new partition with NO file_renaming. Meaning the list of filenames,sizes will NOT be stored in Metastore partition.
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("scale_writers", "false")
                            .setSystemProperty("writer_min_size", "1MB")
                            .setSystemProperty("task_writer_count", "1")
                            .build(),
                    "INSERT INTO partitioned_ordering_table (orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus)\n" +
                            "SELECT orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus FROM tpch.tiny.orders where orderstatus = 'P'",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders where orderstatus = 'P'").getOnlyValue());

            // This SELECT involves querying partitions with and without list of filenames, sizes.
            // Assert that we are able to read mixed partition metadata with and without path/size info.
            assertQuery(
                    Session.builder(getSession())
                            .setCatalogSessionProperty(catalog, FILE_RENAMING_ENABLED, "true")
                            .setCatalogSessionProperty(catalog, PREFER_MANIFESTS_TO_LIST_FILES, "true")
                            .setCatalogSessionProperty(catalog, MANIFEST_VERIFICATION_ENABLED, "true")
                            .build(),
                    "SELECT orderkey, custkey, totalprice, orderdate FROM partitioned_ordering_table",
                    "SELECT orderkey, custkey, totalprice, orderdate FROM orders where orderstatus = 'O' OR orderstatus = 'P'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS partitioned_ordering_table");
        }
    }

    @Test
    public void testPreferManifestsToListFilesForUnPartitionedTable()
    {
        try {
            // Create un-partitioned table
            assertUpdate(
                    Session.builder(getSession())
                            .setCatalogSessionProperty(catalog, FILE_RENAMING_ENABLED, "true")
                            .setCatalogSessionProperty(catalog, PREFER_MANIFESTS_TO_LIST_FILES, "true")
                            .setSystemProperty("scale_writers", "false")
                            .setSystemProperty("writer_min_size", "1MB")
                            .setSystemProperty("task_writer_count", "1")
                            .build(),
                    "CREATE TABLE unpartitioned_ordering_table AS SELECT * FROM tpch.tiny.orders where orderstatus = 'O'",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders where orderstatus = 'O'").getOnlyValue());

            assertQuery(
                    Session.builder(getSession())
                            .setCatalogSessionProperty(catalog, FILE_RENAMING_ENABLED, "true")
                            .setCatalogSessionProperty(catalog, PREFER_MANIFESTS_TO_LIST_FILES, "true")
                            .setCatalogSessionProperty(catalog, MANIFEST_VERIFICATION_ENABLED, "true")
                            .build(),
                    "SELECT orderkey, custkey, totalprice, orderdate FROM unpartitioned_ordering_table",
                    "SELECT orderkey, custkey, totalprice, orderdate FROM orders where orderstatus = 'O'");

            // Insert data with NO file_renaming.
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("scale_writers", "false")
                            .setSystemProperty("writer_min_size", "1MB")
                            .setSystemProperty("task_writer_count", "1")
                            .build(),
                    "INSERT INTO unpartitioned_ordering_table (orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus)\n" +
                            "SELECT orderkey, custkey, totalprice, orderdate, orderpriority, clerk, shippriority, comment, orderstatus FROM tpch.tiny.orders where orderstatus = 'P'",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders where orderstatus = 'P'").getOnlyValue());

            assertQuery(
                    Session.builder(getSession())
                            .setCatalogSessionProperty(catalog, FILE_RENAMING_ENABLED, "true")
                            .setCatalogSessionProperty(catalog, PREFER_MANIFESTS_TO_LIST_FILES, "true")
                            .setCatalogSessionProperty(catalog, MANIFEST_VERIFICATION_ENABLED, "true")
                            .build(),
                    "SELECT orderkey, custkey, totalprice, orderdate FROM unpartitioned_ordering_table",
                    "SELECT orderkey, custkey, totalprice, orderdate FROM orders where orderstatus = 'O' OR orderstatus = 'P'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS unpartitioned_ordering_table");
        }
    }

    @Test
    public void testShowCreateTable()
    {
        String createTableFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint,\n" +
                "   %s double,\n" +
                "   \"c 3\" varchar,\n" +
                "   \"c'4\" array(bigint),\n" +
                "   %s map(bigint, varchar)\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'RCBINARY'\n" +
                ")";
        String createTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_show_create_table",
                "c1",
                "c2",
                "c5");
        String expectedShowCreateTable = format(
                createTableFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_show_create_table",
                "\"c1\"",
                "\"c2\"",
                "\"c5\"");
        assertUpdate(createTableSql);
        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_show_create_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedShowCreateTable);

        createTableFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint,\n" +
                "   \"c 2\" varchar,\n" +
                "   \"c'3\" array(bigint),\n" +
                "   %s map(bigint, varchar) COMMENT 'comment test4',\n" +
                "   %s double COMMENT 'comment test5'\n)\n" +
                "COMMENT 'test'\n" +
                "WITH (\n" +
                "   bucket_count = 5,\n" +
                "   bucketed_by = ARRAY['c1','c 2'],\n" +
                "   format = 'ORC',\n" +
                "   orc_bloom_filter_columns = ARRAY['c1','c2'],\n" +
                "   orc_bloom_filter_fpp = 7E-1,\n" +
                "   partitioned_by = ARRAY['c5'],\n" +
                "   sorted_by = ARRAY['c1','c 2 DESC']\n" +
                ")";
        createTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "\"test_show_create_table'2\"",
                "\"c1\"",
                "\"c2\"",
                "\"c5\"");
        assertUpdate(createTableSql);
        actualResult = computeActual("SHOW CREATE TABLE \"test_show_create_table'2\"");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);
    }

    @Test
    public void testShowCreateSchema()
    {
        String createSchemaSql = "CREATE SCHEMA show_create_hive_schema";
        assertUpdate(createSchemaSql);
        String expectedShowCreateSchema = "CREATE SCHEMA show_create_hive_schema\n" +
                "WITH (\n" +
                "   location = '.*show_create_hive_schema'\n" +
                ")";

        MaterializedResult actualResult = computeActual("SHOW CREATE SCHEMA show_create_hive_schema");
        assertThat(getOnlyElement(actualResult.getOnlyColumnAsSet()).toString().matches(expectedShowCreateSchema));

        assertQueryFails(format("SHOW CREATE SCHEMA %s.%s", getSession().getCatalog().get(), ""), ".*mismatched input '.'. Expecting: <EOF>");
        assertQueryFails(format("SHOW CREATE SCHEMA %s.%s.%s", getSession().getCatalog().get(), "show_create_hive_schema", "tabletest"), ".*Too many parts in schema name: hive.show_create_hive_schema.tabletest");
        assertQueryFails(format("SHOW CREATE SCHEMA %s", "schema_not_exist"), ".*Schema 'hive.schema_not_exist' does not exist");
        assertUpdate("DROP SCHEMA show_create_hive_schema");
    }

    @Test
    public void testTextfileAmbiguousTimestamp()
    {
        try {
            // set the session time zone to the same as the system timezone to avoid extra time zone conversions outside of what we are trying to test
            Session timezoneSession = Session.builder(getSession()).setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/Bahia_Banderas")).build();
            @Language("SQL") String createTableSql = format("" +
                            "CREATE TABLE test_timestamp_textfile \n" +
                            "WITH (\n" +
                            "   format = 'TEXTFILE'\n" +
                            ")\n" +
                            "AS SELECT TIMESTAMP '2022-10-30 01:16:13.000' ts, ARRAY[TIMESTAMP '2022-10-30 01:16:13.000'] array_ts",
                    getSession().getCatalog().get(),
                    getSession().getSchema().get());
            assertUpdate(timezoneSession, createTableSql, 1);
            // 2022-10-30 01:16:13.00 is an ambiguous timestamp in America/Bahia_Banderas because
            // it occurs during a fall DST transition where the hour from 1-2am repeats
            // Ambiguous timestamps should be interpreted as the earlier of the two possible unixtimes for consistency.
            assertQuery(timezoneSession, "SELECT to_unixtime(ts), to_unixtime(array_ts[1]) FROM test_timestamp_textfile", "SELECT 1.667110573E9, 1.667110573E9");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_timestamp_textfile");
        }
    }

    @Test
    public void testCreateExternalTable()
            throws Exception
    {
        File tempDir = createTempDir();
        File dataFile = new File(tempDir, "test.txt");
        Files.write("hello\nworld\n", dataFile, UTF_8);

        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_create_external (\n" +
                        "   \"name\" varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   external_location = '%s',\n" +
                        "   format = 'TEXTFILE'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                new Path(tempDir.toURI().toASCIIString()).toString());

        assertUpdate(createTableSql);
        MaterializedResult actual = computeActual("SHOW CREATE TABLE test_create_external");
        assertEquals(actual.getOnlyValue(), createTableSql);

        actual = computeActual("SELECT name FROM test_create_external");
        assertEquals(actual.getOnlyColumnAsSet(), ImmutableSet.of("hello", "world"));

        assertQueryFails(
                "INSERT INTO test_create_external VALUES ('somevalue')",
                ".*Cannot write to non-managed Hive table.*");

        assertUpdate("DROP TABLE test_create_external");

        // file should still exist after drop
        assertFile(dataFile);

        deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testPathHiddenColumn()
    {
        testWithAllStorageFormats(this::testPathHiddenColumn);
    }

    private void testPathHiddenColumn(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "CREATE TABLE test_path " +
                "WITH (" +
                "format = '" + storageFormat + "'," +
                "partitioned_by = ARRAY['col1']" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(col0, col1) ";
        assertUpdate(session, createTable, 8);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_path"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_path");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, ROW_ID_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(PATH_COLUMN_NAME)) {
                // $path should be hidden column
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getPartitions("test_path").size(), 3);

        MaterializedResult results = computeActual(session, format("SELECT *, \"%s\" FROM test_path", PATH_COLUMN_NAME));
        Map<Integer, String> partitionPathMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            String pathName = (String) row.getField(2);
            String parentDirectory = new Path(pathName).getParent().toString();

            assertTrue(pathName.length() > 0);
            assertEquals(col0 % 3, col1);
            if (partitionPathMap.containsKey(col1)) {
                // the rows in the same partition should be in the same partition directory
                assertEquals(partitionPathMap.get(col1), parentDirectory);
            }
            else {
                partitionPathMap.put(col1, parentDirectory);
            }
        }
        assertEquals(partitionPathMap.size(), 3);

        assertUpdate(session, "DROP TABLE test_path");
        assertFalse(getQueryRunner().tableExists(session, "test_path"));
    }

    @Test
    public void testBucketHiddenColumn()
    {
        @Language("SQL") String createTable = "CREATE TABLE test_bucket_hidden_column " +
                "WITH (" +
                "bucketed_by = ARRAY['col0']," +
                "bucket_count = 2" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 11), (1, 12), (2, 13), " +
                "(3, 14), (4, 15), (5, 16), " +
                "(6, 17), (7, 18), (8, 19)" +
                " ) t (col0, col1) ";
        assertUpdate(createTable, 9);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_bucket_hidden_column"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_bucket_hidden_column");
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("col0"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 2);

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, BUCKET_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, ROW_ID_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(BUCKET_COLUMN_NAME)) {
                // $bucket_number should be hidden column
                assertTrue(columnMetadata.isHidden());
            }
            else if (columnMetadata.getName().equals(ROW_ID_COLUMN_NAME)) {
                // $row_id should be hidden column
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getBucketCount("test_bucket_hidden_column"), 2);

        MaterializedResult results = computeActual(format("SELECT *, \"%1$s\" FROM test_bucket_hidden_column WHERE \"%1$s\" = 1",
                BUCKET_COLUMN_NAME));
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            int bucket = (int) row.getField(2);

            assertEquals(col1, col0 + 11);
            assertEquals(col1 % 2, 0);

            // Because Hive's hash function for integer n is h(n) = n.
            assertEquals(bucket, col0 % 2);
        }
        assertEquals(results.getRowCount(), 4);

        assertUpdate("DROP TABLE test_bucket_hidden_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_bucket_hidden_column"));
    }

    @Test
    public void testFileSizeHiddenColumn()
    {
        @Language("SQL") String createTable = "CREATE TABLE test_file_size " +
                "AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(col0, col1) ";
        assertUpdate(createTable, 8);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_file_size"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_file_size");

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, ROW_ID_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(FILE_SIZE_COLUMN_NAME)) {
                assertTrue(columnMetadata.isHidden());
            }
        }

        MaterializedResult results = computeActual(format("SELECT *, \"%s\" FROM test_file_size", FILE_SIZE_COLUMN_NAME));
        Map<Integer, Long> fileSizeMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            long fileSize = (Long) row.getField(2);

            assertTrue(fileSize > 0);
            assertEquals(col0 % 3, col1);
            if (fileSizeMap.containsKey(col1)) {
                assertEquals(fileSizeMap.get(col1).longValue(), fileSize);
            }
            else {
                fileSizeMap.put(col1, fileSize);
            }
        }
        assertEquals(fileSizeMap.size(), 3);

        assertUpdate("DROP TABLE test_file_size");
    }

    @Test
    public void testFileModifiedTimeHiddenColumn()
    {
        long testStartTime = Instant.now().toEpochMilli();

        @Language("SQL") String createTable = "CREATE TABLE test_file_modified_time " +
                "WITH (" +
                "partitioned_by = ARRAY['col1']" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(col0, col1) ";
        assertUpdate(createTable, 8);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_file_modified_time"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_file_modified_time");

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, ROW_ID_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(FILE_MODIFIED_TIME_COLUMN_NAME)) {
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getPartitions("test_file_modified_time").size(), 3);

        MaterializedResult results = computeActual(format("SELECT *, \"%s\" FROM test_file_modified_time", FILE_MODIFIED_TIME_COLUMN_NAME));
        Map<Integer, Long> fileModifiedTimeMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            long fileModifiedTime = (Long) row.getField(2);

            assertTrue(fileModifiedTime > (testStartTime - 2_000));
            assertEquals(col0 % 3, col1);
            if (fileModifiedTimeMap.containsKey(col1)) {
                assertEquals(fileModifiedTimeMap.get(col1).longValue(), fileModifiedTime);
            }
            else {
                fileModifiedTimeMap.put(col1, fileModifiedTime);
            }
        }
        assertEquals(fileModifiedTimeMap.size(), 3);

        assertUpdate("DROP TABLE test_file_modified_time");
    }

    @Test
    public void testDeleteAndInsert()
    {
        Session session = getSession();

        // Partition 1 is untouched
        // Partition 2 is altered (dropped and then added back)
        // Partition 3 is added
        // Partition 4 is dropped

        assertUpdate(
                session,
                "CREATE TABLE tmp_delete_insert WITH (partitioned_by=array ['z']) AS " +
                        "SELECT * from (VALUES (CAST (101 AS BIGINT), CAST (1 AS BIGINT)), (201, 2), (202, 2), (401, 4), (402, 4), (403, 4)) t(a, z)",
                6);

        List<MaterializedRow> expectedBefore = MaterializedResult.resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(201L, 2L)
                .row(202L, 2L)
                .row(401L, 4L)
                .row(402L, 4L)
                .row(403L, 4L)
                .build()
                .getMaterializedRows();
        List<MaterializedRow> expectedAfter = MaterializedResult.resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(203L, 2L)
                .row(204L, 2L)
                .row(205L, 2L)
                .row(301L, 2L)
                .row(302L, 3L)
                .build()
                .getMaterializedRows();

        try {
            transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                    .execute(session, transactionSession -> {
                        assertUpdate(transactionSession, "DELETE FROM tmp_delete_insert WHERE z >= 2");
                        assertUpdate(transactionSession, "INSERT INTO tmp_delete_insert VALUES (203, 2), (204, 2), (205, 2), (301, 2), (302, 3)", 5);
                        MaterializedResult actualFromAnotherTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
                        assertEqualsIgnoreOrder(actualFromAnotherTransaction, expectedBefore);
                        MaterializedResult actualFromCurrentTransaction = computeActual(transactionSession, "SELECT * FROM tmp_delete_insert");
                        assertEqualsIgnoreOrder(actualFromCurrentTransaction, expectedAfter);
                        rollback();
                    });
        }
        catch (RollbackException e) {
            // ignore
        }

        MaterializedResult actualAfterRollback = computeActual(session, "SELECT * FROM tmp_delete_insert");
        assertEqualsIgnoreOrder(actualAfterRollback, expectedBefore);

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    assertUpdate(transactionSession, "DELETE FROM tmp_delete_insert WHERE z >= 2");
                    assertUpdate(transactionSession, "INSERT INTO tmp_delete_insert VALUES (203, 2), (204, 2), (205, 2), (301, 2), (302, 3)", 5);
                    MaterializedResult actualOutOfTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
                    assertEqualsIgnoreOrder(actualOutOfTransaction, expectedBefore);
                    MaterializedResult actualInTransaction = computeActual(transactionSession, "SELECT * FROM tmp_delete_insert");
                    assertEqualsIgnoreOrder(actualInTransaction, expectedAfter);
                });

        MaterializedResult actualAfterTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
        assertEqualsIgnoreOrder(actualAfterTransaction, expectedAfter);
    }

    @Test
    public void testCreateAndInsert()
    {
        Session session = getSession();

        List<MaterializedRow> expected = MaterializedResult.resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(201L, 2L)
                .row(202L, 2L)
                .row(301L, 3L)
                .row(302L, 3L)
                .build()
                .getMaterializedRows();

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    assertUpdate(
                            transactionSession,
                            "CREATE TABLE tmp_create_insert WITH (partitioned_by=array ['z']) AS " +
                                    "SELECT * from (VALUES (CAST (101 AS BIGINT), CAST (1 AS BIGINT)), (201, 2), (202, 2)) t(a, z)",
                            3);
                    assertUpdate(transactionSession, "INSERT INTO tmp_create_insert VALUES (301, 3), (302, 3)", 2);
                    MaterializedResult actualFromCurrentTransaction = computeActual(transactionSession, "SELECT * FROM tmp_create_insert");
                    assertEqualsIgnoreOrder(actualFromCurrentTransaction, expected);
                });

        MaterializedResult actualAfterTransaction = computeActual(session, "SELECT * FROM tmp_create_insert");
        assertEqualsIgnoreOrder(actualAfterTransaction, expected);
    }

    @Test
    public void testCreateUnpartitionedTableAndQuery()
    {
        Session session = getSession();

        List<MaterializedRow> expected = MaterializedResult.resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(201L, 2L)
                .row(202L, 2L)
                .row(301L, 3L)
                .row(302L, 3L)
                .build()
                .getMaterializedRows();

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    assertUpdate(
                            transactionSession,
                            "CREATE TABLE tmp_create_query AS " +
                                    "SELECT * from (VALUES (CAST (101 AS BIGINT), CAST (1 AS BIGINT)), (201, 2), (202, 2), (301, 3), (302, 3)) t(a, z)",
                            5);
                    MaterializedResult actualFromCurrentTransaction = computeActual(transactionSession, "SELECT * FROM tmp_create_query");
                    assertEqualsIgnoreOrder(actualFromCurrentTransaction, expected);
                });

        MaterializedResult actualAfterTransaction = computeActual(session, "SELECT * FROM tmp_create_query");
        assertEqualsIgnoreOrder(actualAfterTransaction, expected);
    }

    @Test
    public void testAddColumn()
    {
        assertUpdate("CREATE TABLE test_add_column (a bigint COMMENT 'test comment AAA')");
        assertUpdate("ALTER TABLE test_add_column ADD COLUMN b bigint COMMENT 'test comment BBB'");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN a varchar", ".* Column 'a' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN c bad_type", ".* Unknown type 'bad_type' for column 'c'");
        assertQuery("SHOW COLUMNS FROM test_add_column", "VALUES ('a', 'bigint', '', 'test comment AAA', 19, NULL, NULL), ('b', 'bigint', '', 'test comment BBB', 19, NULL, NULL)");
        assertUpdate("DROP TABLE test_add_column");
    }

    @Test
    public void testRenameColumn()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_rename_column\n" +
                "WITH (\n" +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN orderkey TO new_orderkey");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN \"$path\" TO test", ".* Cannot rename hidden column");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN orderstatus TO new_orderstatus", "Renaming partition columns is not supported");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders");
        assertUpdate("DROP TABLE test_rename_column");
    }

    @Test
    public void testDropColumn()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_drop_column\n" +
                "WITH (\n" +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT custkey, orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertQuery("SELECT orderkey, orderstatus FROM test_drop_column", "SELECT orderkey, orderstatus FROM orders");

        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN \"$path\"", ".* Cannot drop hidden column");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN orderstatus", "Cannot drop partition columns");
        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN orderkey");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN custkey", "Cannot drop the only non-partition column in a table");
        assertQuery("SELECT * FROM test_drop_column", "SELECT custkey, orderstatus FROM orders");

        assertUpdate("DROP TABLE test_drop_column");
    }

    @Test
    public void testAvroTypeValidation()
    {
        assertQueryFails("CREATE TABLE test_avro_types (x map(bigint, bigint)) WITH (format = 'AVRO')", "Column x has a non-varchar map key, which is not supported by Avro");
        assertQueryFails("CREATE TABLE test_avro_types (x tinyint) WITH (format = 'AVRO')", "Column x is tinyint, which is not supported by Avro. Use integer instead.");
        assertQueryFails("CREATE TABLE test_avro_types (x smallint) WITH (format = 'AVRO')", "Column x is smallint, which is not supported by Avro. Use integer instead.");

        assertQueryFails("CREATE TABLE test_avro_types WITH (format = 'AVRO') AS SELECT cast(42 AS smallint) z", "Column z is smallint, which is not supported by Avro. Use integer instead.");
    }

    @Test
    public void testOrderByChar()
    {
        assertUpdate("CREATE TABLE char_order_by (c_char char(2))");
        assertUpdate("INSERT INTO char_order_by (c_char) VALUES" +
                "(CAST('a' as CHAR(2)))," +
                "(CAST('a\0' as CHAR(2)))," +
                "(CAST('a  ' as CHAR(2)))", 3);

        MaterializedResult actual = computeActual(getSession(),
                "SELECT * FROM char_order_by ORDER BY c_char ASC");

        assertUpdate("DROP TABLE char_order_by");

        MaterializedResult expected = resultBuilder(getSession(), createCharType(2))
                .row("a\0")
                .row("a ")
                .row("a ")
                .build();

        assertEquals(actual, expected);
    }

    /**
     * Tests correctness of comparison of char(x) and varchar pushed down to a table scan as a TupleDomain
     */
    @Test
    public void testPredicatePushDownToTableScan()
    {
        // Test not specific to Hive, but needs a connector supporting table creation

        assertUpdate("CREATE TABLE test_table_with_char (a char(20))");
        try {
            assertUpdate("INSERT INTO test_table_with_char (a) VALUES" +
                    "(cast('aaa' as char(20)))," +
                    "(cast('bbb' as char(20)))," +
                    "(cast('bbc' as char(20)))," +
                    "(cast('bbd' as char(20)))", 4);

            assertQuery(
                    "SELECT a, a <= 'bbc' FROM test_table_with_char",
                    "VALUES (cast('aaa' as char(20)), true), " +
                            "(cast('bbb' as char(20)), true), " +
                            "(cast('bbc' as char(20)), true), " +
                            "(cast('bbd' as char(20)), false)");

            assertQuery(
                    "SELECT a FROM test_table_with_char WHERE a <= 'bbc'",
                    "VALUES cast('aaa' as char(20)), " +
                            "cast('bbb' as char(20)), " +
                            "cast('bbc' as char(20))");
        }
        finally {
            assertUpdate("DROP TABLE test_table_with_char");
        }
    }

    @Test
    public void testMismatchedBucketWithBucketPredicate()
    {
        try {
            assertUpdate(
                    "CREATE TABLE test_mismatch_bucketing_with_predicate_8\n" +
                            "WITH (bucket_count = 8, bucketed_by = ARRAY['key8']) AS\n" +
                            "SELECT custkey key8, comment value8 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_mismatch_bucketing_with_predicate_32\n" +
                            "WITH (bucket_count = 32, bucketed_by = ARRAY['key32']) AS\n" +
                            "SELECT custkey key32, comment value32 FROM orders",
                    15000);

            Session withMismatchOptimization = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setCatalogSessionProperty(catalog, "optimize_mismatched_bucket_count", "true")
                    .build();
            Session withoutMismatchOptimization = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setCatalogSessionProperty(catalog, "optimize_mismatched_bucket_count", "false")
                    .build();

            @Language("SQL") String query = "SELECT count(*) AS count\n" +
                    "FROM (\n" +
                    "  SELECT key32\n" +
                    "  FROM test_mismatch_bucketing_with_predicate_32\n" +
                    "  WHERE \"$bucket\" between 16 AND 31\n" +
                    ") a\n" +
                    "JOIN test_mismatch_bucketing_with_predicate_8 b\n" +
                    "ON a.key32 = b.key8";

            assertQuery(withMismatchOptimization, query, "SELECT 130361");
            assertQuery(withoutMismatchOptimization, query, "SELECT 130361");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_with_predicate_8");
            assertUpdate("DROP TABLE IF EXISTS test_mismatch_bucketing_with_predicate_32");
        }
    }

    @Test
    public void testMismatchedBucketing()
    {
        testMismatchedBucketing(getSession());
        testMismatchedBucketing(materializeExchangesSession);
    }

    public void testMismatchedBucketing(Session session)
    {
        try {
            assertUpdate(
                    session,
                    "CREATE TABLE test_mismatch_bucketing16\n" +
                            "WITH (bucket_count = 16, bucketed_by = ARRAY['key16']) AS\n" +
                            "SELECT orderkey key16, comment value16 FROM orders",
                    15000);
            assertUpdate(
                    session,
                    "CREATE TABLE test_mismatch_bucketing32\n" +
                            "WITH (bucket_count = 32, bucketed_by = ARRAY['key32']) AS\n" +
                            "SELECT orderkey key32, comment value32 FROM orders",
                    15000);
            assertUpdate(
                    session,
                    "CREATE TABLE test_mismatch_bucketingN AS\n" +
                            "SELECT orderkey keyN, comment valueN FROM orders",
                    15000);

            Session withMismatchOptimization = Session.builder(session)
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setCatalogSessionProperty(catalog, "optimize_mismatched_bucket_count", "true")
                    .build();
            Session withoutMismatchOptimization = Session.builder(session)
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setCatalogSessionProperty(catalog, "optimize_mismatched_bucket_count", "false")
                    .build();

            @Language("SQL") String writeToTableWithMoreBuckets = "CREATE TABLE test_mismatch_bucketing_out32\n" +
                    "WITH (bucket_count = 32, bucketed_by = ARRAY['key16'])\n" +
                    "AS\n" +
                    "SELECT key16, value16, key32, value32, keyN, valueN\n" +
                    "FROM\n" +
                    "  test_mismatch_bucketing16\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketing32\n" +
                    "ON key16=key32\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketingN\n" +
                    "ON key16=keyN";
            @Language("SQL") String writeToTableWithFewerBuckets = "CREATE TABLE test_mismatch_bucketing_out8\n" +
                    "WITH (bucket_count = 8, bucketed_by = ARRAY['key16'])\n" +
                    "AS\n" +
                    "SELECT key16, value16, key32, value32, keyN, valueN\n" +
                    "FROM\n" +
                    "  test_mismatch_bucketing16\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketing32\n" +
                    "ON key16=key32\n" +
                    "JOIN\n" +
                    "  test_mismatch_bucketingN\n" +
                    "ON key16=keyN";

            assertUpdate(withoutMismatchOptimization, writeToTableWithMoreBuckets, 15000, assertRemoteExchangesCount(3));
            assertQuery(withoutMismatchOptimization, "SELECT * FROM test_mismatch_bucketing_out32", "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders");
            assertUpdate(withoutMismatchOptimization, "DROP TABLE IF EXISTS test_mismatch_bucketing_out32");

            assertUpdate(withMismatchOptimization, writeToTableWithMoreBuckets, 15000, assertRemoteExchangesCount(2));
            assertQuery(withMismatchOptimization, "SELECT * FROM test_mismatch_bucketing_out32", "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders");

            assertUpdate(withMismatchOptimization, writeToTableWithFewerBuckets, 15000, assertRemoteExchangesCount(2));
            assertQuery(withMismatchOptimization, "SELECT * FROM test_mismatch_bucketing_out8", "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders");
        }
        finally {
            assertUpdate(session, "DROP TABLE IF EXISTS test_mismatch_bucketing16");
            assertUpdate(session, "DROP TABLE IF EXISTS test_mismatch_bucketing32");
            assertUpdate(session, "DROP TABLE IF EXISTS test_mismatch_bucketingN");
            assertUpdate(session, "DROP TABLE IF EXISTS test_mismatch_bucketing_out32");
            assertUpdate(session, "DROP TABLE IF EXISTS test_mismatch_bucketing_out8");
        }
    }

    private Session noReorderJoins(Session session)
    {
        return Session.builder(session)
                .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                .build();
    }

    @Test
    public void testPartialMergePushdown()
    {
        testPartialMergePushdown(noReorderJoins(getSession()));
        testPartialMergePushdown(noReorderJoins(materializeExchangesSession));
    }

    public void testPartialMergePushdown(Session session)
    {
        try {
            assertUpdate(
                    "CREATE TABLE test_partial_merge_pushdown_16buckets\n" +
                            "WITH (bucket_count = 16, bucketed_by = ARRAY['key16']) AS\n" +
                            "SELECT orderkey key16, comment value16 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_partial_merge_pushdown_32buckets\n" +
                            "WITH (bucket_count = 32, bucketed_by = ARRAY['key32']) AS\n" +
                            "SELECT orderkey key32, comment value32 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_partial_merge_pushdown_nobucket AS\n" +
                            "SELECT orderkey key_nobucket, comment value_nobucket FROM orders",
                    15000);

            Session withPartialMergePushdownOptimization = Session.builder(session)
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(PARTIAL_MERGE_PUSHDOWN_STRATEGY, PUSH_THROUGH_LOW_MEMORY_OPERATORS.name())
                    .build();
            Session withoutPartialMergePushdownOptimization = Session.builder(session)
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(PARTIAL_MERGE_PUSHDOWN_STRATEGY, PartialMergePushdownStrategy.NONE.name())
                    .build();

            //
            // join and write to bucketed table
            // ================================
            @Language("SQL") String joinAndWriteToTableWithSameBuckets = "CREATE TABLE test_partial_merge_pushdown_out16\n" +
                    "WITH (bucket_count = 16, bucketed_by = ARRAY['key16'])\n" +
                    "AS\n" +
                    "SELECT key16, value16, key32, value32, key_nobucket, value_nobucket\n" +
                    "FROM\n" +
                    "  test_partial_merge_pushdown_16buckets\n" +
                    "JOIN\n" +
                    "  test_partial_merge_pushdown_32buckets\n" +
                    "ON key16=key32\n" +
                    "JOIN\n" +
                    "  test_partial_merge_pushdown_nobucket\n" +
                    "ON key16=key_nobucket";
            @Language("SQL") String joinAndWriteToTableWithFewerBuckets = "CREATE TABLE test_partial_merge_pushdown_out8\n" +
                    "WITH (bucket_count = 8, bucketed_by = ARRAY['key16'])\n" +
                    "AS\n" +
                    "SELECT key16, value16, key32, value32, key_nobucket, value_nobucket\n" +
                    "FROM\n" +
                    "  test_partial_merge_pushdown_16buckets\n" +
                    "JOIN\n" +
                    "  test_partial_merge_pushdown_32buckets\n" +
                    "ON key16=key32\n" +
                    "JOIN\n" +
                    "  test_partial_merge_pushdown_nobucket\n" +
                    "ON key16=key_nobucket";

            assertUpdate(withoutPartialMergePushdownOptimization, joinAndWriteToTableWithSameBuckets, 15000, assertRemoteExchangesCount(3));
            assertQuery("SELECT * FROM test_partial_merge_pushdown_out16", "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders");
            assertUpdate("DROP TABLE IF EXISTS test_partial_merge_pushdown_out16");

            assertUpdate(withPartialMergePushdownOptimization, joinAndWriteToTableWithSameBuckets, 15000, assertRemoteExchangesCount(2));
            assertQuery("SELECT * FROM test_partial_merge_pushdown_out16", "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders");
            assertUpdate("DROP TABLE IF EXISTS test_partial_merge_pushdown_out16");

            assertUpdate(withoutPartialMergePushdownOptimization, joinAndWriteToTableWithFewerBuckets, 15000, assertRemoteExchangesCount(4));
            assertQuery("SELECT * FROM test_partial_merge_pushdown_out8", "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders");
            assertUpdate("DROP TABLE IF EXISTS test_partial_merge_pushdown_out8");

            // cannot pushdown the partial merge for TableWrite
            assertUpdate(withPartialMergePushdownOptimization, joinAndWriteToTableWithFewerBuckets, 15000, assertRemoteExchangesCount(3));
            assertQuery("SELECT * FROM test_partial_merge_pushdown_out8", "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders");
            assertUpdate("DROP TABLE IF EXISTS test_partial_merge_pushdown_out8");

            //
            // read and write to bucketed table
            // =====================================
            @Language("SQL") String readAndWriteToTableWithFewerBuckets = "CREATE TABLE test_partial_merge_pushdown_out8\n" +
                    "WITH (bucket_count = 8, bucketed_by = ARRAY['key16'])\n" +
                    "AS\n" +
                    "SELECT key16, value16\n" +
                    "FROM\n" +
                    "  test_partial_merge_pushdown_16buckets";

            assertUpdate(withoutPartialMergePushdownOptimization, readAndWriteToTableWithFewerBuckets, 15000, assertRemoteExchangesCount(2));
            assertQuery("SELECT * FROM test_partial_merge_pushdown_out8", "SELECT orderkey, comment from orders");
            assertUpdate("DROP TABLE IF EXISTS test_partial_merge_pushdown_out8");

            assertUpdate(withPartialMergePushdownOptimization, readAndWriteToTableWithFewerBuckets, 15000, assertRemoteExchangesCount(1));
            assertQuery("SELECT * FROM test_partial_merge_pushdown_out8", "SELECT orderkey, comment from orders");
            assertUpdate("DROP TABLE IF EXISTS test_partial_merge_pushdown_out8");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_partial_merge_pushdown_16buckets");
            assertUpdate("DROP TABLE IF EXISTS test_partial_merge_pushdown_32buckets");
            assertUpdate("DROP TABLE IF EXISTS test_partial_merge_pushdown_nobucket");
            assertUpdate("DROP TABLE IF EXISTS test_partial_merge_pushdown_out16");
            assertUpdate("DROP TABLE IF EXISTS test_partial_merge_pushdown_out8");
        }
    }

    @Test
    public void testMaterializedPartitioning()
    {
        testMaterializedPartitioning(Session.builder(materializeExchangesSession).setSystemProperty("max_concurrent_materializations", "1").build());
        testMaterializedPartitioning(Session.builder(materializeExchangesSession).setSystemProperty("max_concurrent_materializations", "2").build());
        testMaterializedPartitioning(materializeExchangesSession);

        // verify error messages
        Session materializeAllDefaultPartitioningProvider = Session.builder(getSession())
                .setSystemProperty(EXCHANGE_MATERIALIZATION_STRATEGY, "ALL")
                .build();
        assertQueryFails(
                materializeAllDefaultPartitioningProvider,
                "SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey",
                "The \"partitioning_provider_catalog\" session property must be set to enable the exchanges materialization\\. " +
                        "The catalog must support providing a custom partitioning and storing temporary tables\\.");
        Session materializeAllWrongPartitioningProvider = Session.builder(getSession())
                .setSystemProperty(EXCHANGE_MATERIALIZATION_STRATEGY, "ALL")
                .setSystemProperty(PARTITIONING_PROVIDER_CATALOG, "tpch")
                .build();
        assertQueryFails(
                materializeAllWrongPartitioningProvider,
                "SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey",
                "Catalog \"tpch\" cannot be used as a partitioning provider: This connector does not support custom partitioning");

        // make sure that bucketing is not ignored for temporary tables
        Session bucketingIgnored = Session.builder(materializeExchangesSession)
                .setCatalogSessionProperty(catalog, "ignore_table_bucketing", "true")
                .build();
        assertQuery(bucketingIgnored, "SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey", assertRemoteMaterializedExchangesCount(1));

        Session bucketingExecutionDisabled = Session.builder(materializeExchangesSession)
                .setCatalogSessionProperty(catalog, "bucket_execution_enabled", "false")
                .build();
        assertQuery(bucketingExecutionDisabled, "SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey", assertRemoteMaterializedExchangesCount(1));
    }

    private void testMaterializedPartitioning(Session materializeExchangesSession)
    {
        // Simple smoke tests for materialized partitioning
        // Comprehensive testing is done by TestHiveDistributedAggregationsWithExchangeMaterialization, TestHiveDistributedQueriesWithExchangeMaterialization

        // simple aggregation
        assertQuery(materializeExchangesSession, "SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey", assertRemoteMaterializedExchangesCount(1));
        // simple distinct
        assertQuery(materializeExchangesSession, "SELECT distinct orderkey FROM lineitem", assertRemoteMaterializedExchangesCount(1));
        // more complex aggregation
        assertQuery(materializeExchangesSession, "SELECT custkey, orderstatus, COUNT(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus", assertRemoteMaterializedExchangesCount(2));
        // mark distinct
        assertQuery(
                materializeExchangesSession,
                "SELECT custkey, COUNT(DISTINCT orderstatus), COUNT(DISTINCT orderkey) FROM orders GROUP BY custkey",
                assertRemoteMaterializedExchangesCount(3)
                        // make sure that the count distinct has been planned as a MarkDistinctNode
                        .andThen(plan -> assertTrue(searchFrom(plan.getRoot()).where(node -> node instanceof MarkDistinctNode).matches())));

        // join
        assertQuery(materializeExchangesSession, "SELECT * FROM (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) x", assertRemoteMaterializedExchangesCount(2));

        // 3-way join
        try {
            assertUpdate("CREATE TABLE test_orders_part1 AS SELECT orderkey, totalprice FROM orders", "SELECT count(*) FROM orders");
            assertUpdate("CREATE TABLE test_orders_part2 AS SELECT orderkey, comment FROM orders", "SELECT count(*) FROM orders");

            assertQuery(
                    materializeExchangesSession,
                    "SELECT lineitem.orderkey, lineitem.comment, test_orders_part1.totalprice, test_orders_part2.comment ordercomment\n" +
                            "FROM lineitem JOIN test_orders_part1\n" +
                            "ON lineitem.orderkey = test_orders_part1.orderkey\n" +
                            "JOIN test_orders_part2\n" +
                            "ON lineitem.orderkey = test_orders_part2.orderkey",
                    "SELECT lineitem.orderkey, lineitem.comment, orders.totalprice, orders.comment ordercomment\n" +
                            "FROM lineitem JOIN orders\n" +
                            "ON lineitem.orderkey = orders.orderkey",
                    assertRemoteMaterializedExchangesCount(3));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_orders_part1");
            assertUpdate("DROP TABLE IF EXISTS test_orders_part2");
        }

        try {
            // join a bucketed table with an unbucketed table
            assertUpdate(
                    // bucket count has to be different from materialized bucket number
                    "CREATE TABLE test_bucketed_lineitem1\n" +
                            "WITH (bucket_count = 17, bucketed_by = ARRAY['orderkey']) AS\n" +
                            "SELECT * FROM lineitem",
                    "SELECT count(*) from lineitem");
            // bucketed table as probe side
            assertQuery(
                    materializeExchangesSession,
                    "SELECT * FROM test_bucketed_lineitem1 JOIN orders ON test_bucketed_lineitem1.orderkey = orders.orderkey",
                    "SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                    assertRemoteMaterializedExchangesCount(1));
            // unbucketed table as probe side
            assertQuery(
                    materializeExchangesSession,
                    "SELECT * FROM orders JOIN test_bucketed_lineitem1 ON test_bucketed_lineitem1.orderkey = orders.orderkey",
                    "SELECT * FROM orders JOIN lineitem ON lineitem.orderkey = orders.orderkey",
                    assertRemoteMaterializedExchangesCount(1));

            // join a bucketed table with an unbucketed table; the join has constant pushdown
            assertUpdate(
                    // bucket count has to be different from materialized bucket number
                    "CREATE TABLE test_bucketed_lineitem2\n" +
                            "WITH (bucket_count = 17, bucketed_by = ARRAY['partkey', 'suppkey']) AS\n" +
                            "SELECT * FROM lineitem",
                    "SELECT count(*) from lineitem");
            // bucketed table as probe side
            assertQuery(
                    materializeExchangesSession,
                    "SELECT * \n" +
                            "FROM test_bucketed_lineitem2 JOIN partsupp\n" +
                            "ON test_bucketed_lineitem2.partkey = partsupp.partkey AND\n" +
                            "test_bucketed_lineitem2.suppkey = partsupp.suppkey\n" +
                            "WHERE test_bucketed_lineitem2.suppkey = 42",
                    "SELECT * \n" +
                            "FROM lineitem JOIN partsupp\n" +
                            "ON lineitem.partkey = partsupp.partkey AND\n" +
                            "lineitem.suppkey = partsupp.suppkey\n" +
                            "WHERE lineitem.suppkey = 42",
                    assertRemoteMaterializedExchangesCount(1));
            // unbucketed table as probe side
            assertQuery(
                    materializeExchangesSession,
                    "SELECT * \n" +
                            "FROM partsupp JOIN test_bucketed_lineitem2\n" +
                            "ON test_bucketed_lineitem2.partkey = partsupp.partkey AND\n" +
                            "test_bucketed_lineitem2.suppkey = partsupp.suppkey\n" +
                            "WHERE test_bucketed_lineitem2.suppkey = 42",
                    "SELECT * \n" +
                            "FROM partsupp JOIN lineitem\n" +
                            "ON lineitem.partkey = partsupp.partkey AND\n" +
                            "lineitem.suppkey = partsupp.suppkey\n" +
                            "WHERE lineitem.suppkey = 42",
                    assertRemoteMaterializedExchangesCount(1));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_bucketed_lineitem1");
            assertUpdate("DROP TABLE IF EXISTS test_bucketed_lineitem2");
        }

        // Window functions
        assertQuery(
                materializeExchangesSession,
                "SELECT sum(rn) FROM (SELECT row_number() OVER(PARTITION BY orderkey ORDER BY linenumber) as rn FROM lineitem) WHERE rn > 5",
                "SELECT 41137",
                assertRemoteMaterializedExchangesCount(1)
                        // make sure that the window function has been planned as a WindowNode
                        .andThen(plan -> assertTrue(searchFrom(plan.getRoot()).where(node -> node instanceof WindowNode).matches())));
        assertQuery(
                materializeExchangesSession,
                "SELECT sum(rn) FROM (SELECT row_number() OVER(PARTITION BY orderkey) as rn FROM lineitem)",
                "SELECT 180782",
                assertRemoteMaterializedExchangesCount(1)
                        // make sure that the window function has been planned as a RowNumberNode
                        .andThen(plan -> assertTrue(searchFrom(plan.getRoot()).where(node -> node instanceof RowNumberNode).matches())));
        assertQuery(
                materializeExchangesSession,
                "SELECT sum(rn) FROM (SELECT row_number() OVER(PARTITION BY orderkey ORDER BY linenumber) as rn FROM lineitem) WHERE rn < 5",
                "SELECT 107455",
                assertRemoteMaterializedExchangesCount(1)
                        // make sure that the window function has been planned as a TopNRowNumberNode
                        .andThen(plan -> assertTrue(searchFrom(plan.getRoot()).where(node -> node instanceof TopNRowNumberNode).matches())));

        // union
        assertQuery(
                materializeExchangesSession,
                "SELECT partkey, count(*), sum(cost) " +
                        "FROM ( " +
                        "  SELECT partkey, CAST(extendedprice AS BIGINT) cost FROM lineitem " +
                        "  UNION ALL " +
                        "  SELECT partkey, CAST(supplycost AS BIGINT) cost FROM partsupp " +
                        ") " +
                        "GROUP BY partkey",
                assertRemoteMaterializedExchangesCount(2));

        // union over aggregation + broadcast join
        Session broadcastJoinMaterializeExchangesSession = Session.builder(materializeExchangesSession)
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
        Session broadcastJoinStreamingExchangesSession = Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();

        // compatible union partitioning
        assertQuery(
                broadcastJoinMaterializeExchangesSession,
                "WITH union_of_aggregations as ( " +
                        "    SELECT " +
                        "        partkey, " +
                        "        count(*) AS value " +
                        "    FROM lineitem " +
                        "    GROUP BY  " +
                        "        1 " +
                        "    UNION ALL " +
                        "    SELECT " +
                        "        partkey, " +
                        "        sum(suppkey) AS value " +
                        "    FROM lineitem " +
                        "    GROUP BY  " +
                        "        1        " +
                        ") " +
                        "SELECT " +
                        "    sum(a.value + b.value) " +
                        "FROM union_of_aggregations a, union_of_aggregations b  " +
                        "WHERE a.partkey = b.partkey ",
                "SELECT 12404708",
                assertRemoteExchangesCount(6)
                        .andThen(assertRemoteMaterializedExchangesCount(4)));

        // incompatible union partitioning, requires an extra remote exchange for build and probe
        String incompatiblePartitioningQuery = "WITH union_of_aggregations as ( " +
                "    SELECT " +
                "        partkey, " +
                "        count(*) as value " +
                "    FROM lineitem " +
                "    GROUP BY  " +
                "        1 " +
                "    UNION ALL " +
                "    SELECT " +
                "        partkey, " +
                "        suppkey as value " +
                "    FROM lineitem " +
                "    GROUP BY  " +
                "        1, 2        " +
                ") " +
                "SELECT " +
                "    sum(a.value + b.value) " +
                "FROM union_of_aggregations a, union_of_aggregations b  " +
                "WHERE a.partkey = b.partkey ";

        // system partitioning handle is always compatible
        assertQuery(
                broadcastJoinStreamingExchangesSession,
                incompatiblePartitioningQuery,
                "SELECT 4639006",
                assertRemoteExchangesCount(6));

        // hive partitioning handle is incompatible
        assertQuery(
                broadcastJoinMaterializeExchangesSession,
                incompatiblePartitioningQuery,
                "SELECT 4639006",
                assertRemoteExchangesCount(8)
                        .andThen(assertRemoteMaterializedExchangesCount(4)));
    }

    public static Consumer<Plan> assertRemoteMaterializedExchangesCount(int expectedRemoteExchangesCount)
    {
        return plan ->
        {
            int actualRemoteExchangesCount = searchFrom(plan.getRoot())
                    .where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope() == REMOTE_MATERIALIZED)
                    .findAll()
                    .size();
            if (actualRemoteExchangesCount != expectedRemoteExchangesCount) {
                throw new AssertionError(format(
                        "Expected [%s] materialized exchanges but found [%s] materialized exchanges.",
                        expectedRemoteExchangesCount,
                        actualRemoteExchangesCount));
            }
        };
    }

    @Test
    public void testGroupedExecution()
    {
        testGroupedExecution(getSession());
        testGroupedExecution(materializeExchangesSession);
    }

    private void testGroupedExecution(Session session)
    {
        try {
            assertUpdate(
                    session,
                    "CREATE TABLE test_grouped_join1\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                            "SELECT orderkey key1, comment value1 FROM orders",
                    15000);
            assertUpdate(
                    session,
                    "CREATE TABLE test_grouped_join2\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                            "SELECT orderkey key2, comment value2 FROM orders",
                    15000);
            assertUpdate(
                    session,
                    "CREATE TABLE test_grouped_join3\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                            "SELECT orderkey key3, comment value3 FROM orders",
                    15000);
            assertUpdate(
                    session,
                    "CREATE TABLE test_grouped_join4\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key4_bucket']) AS\n" +
                            "SELECT orderkey key4_bucket, orderkey key4_non_bucket, comment value4 FROM orders",
                    15000);
            assertUpdate(
                    session,
                    "CREATE TABLE test_grouped_joinN AS\n" +
                            "SELECT orderkey keyN, comment valueN FROM orders",
                    15000);
            assertUpdate(
                    session,
                    "CREATE TABLE test_grouped_joinDual\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['keyD']) AS\n" +
                            "SELECT orderkey keyD, comment valueD FROM orders CROSS JOIN UNNEST(repeat(NULL, 2))",
                    30000);
            assertUpdate(
                    session,
                    "CREATE TABLE test_grouped_window\n" +
                            "WITH (bucket_count = 5, bucketed_by = ARRAY['key']) AS\n" +
                            "SELECT custkey key, orderkey value FROM orders WHERE custkey <= 5 ORDER BY orderkey LIMIT 10",
                    10);

            // NO colocated join, no grouped execution
            Session notColocatedNotGrouped = Session.builder(session)
                    .setSystemProperty(COLOCATED_JOIN, "false")
                    .setSystemProperty(GROUPED_EXECUTION, "false")
                    .build();

            // colocated join, no grouped execution
            Session colocatedNotGrouped = Session.builder(session)
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "false")
                    .build();

            // No colocated join, grouped execution
            Session notColocatedGrouped = Session.builder(session)
                    .setSystemProperty(COLOCATED_JOIN, "false")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .build();

            // Co-located JOIN with all groups at once
            Session colocatedAllGroupsAtOnce = Session.builder(session)
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "0")
                    .build();
            // Co-located JOIN, 1 group per worker at a time
            Session colocatedOneGroupAtATime = Session.builder(session)
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .build();
            // Broadcast JOIN, 1 group per worker at a time
            Session broadcastOneGroupAtATime = Session.builder(session)
                    .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .build();

            //
            // HASH JOIN
            // =========

            @Language("SQL") String joinThreeBucketedTable =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "JOIN test_grouped_join3\n" +
                            "ON key2 = key3";
            @Language("SQL") String joinThreeMixedTable =
                    "SELECT key1, value1, key2, value2, keyN, valueN\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "JOIN test_grouped_joinN\n" +
                            "ON key2 = keyN";
            @Language("SQL") String expectedJoinQuery = "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders";
            @Language("SQL") String leftJoinBucketedTable =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM test_grouped_join1\n" +
                            "LEFT JOIN (SELECT * FROM test_grouped_join2 WHERE key2 % 2 = 0)\n" +
                            "ON key1 = key2";
            @Language("SQL") String rightJoinBucketedTable =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM (SELECT * FROM test_grouped_join2 WHERE key2 % 2 = 0)\n" +
                            "RIGHT JOIN test_grouped_join1\n" +
                            "ON key1 = key2";
            @Language("SQL") String expectedOuterJoinQuery = "SELECT orderkey, comment, CASE mod(orderkey, 2) WHEN 0 THEN orderkey END, CASE mod(orderkey, 2) WHEN 0 THEN comment END from orders";

            assertQuery(notColocatedNotGrouped, joinThreeBucketedTable, expectedJoinQuery);
            assertQuery(notColocatedNotGrouped, leftJoinBucketedTable, expectedOuterJoinQuery);
            assertQuery(notColocatedNotGrouped, rightJoinBucketedTable, expectedOuterJoinQuery);
            assertQuery(notColocatedGrouped, joinThreeBucketedTable, expectedJoinQuery);
            assertQuery(notColocatedGrouped, leftJoinBucketedTable, expectedOuterJoinQuery);
            assertQuery(notColocatedGrouped, rightJoinBucketedTable, expectedOuterJoinQuery);

            assertQuery(colocatedNotGrouped, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedNotGrouped, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnce, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, joinThreeBucketedTable, expectedJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, joinThreeMixedTable, expectedJoinQuery, assertRemoteExchangesCount(2));

            assertQuery(colocatedNotGrouped, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedNotGrouped, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, leftJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, rightJoinBucketedTable, expectedOuterJoinQuery, assertRemoteExchangesCount(1));

            //
            // CROSS JOIN and HASH JOIN mixed
            // ==============================

            @Language("SQL") String crossJoin =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "CROSS JOIN (SELECT * FROM test_grouped_join3 WHERE key3 <= 3)";
            @Language("SQL") String expectedCrossJoinQuery =
                    "SELECT key1, value1, key1, value1, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT orderkey key1, comment value1 FROM orders)\n" +
                            "CROSS JOIN\n" +
                            "  (SELECT orderkey key3, comment value3 FROM orders where orderkey <= 3)";
            assertQuery(notColocatedNotGrouped, crossJoin, expectedCrossJoinQuery);
            assertQuery(notColocatedGrouped, crossJoin, expectedCrossJoinQuery);
            assertQuery(colocatedNotGrouped, crossJoin, expectedCrossJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnce, crossJoin, expectedCrossJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, crossJoin, expectedCrossJoinQuery, assertRemoteExchangesCount(2));

            //
            // Bucketed and unbucketed HASH JOIN mixed
            // =======================================
            @Language("SQL") String bucketedAndUnbucketedJoin =
                    "SELECT key1, value1, keyN, valueN, key2, value2, key3, value3\n" +
                            "FROM\n" +
                            "  test_grouped_join1\n" +
                            "JOIN (\n" +
                            "  SELECT *\n" +
                            "  FROM test_grouped_joinN\n" +
                            "  JOIN test_grouped_join2\n" +
                            "  ON keyN = key2\n" +
                            ")\n" +
                            "ON key1 = keyN\n" +
                            "JOIN test_grouped_join3\n" +
                            "ON key1 = key3";
            @Language("SQL") String expectedBucketedAndUnbucketedJoinQuery = "SELECT orderkey, comment, orderkey, comment, orderkey, comment, orderkey, comment from orders";
            assertQuery(notColocatedNotGrouped, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery);
            assertQuery(notColocatedGrouped, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery);
            assertQuery(colocatedNotGrouped, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnce, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, bucketedAndUnbucketedJoin, expectedBucketedAndUnbucketedJoinQuery, assertRemoteExchangesCount(2));

            //
            // UNION ALL / GROUP BY
            // ====================

            @Language("SQL") String groupBySingleBucketed =
                    "SELECT\n" +
                            "  keyD,\n" +
                            "  count(valueD)\n" +
                            "FROM\n" +
                            "  test_grouped_joinDual\n" +
                            "GROUP BY keyD";
            @Language("SQL") String expectedSingleGroupByQuery = "SELECT orderkey, 2 from orders";
            @Language("SQL") String groupByOfUnionBucketed =
                    "SELECT\n" +
                            "  key\n" +
                            ", arbitrary(value1)\n" +
                            ", arbitrary(value2)\n" +
                            ", arbitrary(value3)\n" +
                            "FROM (\n" +
                            "  SELECT key1 key, value1, NULL value2, NULL value3\n" +
                            "  FROM test_grouped_join1\n" +
                            "UNION ALL\n" +
                            "  SELECT key2 key, NULL value1, value2, NULL value3\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE key2 % 2 = 0\n" +
                            "UNION ALL\n" +
                            "  SELECT key3 key, NULL value1, NULL value2, value3\n" +
                            "  FROM test_grouped_join3\n" +
                            "  WHERE key3 % 3 = 0\n" +
                            ")\n" +
                            "GROUP BY key";
            @Language("SQL") String groupByOfUnionMixed =
                    "SELECT\n" +
                            "  key\n" +
                            ", arbitrary(value1)\n" +
                            ", arbitrary(value2)\n" +
                            ", arbitrary(valueN)\n" +
                            "FROM (\n" +
                            "  SELECT key1 key, value1, NULL value2, NULL valueN\n" +
                            "  FROM test_grouped_join1\n" +
                            "UNION ALL\n" +
                            "  SELECT key2 key, NULL value1, value2, NULL valueN\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE key2 % 2 = 0\n" +
                            "UNION ALL\n" +
                            "  SELECT keyN key, NULL value1, NULL value2, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            "  WHERE keyN % 3 = 0\n" +
                            ")\n" +
                            "GROUP BY key";
            @Language("SQL") String expectedGroupByOfUnion = "SELECT orderkey, comment, CASE mod(orderkey, 2) WHEN 0 THEN comment END, CASE mod(orderkey, 3) WHEN 0 THEN comment END from orders";
            // In this case:
            // * left side can take advantage of bucketed execution
            // * right side does not have the necessary organization to allow its parent to take advantage of bucketed execution
            // In this scenario, we give up bucketed execution altogether. This can potentially be improved.
            //
            //       AGG(key)
            //           |
            //       UNION ALL
            //      /         \
            //  AGG(key)  Scan (not bucketed)
            //     |
            // Scan (bucketed on key)
            @Language("SQL") String groupByOfUnionOfGroupByMixed =
                    "SELECT\n" +
                            "  key, sum(cnt) cnt\n" +
                            "FROM (\n" +
                            "  SELECT keyD key, count(valueD) cnt\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            "UNION ALL\n" +
                            "  SELECT keyN key, 1 cnt\n" +
                            "  FROM test_grouped_joinN\n" +
                            ")\n" +
                            "group by key";
            @Language("SQL") String expectedGroupByOfUnionOfGroupBy = "SELECT orderkey, 3 from orders";

            // Eligible GROUP BYs run in the same fragment regardless of colocated_join flag
            assertQuery(notColocatedGrouped, groupBySingleBucketed, expectedSingleGroupByQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, groupBySingleBucketed, expectedSingleGroupByQuery, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, groupBySingleBucketed, expectedSingleGroupByQuery, assertRemoteExchangesCount(1));
            assertQuery(notColocatedGrouped, groupByOfUnionBucketed, expectedGroupByOfUnion, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, groupByOfUnionBucketed, expectedGroupByOfUnion, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionBucketed, expectedGroupByOfUnion, assertRemoteExchangesCount(1));

            // cannot be executed in a grouped manner but should still produce correct result
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionMixed, expectedGroupByOfUnion, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionOfGroupByMixed, expectedGroupByOfUnionOfGroupBy, assertRemoteExchangesCount(2));

            //
            // GROUP BY and JOIN mixed
            // ========================
            @Language("SQL") String joinGroupedWithGrouped =
                    "SELECT key1, count1, count2\n" +
                            "FROM (\n" +
                            "  SELECT keyD key1, count(valueD) count1\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ") JOIN (\n" +
                            "  SELECT keyD key2, count(valueD) count2\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ")\n" +
                            "ON key1 = key2";
            @Language("SQL") String expectedJoinGroupedWithGrouped = "SELECT orderkey, 2, 2 from orders";
            @Language("SQL") String joinGroupedWithUngrouped =
                    "SELECT keyD, countD, valueN\n" +
                            "FROM (\n" +
                            "  SELECT keyD, count(valueD) countD\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ") JOIN (\n" +
                            "  SELECT keyN, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            ")\n" +
                            "ON keyD = keyN";
            @Language("SQL") String expectedJoinGroupedWithUngrouped = "SELECT orderkey, 2, comment from orders";
            @Language("SQL") String joinUngroupedWithGrouped =
                    "SELECT keyN, valueN, countD\n" +
                            "FROM (\n" +
                            "  SELECT keyN, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            ") JOIN (\n" +
                            "  SELECT keyD, count(valueD) countD\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ")\n" +
                            "ON keyN = keyD";
            @Language("SQL") String expectedJoinUngroupedWithGrouped = "SELECT orderkey, comment, 2 from orders";
            @Language("SQL") String groupOnJoinResult =
                    "SELECT keyD, count(valueD), count(valueN)\n" +
                            "FROM\n" +
                            "  test_grouped_joinDual\n" +
                            "JOIN\n" +
                            "  test_grouped_joinN\n" +
                            "ON keyD=keyN\n" +
                            "GROUP BY keyD";
            @Language("SQL") String expectedGroupOnJoinResult = "SELECT orderkey, 2, 2 from orders";

            @Language("SQL") String groupOnUngroupedJoinResult =
                    "SELECT key4_bucket, count(value4), count(valueN)\n" +
                            "FROM\n" +
                            "  test_grouped_join4\n" +
                            "JOIN\n" +
                            "  test_grouped_joinN\n" +
                            "ON key4_non_bucket=keyN\n" +
                            "GROUP BY key4_bucket";
            @Language("SQL") String expectedGroupOnUngroupedJoinResult = "SELECT orderkey, count(*), count(*) from orders group by orderkey";

            // Eligible GROUP BYs run in the same fragment regardless of colocated_join flag
            assertQuery(notColocatedGrouped, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnce, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped, assertRemoteExchangesCount(1));
            assertQuery(notColocatedGrouped, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnce, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped, assertRemoteExchangesCount(2));
            assertQuery(notColocatedGrouped, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnce, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, groupOnJoinResult, expectedGroupOnJoinResult, assertRemoteExchangesCount(2));

            assertQuery(broadcastOneGroupAtATime, groupOnUngroupedJoinResult, expectedGroupOnUngroupedJoinResult, assertRemoteExchangesCount(2));

            // cannot be executed in a grouped manner but should still produce correct result
            assertQuery(colocatedOneGroupAtATime, joinUngroupedWithGrouped, expectedJoinUngroupedWithGrouped, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, groupOnUngroupedJoinResult, expectedGroupOnUngroupedJoinResult, assertRemoteExchangesCount(4));

            //
            // Outer JOIN (that involves LookupOuterOperator)
            // ==============================================

            // Chain on the probe side to test duplicating OperatorFactory
            @Language("SQL") String chainedOuterJoin =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT * FROM test_grouped_join1 where mod(key1, 2) = 0)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_join2 where mod(key2, 3) = 0)\n" +
                            "ON key1 = key2\n" +
                            "FULL JOIN\n" +
                            "  (SELECT * FROM test_grouped_join3 where mod(key3, 5) = 0)\n" +
                            "ON key2 = key3";
            // Probe is grouped execution, but build is not
            @Language("SQL") String sharedBuildOuterJoin =
                    "SELECT key1, value1, keyN, valueN\n" +
                            "FROM\n" +
                            "  (SELECT key1, arbitrary(value1) value1 FROM test_grouped_join1 where mod(key1, 2) = 0 group by key1)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_joinN where mod(keyN, 3) = 0)\n" +
                            "ON key1 = keyN";
            // The preceding test case, which then feeds into another join
            @Language("SQL") String chainedSharedBuildOuterJoin =
                    "SELECT key1, value1, keyN, valueN, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT key1, arbitrary(value1) value1 FROM test_grouped_join1 where mod(key1, 2) = 0 group by key1)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_joinN where mod(keyN, 3) = 0)\n" +
                            "ON key1 = keyN\n" +
                            "FULL JOIN\n" +
                            "  (SELECT * FROM test_grouped_join3 where mod(key3, 5) = 0)\n" +
                            "ON keyN = key3";
            @Language("SQL") String expectedChainedOuterJoinResult = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 2 * 3) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 2 * 3) = 0 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 3) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 3) = 0 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 5) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 5) = 0 THEN comment END\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 3) = 0 OR mod(orderkey, 5) = 0";
            @Language("SQL") String expectedSharedBuildOuterJoinResult = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 2) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 2) = 0 THEN comment END,\n" +
                    "  orderkey,\n" +
                    "  comment\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 3) = 0";

            assertQuery(notColocatedNotGrouped, chainedOuterJoin, expectedChainedOuterJoinResult);
            assertQuery(notColocatedGrouped, chainedOuterJoin, expectedChainedOuterJoinResult);
            assertQuery(colocatedNotGrouped, chainedOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, chainedOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, chainedOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(1));
            assertQuery(notColocatedNotGrouped, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult);
            assertQuery(notColocatedGrouped, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult);
            assertQuery(colocatedNotGrouped, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedAllGroupsAtOnce, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, sharedBuildOuterJoin, expectedSharedBuildOuterJoinResult, assertRemoteExchangesCount(2));
            assertQuery(colocatedOneGroupAtATime, chainedSharedBuildOuterJoin, expectedChainedOuterJoinResult, assertRemoteExchangesCount(2));

            //
            // Window function
            // ===============
            assertQuery(
                    colocatedOneGroupAtATime,
                    "SELECT key, count(*) OVER (PARTITION BY key ORDER BY value) FROM test_grouped_window",
                    "VALUES\n" +
                            "(1, 1),\n" +
                            "(2, 1),\n" +
                            "(2, 2),\n" +
                            "(4, 1),\n" +
                            "(4, 2),\n" +
                            "(4, 3),\n" +
                            "(4, 4),\n" +
                            "(4, 5),\n" +
                            "(5, 1),\n" +
                            "(5, 2)",
                    assertRemoteExchangesCount(1));

            assertQuery(
                    colocatedOneGroupAtATime,
                    "SELECT key, row_number() OVER (PARTITION BY key ORDER BY value) FROM test_grouped_window",
                    "VALUES\n" +
                            "(1, 1),\n" +
                            "(2, 1),\n" +
                            "(2, 2),\n" +
                            "(4, 1),\n" +
                            "(4, 2),\n" +
                            "(4, 3),\n" +
                            "(4, 4),\n" +
                            "(4, 5),\n" +
                            "(5, 1),\n" +
                            "(5, 2)",
                    assertRemoteExchangesCount(1));

            assertQuery(
                    colocatedOneGroupAtATime,
                    "SELECT key, n FROM (SELECT key, row_number() OVER (PARTITION BY key ORDER BY value) AS n FROM test_grouped_window) WHERE n <= 2",
                    "VALUES\n" +
                            "(1, 1),\n" +
                            "(2, 1),\n" +
                            "(2, 2),\n" +
                            "(4, 1),\n" +
                            "(4, 2),\n" +
                            "(5, 1),\n" +
                            "(5, 2)",
                    assertRemoteExchangesCount(1));

            //
            // Filter out all or majority of splits
            // ====================================
            @Language("SQL") String noSplits =
                    "SELECT key1, arbitrary(value1)\n" +
                            "FROM test_grouped_join1\n" +
                            "WHERE \"$bucket\" < 0\n" +
                            "GROUP BY key1";
            @Language("SQL") String joinMismatchedBuckets =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM (\n" +
                            "  SELECT *\n" +
                            "  FROM test_grouped_join1\n" +
                            "  WHERE \"$bucket\"=1\n" +
                            ")\n" +
                            "FULL OUTER JOIN (\n" +
                            "  SELECT *\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE \"$bucket\"=11\n" +
                            ")\n" +
                            "ON key1=key2";
            @Language("SQL") String expectedNoSplits = "SELECT 1, 'a' WHERE FALSE";
            @Language("SQL") String expectedJoinMismatchedBuckets = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 13) = 1 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 13) = 1 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 13) = 11 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 13) = 11 THEN comment END\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 13) IN (1, 11)";

            assertQuery(notColocatedNotGrouped, noSplits, expectedNoSplits);
            assertQuery(notColocatedGrouped, noSplits, expectedNoSplits);
            assertQuery(colocatedNotGrouped, noSplits, expectedNoSplits, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, noSplits, expectedNoSplits, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, noSplits, expectedNoSplits, assertRemoteExchangesCount(1));
            assertQuery(notColocatedNotGrouped, joinMismatchedBuckets, expectedJoinMismatchedBuckets);
            assertQuery(notColocatedGrouped, joinMismatchedBuckets, expectedJoinMismatchedBuckets);
            assertQuery(colocatedNotGrouped, joinMismatchedBuckets, expectedJoinMismatchedBuckets, assertRemoteExchangesCount(1));
            assertQuery(colocatedAllGroupsAtOnce, joinMismatchedBuckets, expectedJoinMismatchedBuckets, assertRemoteExchangesCount(1));
            assertQuery(colocatedOneGroupAtATime, joinMismatchedBuckets, expectedJoinMismatchedBuckets, assertRemoteExchangesCount(1));
        }
        finally {
            assertUpdate(session, "DROP TABLE IF EXISTS test_grouped_join1");
            assertUpdate(session, "DROP TABLE IF EXISTS test_grouped_join2");
            assertUpdate(session, "DROP TABLE IF EXISTS test_grouped_join3");
            assertUpdate(session, "DROP TABLE IF EXISTS test_grouped_join4");
            assertUpdate(session, "DROP TABLE IF EXISTS test_grouped_joinN");
            assertUpdate(session, "DROP TABLE IF EXISTS test_grouped_joinDual");
            assertUpdate(session, "DROP TABLE IF EXISTS test_grouped_window");
        }
    }

    private Consumer<Plan> assertRemoteExchangesCount(int expectedRemoteExchangesCount)
    {
        return assertRemoteExchangesCount(expectedRemoteExchangesCount, getSession(), (DistributedQueryRunner) getQueryRunner());
    }

    public static Consumer<Plan> assertRemoteExchangesCount(int expectedRemoteExchangesCount, Session session, DistributedQueryRunner queryRunner)
    {
        return plan ->
        {
            int actualRemoteExchangesCount = searchFrom(plan.getRoot())
                    .where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope().isRemote())
                    .findAll()
                    .size();
            if (actualRemoteExchangesCount != expectedRemoteExchangesCount) {
                Metadata metadata = queryRunner.getCoordinator().getMetadata();
                String formattedPlan = textLogicalPlan(plan.getRoot(), plan.getTypes(), StatsAndCosts.empty(), metadata.getFunctionAndTypeManager(), session, 0);
                throw new AssertionError(format(
                        "Expected [\n%s\n] remote exchanges but found [\n%s\n] remote exchanges. Actual plan is [\n\n%s\n]",
                        expectedRemoteExchangesCount,
                        actualRemoteExchangesCount,
                        formattedPlan));
            }
        };
    }

    @Test
    public void testGroupedJoinWithUngroupedSemiJoin()
    {
        Session groupedExecutionSession = Session.builder(getSession())
                .setSystemProperty(COLOCATED_JOIN, "true")
                .setSystemProperty(GROUPED_EXECUTION, "true")
                .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "AUTOMATIC")
                .build();
        try {
            assertUpdate("CREATE TABLE big_bucketed_table \n" +
                            "WITH (bucket_count = 16, bucketed_by = ARRAY['key1']) AS\n" +
                            "SELECT orderkey key1 FROM orders",
                    15000);
            assertUpdate("CREATE TABLE small_unbucketed_table AS\n" +
                            "SELECT nationkey key2 FROM nation",
                    25);
            assertQuery(groupedExecutionSession,
                    "SELECT count(*) from big_bucketed_table t1 JOIN (SELECT key1 FROM big_bucketed_table where key1 IN (SELECT key2 from small_unbucketed_table)) t2 on t1.key1 = t2.key1 group by t1.key1",
                    "SELECT count(*) from orders where orderkey < 25 group by orderkey");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS big_bucketed_table");
            assertUpdate("DROP TABLE IF EXISTS small_unbucketed_table");
        }
    }

    @Test
    public void testRcTextCharDecoding()
    {
        testRcTextCharDecoding(false);
        testRcTextCharDecoding(true);
    }

    private void testRcTextCharDecoding(boolean rcFileOptimizedWriterEnabled)
    {
        String catalog = getSession().getCatalog().get();
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, RCFILE_OPTIMIZED_WRITER_ENABLED, Boolean.toString(rcFileOptimizedWriterEnabled))
                .build();

        assertUpdate(session, "CREATE TABLE test_table_with_char_rc WITH (format = 'RCTEXT') AS SELECT CAST('khaki' AS CHAR(7)) char_column", 1);
        try {
            assertQuery(session,
                    "SELECT * FROM test_table_with_char_rc WHERE char_column = 'khaki  '",
                    "VALUES (CAST('khaki' AS CHAR(7)))");
        }
        finally {
            assertUpdate(session, "DROP TABLE test_table_with_char_rc");
        }
    }

    @Test
    public void testInvalidPartitionValue()
    {
        assertUpdate("CREATE TABLE invalid_partition_value (a int, b varchar) WITH (partitioned_by = ARRAY['b'])");
        assertQueryFails(
                "INSERT INTO invalid_partition_value VALUES (4, 'test' || chr(13))",
                "\\QHive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: 74 65 73 74 0D\\E");
        assertUpdate("DROP TABLE invalid_partition_value");

        assertQueryFails(
                "CREATE TABLE invalid_partition_value (a, b) WITH (partitioned_by = ARRAY['b']) AS SELECT 4, chr(9731)",
                "\\QHive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: E2 98 83\\E");
    }

    @Test
    public void testShowColumnMetadata()
    {
        String tableName = "test_show_column_table";

        @Language("SQL") String createTable = "CREATE TABLE " + tableName + " (a bigint, b varchar, c double)";

        Session testSession = testSessionBuilder()
                .setIdentity(new Identity("test_access_owner", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .build();

        assertUpdate(createTable);

        // verify showing columns over a table requires SELECT privileges for the table
        assertAccessAllowed("SHOW COLUMNS FROM " + tableName);
        assertAccessDenied(testSession,
                "SHOW COLUMNS FROM " + tableName,
                "Cannot show columns of table .*." + tableName + ".*",
                privilege(tableName, SELECT_COLUMN));

        @Language("SQL") String getColumnsSql = "" +
                "SELECT lower(column_name) " +
                "FROM information_schema.columns " +
                "WHERE table_name = '" + tableName + "'";
        assertEquals(computeActual(getColumnsSql).getOnlyColumnAsSet(), ImmutableSet.of("a", "b", "c"));

        // verify with no SELECT privileges on table, querying information_schema will return empty columns
        executeExclusively(() -> {
            try {
                getQueryRunner().getAccessControl().deny(privilege(tableName, SELECT_COLUMN));
                assertQueryReturnsEmptyResult(testSession, getColumnsSql);
            }
            finally {
                getQueryRunner().getAccessControl().reset();
            }
        });

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCurrentUserInView()
    {
        checkState(getSession().getCatalog().isPresent(), "catalog is not set");
        checkState(getSession().getSchema().isPresent(), "schema is not set");
        String testAccountsUnqualifiedName = "test_accounts";
        String testAccountsViewUnqualifiedName = "test_accounts_view";
        String testAccountsViewFullyQualifiedName = format("%s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), testAccountsViewUnqualifiedName);
        assertUpdate(format("CREATE TABLE %s AS SELECT user_name, account_name" +
                "  FROM (VALUES ('user1', 'account1'), ('user2', 'account2'))" +
                "  t (user_name, account_name)", testAccountsUnqualifiedName), 2);
        assertUpdate(format("CREATE VIEW %s AS SELECT account_name FROM test_accounts WHERE user_name = CURRENT_USER", testAccountsViewUnqualifiedName));
        assertUpdate(format("GRANT SELECT ON %s TO user1", testAccountsViewFullyQualifiedName));
        assertUpdate(format("GRANT SELECT ON %s TO user2", testAccountsViewFullyQualifiedName));

        Session user1 = testSessionBuilder()
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setIdentity(new Identity("user1", getSession().getIdentity().getPrincipal()))
                .build();

        Session user2 = testSessionBuilder()
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setIdentity(new Identity("user2", getSession().getIdentity().getPrincipal()))
                .build();

        assertQuery(user1, "SELECT account_name FROM test_accounts_view", "VALUES 'account1'");
        assertQuery(user2, "SELECT account_name FROM test_accounts_view", "VALUES 'account2'");
        assertUpdate("DROP VIEW test_accounts_view");
        assertUpdate("DROP TABLE test_accounts");
    }

    @Test
    public void testCreateViewWithNonHiveType()
    {
        String testTable = "test_create_view_table";
        String testView = "test_view_with_hll";
        assertUpdate(format("CREATE TABLE %s AS SELECT user_name, account_name" +
                "  FROM (VALUES ('user1', 'account1'), ('user2', 'account2'))" +
                "  t (user_name, account_name)", testTable), 2);
        assertUpdate(format("CREATE VIEW %s AS SELECT approx_set(account_name) as hll FROM %s", testView, testTable));
        assertQuery(format("SELECT cardinality(hll) from %s", testView), "VALUES '2'");
    }

    @Test
    public void testCollectColumnStatisticsOnCreateTable()
    {
        String tableName = "test_collect_column_statistics_on_create_table";
        assertUpdate(format("" +
                "CREATE TABLE %s " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar'] " +
                ") " +
                "AS " +
                "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, c_array, p_varchar " +
                "FROM ( " +
                "  VALUES " +
                "    (null, null, null, null, null, null, null, 'p1'), " +
                "    (null, null, null, null, null, null, null, 'p1'), " +
                "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', CAST('abc1' AS VARCHAR), CAST('bcd1' AS VARBINARY), sequence(0, 10), 'p1')," +
                "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', CAST('abc2' AS VARCHAR), CAST('bcd2' AS VARBINARY), sequence(10, 20), 'p1')," +
                "    (null, null, null, null, null, null, null, 'p2'), " +
                "    (null, null, null, null, null, null, null, 'p2'), " +
                "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', CAST('cba1' AS VARCHAR), CAST('dcb1' AS VARBINARY), sequence(20, 25), 'p2'), " +
                "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', CAST('cba2' AS VARCHAR), CAST('dcb2' AS VARBINARY), sequence(30, 35), 'p2') " +
                ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, c_array, p_varchar)", tableName), 8);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '0', '1', null), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '1.2', '2.2', null), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null, null), " +
                        "('c_array', 176.0E0, null, 0.5, null, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '1', '2', null), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '2.3', '3.3', null), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null, null), " +
                        "('c_array', 96.0E0, null, 0.5, null, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null, null)");

        // non existing partition
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_bigint', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_double', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_timestamp', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_varchar', 0E0, 0E0, 0E0, null, null, null, null), " +
                        "('c_varbinary', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_array', null, 0E0, 0E0, null, null, null, null), " +
                        "('p_varchar', 0E0, 0E0, 0E0, null, null, null, null), " +
                        "(null, null, null, null, 0E0, null, null, null)");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testCollectColumnStatisticsOnInsert()
    {
        String tableName = "test_collect_column_statistics_on_insert";
        assertUpdate(format("" +
                "CREATE TABLE %s ( " +
                "   c_boolean BOOLEAN, " +
                "   c_bigint BIGINT, " +
                "   c_double DOUBLE, " +
                "   c_timestamp TIMESTAMP, " +
                "   c_varchar VARCHAR, " +
                "   c_varbinary VARBINARY, " +
                "   c_array ARRAY(BIGINT), " +
                "   p_varchar VARCHAR " +
                ") " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar'] " +
                ")", tableName));

        assertUpdate(format("" +
                "INSERT INTO %s " +
                "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, c_array, p_varchar " +
                "FROM ( " +
                "  VALUES " +
                "    (null, null, null, null, null, null, null, 'p1'), " +
                "    (null, null, null, null, null, null, null, 'p1'), " +
                "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', CAST('abc1' AS VARCHAR), CAST('bcd1' AS VARBINARY), sequence(0, 10), 'p1')," +
                "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', CAST('abc2' AS VARCHAR), CAST('bcd2' AS VARBINARY), sequence(10, 20), 'p1')," +
                "    (null, null, null, null, null, null, null, 'p2'), " +
                "    (null, null, null, null, null, null, null, 'p2'), " +
                "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', CAST('cba1' AS VARCHAR), CAST('dcb1' AS VARBINARY), sequence(20, 25), 'p2'), " +
                "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', CAST('cba2' AS VARCHAR), CAST('dcb2' AS VARBINARY), sequence(30, 35), 'p2') " +
                ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, c_array, p_varchar)", tableName), 8);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '0', '1', null), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '1.2', '2.2', null), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null, null), " +
                        "('c_array', 176.0E0, null, 0.5E0, null, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_bigint', null, 2.0E0, 0.5E0, null, '1', '2', null), " +
                        "('c_double', null, 2.0E0, 0.5E0, null, '2.3', '3.3', null), " +
                        "('c_timestamp', null, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varchar', 8.0E0, 2.0E0, 0.5E0, null, null, null, null), " +
                        "('c_varbinary', 8.0E0, null, 0.5E0, null, null, null, null), " +
                        "('c_array', 96.0E0, null, 0.5E0, null, null, null, null), " +
                        "('p_varchar', 8.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 4.0E0, null, null, null)");

        // non existing partition
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_bigint', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_double', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_timestamp', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_varchar', 0E0, 0E0, 0E0, null, null, null, null), " +
                        "('c_varbinary', null, 0E0, 0E0, null, null, null, null), " +
                        "('c_array', null, 0E0, 0E0, null, null, null, null), " +
                        "('p_varchar', 0E0, 0E0, 0E0, null, null, null, null), " +
                        "(null, null, null, null, 0E0, null, null, null)");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testAnalyzePropertiesSystemTable()
    {
        assertQuery("SELECT * FROM system.metadata.analyze_properties WHERE catalog_name = 'hive'",
                "SELECT 'hive', 'partitions', '', 'array(array(varchar))', 'Partitions to be analyzed'");
    }

    @Test
    public void testAnalyzeEmptyTable()
    {
        String tableName = "test_analyze_empty_table";
        assertUpdate(format("CREATE TABLE %s (c_bigint BIGINT, c_varchar VARCHAR(2))", tableName));
        assertUpdate("ANALYZE " + tableName, 0);
    }

    @Test
    public void testInvalidAnalyzePartitionedTable()
    {
        String tableName = "test_invalid_analyze_partitioned_table";

        // Test table does not exist
        assertQueryFails("ANALYZE " + tableName, format(".*Table 'hive.tpch.%s' does not exist.*", tableName));

        createPartitionedTableForAnalyzeTest(tableName);

        // Test invalid property
        assertQueryFails(format("ANALYZE %s WITH (error = 1)", tableName), ".*'hive' does not support analyze property 'error'.*");
        assertQueryFails(format("ANALYZE %s WITH (partitions = 1)", tableName), ".*Cannot convert '1' to \\Qarray(array(varchar))\\E.*");
        assertQueryFails(format("ANALYZE %s WITH (partitions = NULL)", tableName), ".*Invalid null value for analyze property.*");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[NULL])", tableName), ".*Invalid null value in analyze partitions property.*");

        // Test non-existed partition
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4', '10']])", tableName), ".*Partition no longer exists.*");

        // Test partition schema mismatch
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4']])", tableName), ".*Partition value count does not match the partition column count.*");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p4', '10', 'error']])", tableName), ".*Partition value count does not match the partition column count.*");

        // Drop the partitioned test table
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testInvalidAnalyzeUnpartitionedTable()
    {
        String tableName = "test_invalid_analyze_unpartitioned_table";

        // Test table does not exist
        assertQueryFails("ANALYZE " + tableName, ".*Table.*does not exist.*");

        createUnpartitionedTableForAnalyzeTest(tableName);

        // Test partition properties on unpartitioned table
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[])", tableName), ".*Only partitioned table can be analyzed with a partition list.*");
        assertQueryFails(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1']])", tableName), ".*Only partitioned table can be analyzed with a partition list.*");

        // Drop the partitioned test table
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testAnalyzePartitionedTable()
    {
        String tableName = "test_analyze_partitioned_table";
        createPartitionedTableForAnalyzeTest(tableName);

        // No column stats before running analyze
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null, null), " +
                        "('c_array', null, null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8', null), " +
                        "(null, null, null, null, 16.0, null, null, null)");

        // No column stats after running an empty analyze
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[])", tableName), 0);
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null, null), " +
                        "('c_array', null, null, null, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8', null), " +
                        "(null, null, null, null, 16.0, null, null, null)");

        // Run analyze on 3 partitions including a null partition and a duplicate partition
        assertUpdate(format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['p1', '7'], ARRAY['p2', '7'], ARRAY['p2', '7'], ARRAY[NULL, NULL]])", tableName), 12);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1', null), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2', null), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null, null), " +
                        "('c_array', 176.0, null, 0.5, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7', null), " +
                        "(null, null, null, null, 4.0, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2', null), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3', null), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null, null), " +
                        "('c_array', 96.0, null, 0.5, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7', null), " +
                        "(null, null, null, null, 4.0, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7', null), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7', null), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null, null), " +
                        "('c_array', 192.0, null, 0.0, null, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null, null), " +
                        "('p_bigint', null, 0.0, 1.0, null, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null, null)");

        // Partition [p3, 8], [e1, 9], [e2, 9] have no column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null, null), " +
                        "('c_array', null, null, null, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8', null), " +
                        "(null, null, null, null, 4.0, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null, null), " +
                        "('c_array', null, null, null, null, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 0.0, null, null, null, null), " +
                        "('p_bigint', null, 0.0, 0.0, null, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null, null), " +
                        "('c_array', null, null, null, null, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 0.0, null, null, null, null), " +
                        "('p_bigint', null, 0.0, 0.0, null, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        // All partitions except empty partitions have column stats
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p1' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '0', '1', null), " +
                        "('c_double', null, 2.0, 0.5, null, '1.2', '2.2', null), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null, null), " +
                        "('c_array', 176.0, null, 0.5, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7', null), " +
                        "(null, null, null, null, 4.0, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p2' AND p_bigint = 7)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '1', '2', null), " +
                        "('c_double', null, 2.0, 0.5, null, '2.3', '3.3', null), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null, null), " +
                        "('c_array', 96.0, null, 0.5, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '7', '7', null), " +
                        "(null, null, null, null, 4.0, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar IS NULL AND p_bigint IS NULL)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 1.0, 0.0, null, null, null, null), " +
                        "('c_bigint', null, 4.0, 0.0, null, '4', '7', null), " +
                        "('c_double', null, 4.0, 0.0, null, '4.7', '7.7', null), " +
                        "('c_timestamp', null, 4.0, 0.0, null, null, null, null), " +
                        "('c_varchar', 16.0, 4.0, 0.0, null, null, null, null), " +
                        "('c_varbinary', 8.0, null, 0.0, null, null, null, null), " +
                        "('c_array', 192.0, null, 0.0, null, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 1.0, null, null, null, null), " +
                        "('p_bigint', null, 0.0, 1.0, null, null, null, null), " +
                        "(null, null, null, null, 4.0, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'p3' AND p_bigint = 8)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.5, null, null, null, null), " +
                        "('c_bigint', null, 2.0, 0.5, null, '2', '3', null), " +
                        "('c_double', null, 2.0, 0.5, null, '3.4', '4.4', null), " +
                        "('c_timestamp', null, 2.0, 0.5, null, null, null, null), " +
                        "('c_varchar', 8.0, 2.0, 0.5, null, null, null, null), " +
                        "('c_varbinary', 4.0, null, 0.5, null, null, null, null), " +
                        "('c_array', 96.0, null, 0.5, null, null, null, null), " +
                        "('p_varchar', 8.0, 1.0, 0.0, null, null, null, null), " +
                        "('p_bigint', null, 1.0, 0.0, null, '8', '8', null), " +
                        "(null, null, null, null, 4.0, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e1' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 0.0, 0.0, null, null, null, null), " +
                        "('c_bigint', null, 0.0, 0.0, null, null, null, null), " +
                        "('c_double', null, 0.0, 0.0, null, null, null, null), " +
                        "('c_timestamp', null, 0.0, 0.0, null, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 0.0, null, null, null, null), " +
                        "('c_varbinary', 0.0, null, 0.0, null, null, null, null), " +
                        "('c_array', 0.0, null, 0.0, null, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 0.0, null, null, null, null), " +
                        "('p_bigint', null, 0.0, 0.0, null, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null, null)");
        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar = 'e2' AND p_bigint = 9)", tableName),
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 0.0, 0.0, null, null, null, null), " +
                        "('c_bigint', null, 0.0, 0.0, null, null, null, null), " +
                        "('c_double', null, 0.0, 0.0, null, null, null, null), " +
                        "('c_timestamp', null, 0.0, 0.0, null, null, null, null), " +
                        "('c_varchar', 0.0, 0.0, 0.0, null, null, null, null), " +
                        "('c_varbinary', 0.0, null, 0.0, null, null, null, null), " +
                        "('c_array', 0.0, null, 0.0, null, null, null, null), " +
                        "('p_varchar', 0.0, 0.0, 0.0, null, null, null, null), " +
                        "('p_bigint', null, 0.0, 0.0, null, null, null, null), " +
                        "(null, null, null, null, 0.0, null, null, null)");

        // Drop the partitioned test table
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testAnalyzeUnpartitionedTable()
    {
        String tableName = "test_analyze_unpartitioned_table";
        createUnpartitionedTableForAnalyzeTest(tableName);

        // No column stats before running analyze
        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, null, null, null, null, null, null), " +
                        "('c_bigint', null, null, null, null, null, null, null), " +
                        "('c_double', null, null, null, null, null, null, null), " +
                        "('c_timestamp', null, null, null, null, null, null, null), " +
                        "('c_varchar', null, null, null, null, null, null, null), " +
                        "('c_varbinary', null, null, null, null, null, null, null), " +
                        "('c_array', null, null, null, null, null, null, null), " +
                        "('p_varchar', null, null, null, null, null, null, null), " +
                        "('p_bigint', null, null, null, null, null, null, null), " +
                        "(null, null, null, null, 16.0, null, null, null)");

        // Run analyze on the whole table
        assertUpdate("ANALYZE " + tableName, 16);

        assertQuery("SHOW STATS FOR " + tableName,
                "SELECT * FROM VALUES " +
                        "('c_boolean', null, 2.0, 0.375, null, null, null, null), " +
                        "('c_bigint', null, 8.0, 0.375, null, '0', '7', null), " +
                        "('c_double', null, 10.0, 0.375, null, '1.2', '7.7', null), " +
                        "('c_timestamp', null, 10.0, 0.375, null, null, null, null), " +
                        "('c_varchar', 40.0, 10.0, 0.375, null, null, null, null), " +
                        "('c_varbinary', 20.0, null, 0.375, null, null, null, null), " +
                        "('c_array', 560.0, null, 0.375, null, null, null, null), " +
                        "('p_varchar', 24.0, 3.0, 0.25, null, null, null, null), " +
                        "('p_bigint', null, 2.0, 0.25, null, '7', '8', null), " +
                        "(null, null, null, null, 16.0, null, null, null)");

        // Drop the unpartitioned test table
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    protected void createPartitionedTableForAnalyzeTest(String tableName)
    {
        createTableForAnalyzeTest(tableName, true);
    }

    protected void createUnpartitionedTableForAnalyzeTest(String tableName)
    {
        createTableForAnalyzeTest(tableName, false);
    }

    private void createTableForAnalyzeTest(String tableName, boolean partitioned)
    {
        Session defaultSession = getSession();

        // Disable column statistics collection when creating the table
        Session disableColumnStatsSession = Session.builder(defaultSession)
                .setCatalogSessionProperty(defaultSession.getCatalog().get(), "collect_column_statistics_on_write", "false")
                .build();

        assertUpdate(
                disableColumnStatsSession,
                "" +
                        "CREATE TABLE " +
                        tableName +
                        (partitioned ? " WITH (partitioned_by = ARRAY['p_varchar', 'p_bigint'])\n" : " ") +
                        "AS " +
                        "SELECT c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, c_array, p_varchar, p_bigint " +
                        "FROM ( " +
                        "  VALUES " +
                        // p_varchar = 'p1', p_bigint = BIGINT '7'
                        "    (null, null, null, null, null, null, null, 'p1', BIGINT '7'), " +
                        "    (null, null, null, null, null, null, null, 'p1', BIGINT '7'), " +
                        "    (true, BIGINT '1', DOUBLE '2.2', TIMESTAMP '2012-08-08 01:00', 'abc1', X'bcd1', sequence(0, 10), 'p1', BIGINT '7'), " +
                        "    (false, BIGINT '0', DOUBLE '1.2', TIMESTAMP '2012-08-08 00:00', 'abc2', X'bcd2', sequence(10, 20), 'p1', BIGINT '7'), " +
                        // p_varchar = 'p2', p_bigint = BIGINT '7'
                        "    (null, null, null, null, null, null, null, 'p2', BIGINT '7'), " +
                        "    (null, null, null, null, null, null, null, 'p2', BIGINT '7'), " +
                        "    (true, BIGINT '2', DOUBLE '3.3', TIMESTAMP '2012-09-09 01:00', 'cba1', X'dcb1', sequence(20, 25), 'p2', BIGINT '7'), " +
                        "    (false, BIGINT '1', DOUBLE '2.3', TIMESTAMP '2012-09-09 00:00', 'cba2', X'dcb2', sequence(30, 35), 'p2', BIGINT '7'), " +
                        // p_varchar = 'p3', p_bigint = BIGINT '8'
                        "    (null, null, null, null, null, null, null, 'p3', BIGINT '8'), " +
                        "    (null, null, null, null, null, null, null, 'p3', BIGINT '8'), " +
                        "    (true, BIGINT '3', DOUBLE '4.4', TIMESTAMP '2012-10-10 01:00', 'bca1', X'cdb1', sequence(40, 45), 'p3', BIGINT '8'), " +
                        "    (false, BIGINT '2', DOUBLE '3.4', TIMESTAMP '2012-10-10 00:00', 'bca2', X'cdb2', sequence(50, 55), 'p3', BIGINT '8'), " +
                        // p_varchar = NULL, p_bigint = NULL
                        "    (false, BIGINT '7', DOUBLE '7.7', TIMESTAMP '1977-07-07 07:07', 'efa1', X'efa1', sequence(60, 65), NULL, NULL), " +
                        "    (false, BIGINT '6', DOUBLE '6.7', TIMESTAMP '1977-07-07 07:06', 'efa2', X'efa2', sequence(70, 75), NULL, NULL), " +
                        "    (false, BIGINT '5', DOUBLE '5.7', TIMESTAMP '1977-07-07 07:05', 'efa3', X'efa3', sequence(80, 85), NULL, NULL), " +
                        "    (false, BIGINT '4', DOUBLE '4.7', TIMESTAMP '1977-07-07 07:04', 'efa4', X'efa4', sequence(90, 95), NULL, NULL) " +
                        ") AS x (c_boolean, c_bigint, c_double, c_timestamp, c_varchar, c_varbinary, c_array, p_varchar, p_bigint)", 16);

        if (partitioned) {
            // Create empty partitions
            assertUpdate(disableColumnStatsSession, format("CALL system.create_empty_partition('%s', '%s', ARRAY['p_varchar', 'p_bigint'], ARRAY['%s', '%s'])", TPCH_SCHEMA, tableName, "e1", "9"));
            assertUpdate(disableColumnStatsSession, format("CALL system.create_empty_partition('%s', '%s', ARRAY['p_varchar', 'p_bigint'], ARRAY['%s', '%s'])", TPCH_SCHEMA, tableName, "e2", "9"));
        }
    }

    @Test
    public void testInsertMultipleColumnsFromSameChannel()
    {
        String tableName = "test_insert_multiple_columns_same_channel";
        assertUpdate(format("" +
                "CREATE TABLE %s ( " +
                "   c_bigint_1 BIGINT, " +
                "   c_bigint_2 BIGINT, " +
                "   p_varchar_1 VARCHAR, " +
                "   p_varchar_2 VARCHAR " +
                ") " +
                "WITH ( " +
                "   partitioned_by = ARRAY['p_varchar_1', 'p_varchar_2'] " +
                ")", tableName));

        assertUpdate(format("" +
                "INSERT INTO %s " +
                "SELECT 1 c_bigint_1, 1 c_bigint_2, '2' p_varchar_1, '2' p_varchar_2 ", tableName), 1);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar_1 = '2' AND p_varchar_2 = '2')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_bigint_1', null, 1.0E0, 0.0E0, null, '1', '1', null), " +
                        "('c_bigint_2', null, 1.0E0, 0.0E0, null, '1', '1', null), " +
                        "('p_varchar_1', 1.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "('p_varchar_2', 1.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 1.0E0, null, null, null)");

        assertUpdate(format("" +
                "INSERT INTO %s (c_bigint_1, c_bigint_2, p_varchar_1, p_varchar_2) " +
                "SELECT orderkey, orderkey, orderstatus, orderstatus " +
                "FROM orders " +
                "WHERE orderstatus='O' AND orderkey = 15008", tableName), 1);

        assertQuery(format("SHOW STATS FOR (SELECT * FROM %s WHERE p_varchar_1 = 'O' AND p_varchar_2 = 'O')", tableName),
                "SELECT * FROM VALUES " +
                        "('c_bigint_1', null, 1.0E0, 0.0E0, null, '15008', '15008', null), " +
                        "('c_bigint_2', null, 1.0E0, 0.0E0, null, '15008', '15008', null), " +
                        "('p_varchar_1', 1.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "('p_varchar_2', 1.0E0, 1.0E0, 0.0E0, null, null, null, null), " +
                        "(null, null, null, null, 1.0E0, null, null, null)");

        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testCreateAvroTableWithSchemaUrl()
            throws Exception
    {
        String tableName = "test_create_avro_table_with_schema_url";
        File schemaFile = createAvroSchemaFile();

        String createTableSql = getAvroCreateTableSql(tableName, schemaFile.getAbsolutePath());
        String expectedShowCreateTable = getAvroCreateTableSql(tableName, schemaFile.toURI().toString());

        assertUpdate(createTableSql);

        try {
            MaterializedResult actual = computeActual(format("SHOW CREATE TABLE %s", tableName));
            assertEquals(actual.getOnlyValue(), expectedShowCreateTable);
        }
        finally {
            assertUpdate(format("DROP TABLE %s", tableName));
            verify(schemaFile.delete(), "cannot delete temporary file: %s", schemaFile);
        }
    }

    @Test
    public void testAlterAvroTableWithSchemaUrl()
            throws Exception
    {
        testAlterAvroTableWithSchemaUrl(true, true, true);
    }

    protected void testAlterAvroTableWithSchemaUrl(boolean renameColumn, boolean addColumn, boolean dropColumn)
            throws Exception
    {
        String tableName = "test_alter_avro_table_with_schema_url";
        File schemaFile = createAvroSchemaFile();

        assertUpdate(getAvroCreateTableSql(tableName, schemaFile.getAbsolutePath()));

        try {
            if (renameColumn) {
                assertQueryFails(format("ALTER TABLE %s RENAME COLUMN dummy_col TO new_dummy_col", tableName), "ALTER TABLE not supported when Avro schema url is set");
            }
            if (addColumn) {
                assertQueryFails(format("ALTER TABLE %s ADD COLUMN new_dummy_col VARCHAR", tableName), "ALTER TABLE not supported when Avro schema url is set");
            }
            if (dropColumn) {
                assertQueryFails(format("ALTER TABLE %s DROP COLUMN dummy_col", tableName), "ALTER TABLE not supported when Avro schema url is set");
            }
        }
        finally {
            assertUpdate(format("DROP TABLE %s", tableName));
            verify(schemaFile.delete(), "cannot delete temporary file: %s", schemaFile);
        }
    }

    private String getAvroCreateTableSql(String tableName, String schemaFile)
    {
        return format("CREATE TABLE %s.%s.%s (%n" +
                        "   \"dummy_col\" varchar,%n" +
                        "   \"another_dummy_col\" varchar%n" +
                        ")%n" +
                        "WITH (%n" +
                        "   avro_schema_url = '%s',%n" +
                        "   format = 'AVRO'%n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                schemaFile);
    }

    private static File createAvroSchemaFile()
            throws Exception
    {
        File schemaFile = File.createTempFile("avro_single_column-", ".avsc");
        String schema = "{\n" +
                "  \"namespace\": \"com.facebook.test\",\n" +
                "  \"name\": \"single_column\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"fields\": [\n" +
                "    { \"name\":\"string_col\", \"type\":\"string\" }\n" +
                "]}";
        asCharSink(schemaFile, UTF_8).write(schema);
        return schemaFile;
    }

    @Test
    public void testCreateOrcTableWithSchemaUrl()
            throws Exception
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_orc (\n" +
                        "   dummy_col varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   avro_schema_url = 'dummy.avsc',\n" +
                        "   format = 'ORC'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get());

        assertQueryFails(createTableSql, "Cannot specify avro_schema_url table property for storage format: ORC");
    }

    @Test
    public void testCtasFailsWithAvroSchemaUrl()
            throws Exception
    {
        @Language("SQL") String ctasSqlWithoutData = "CREATE TABLE create_avro\n" +
                "WITH (avro_schema_url = 'dummy_schema')\n" +
                "AS SELECT 'dummy_value' as dummy_col WITH NO DATA";

        assertQueryFails(ctasSqlWithoutData, "CREATE TABLE AS not supported when Avro schema url is set");

        @Language("SQL") String ctasSql = "CREATE TABLE create_avro\n" +
                "WITH (avro_schema_url = 'dummy_schema')\n" +
                "AS SELECT * FROM (VALUES('a')) t (a)";

        assertQueryFails(ctasSql, "CREATE TABLE AS not supported when Avro schema url is set");
    }

    @Test
    public void testBucketedTablesFailWithTooManyBuckets()
            throws Exception
    {
        @Language("SQL") String createSql = "CREATE TABLE t (dummy VARCHAR)\n" +
                "WITH (bucket_count = 1000001, bucketed_by=ARRAY['dummy'])";

        assertQueryFails(createSql, "bucket_count should be no more than 1000000");
    }

    @Test
    public void testBucketedTablesFailWithAvroSchemaUrl()
            throws Exception
    {
        @Language("SQL") String createSql = "CREATE TABLE create_avro (dummy VARCHAR)\n" +
                "WITH (avro_schema_url = 'dummy_schema',\n" +
                "      bucket_count = 2, bucketed_by=ARRAY['dummy'])";

        assertQueryFails(createSql, "Bucketing/Partitioning columns not supported when Avro schema url is set");
    }

    @Test
    public void testPartitionedTablesFailWithAvroSchemaUrl()
            throws Exception
    {
        @Language("SQL") String createSql = "CREATE TABLE create_avro (dummy VARCHAR)\n" +
                "WITH (avro_schema_url = 'dummy_schema',\n" +
                "      partitioned_by=ARRAY['dummy'])";

        assertQueryFails(createSql, "Bucketing/Partitioning columns not supported when Avro schema url is set");
    }

    @Test
    public void testPrunePartitionFailure()
    {
        assertUpdate("CREATE TABLE test_prune_failure\n" +
                "WITH (partitioned_by = ARRAY['p']) AS\n" +
                "SELECT 123 x, 'abc' p", 1);

        assertQueryReturnsEmptyResult("" +
                "SELECT * FROM test_prune_failure\n" +
                "WHERE x < 0 AND cast(p AS int) > 0");

        assertQueryFails("" +
                "SELECT * FROM test_prune_failure\n" +
                "WHERE x > 0 AND cast(p AS int) > 0", "Cannot cast 'abc' to INT");

        assertUpdate("DROP TABLE test_prune_failure");
    }

    @Test
    public void testTemporaryStagingDirectorySessionProperties()
    {
        String tableName = "test_temporary_staging_directory_session_properties";
        assertUpdate(format("CREATE TABLE %s(i int)", tableName));

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "temporary_staging_directory_enabled", "false")
                .build();

        HiveInsertTableHandle hiveInsertTableHandle = getHiveInsertTableHandle(session, tableName);
        assertEquals(hiveInsertTableHandle.getLocationHandle().getWritePath(), hiveInsertTableHandle.getLocationHandle().getTargetPath());

        session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "temporary_staging_directory_enabled", "true")
                .setCatalogSessionProperty(catalog, "temporary_staging_directory_path", "/tmp/custom/temporary-${USER}")
                .build();

        hiveInsertTableHandle = getHiveInsertTableHandle(session, tableName);
        assertNotEquals(hiveInsertTableHandle.getLocationHandle().getWritePath(), hiveInsertTableHandle.getLocationHandle().getTargetPath());
        assertTrue(hiveInsertTableHandle.getLocationHandle().getWritePath().toString().startsWith("file:/tmp/custom/temporary-"));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testLikeSerializesWithPushdownFilter()
    {
        Session pushdownFilterEnabled = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(catalog, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
        assertQuerySucceeds(pushdownFilterEnabled, "SELECT comment FROM lineitem WHERE comment LIKE 'abc%'");
    }

    @Test
    public void testGroupByWithUnion()
    {
        assertQuery("SELECT\n" +
                "      linenumber,\n" +
                "      'xxx'\n" +
                "  FROM\n" +
                "  (\n" +
                "      (SELECT orderkey, linenumber FROM lineitem)\n" +
                "      UNION\n" +
                "      (SELECT orderkey, linenumber FROM lineitem)\n" +
                "  ) WHERE orderkey = 1 \n" +
                "  GROUP BY\n" +
                "      linenumber");
    }

    @Test
    public void testIgnoreTableBucketing()
    {
        String query = "SELECT count(*) FROM orders WHERE \"$bucket\" = 1";
        assertQuery(bucketedSession, query, "SELECT 1350");
        Session ignoreBucketingSession = Session.builder(bucketedSession)
                .setCatalogSessionProperty(bucketedSession.getCatalog().get(), "ignore_table_bucketing", "true")
                .build();
        assertQueryFails(ignoreBucketingSession, query, "Table bucketing is ignored\\. The virtual \"\\$bucket\" column cannot be referenced\\.");
    }

    @Test
    public void testTableWriterMergeNodeIsPresent()
    {
        assertUpdate(
                getSession(),
                "CREATE TABLE test_table_writer_merge_operator AS SELECT orderkey FROM orders",
                15000,
                assertTableWriterMergeNodeIsPresent());
    }

    @Test
    public void testPropertiesTable()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_show_properties" +
                " WITH (" +
                "format = 'orc', " +
                "partitioned_by = ARRAY['ship_priority', 'order_status']," +
                "orc_bloom_filter_columns = ARRAY['ship_priority', 'order_status']," +
                "orc_bloom_filter_fpp = 0.5" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        String queryId = (String) computeScalar("SELECT query_id FROM system.runtime.queries WHERE query LIKE 'CREATE TABLE test_show_properties%'");
        String nodeVersion = (String) computeScalar("SELECT node_version FROM system.runtime.nodes WHERE coordinator");
        assertQuery("SELECT * FROM \"test_show_properties$properties\"",
                "SELECT '" + "ship_priority,order_status" + "','" + "0.5" + "','" + queryId + "','" + nodeVersion + "'");
        assertUpdate("DROP TABLE test_show_properties");
    }

    @Test
    public void testPageFileFormatSmallStripe()
    {
        Session smallStripeSession = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(catalog, "pagefile_writer_max_stripe_size", "1B")
                .build();
        assertUpdate(
                smallStripeSession,
                "CREATE TABLE test_pagefile_small_stripe\n" +
                        "WITH (\n" +
                        "format = 'PAGEFILE'\n" +
                        ") AS\n" +
                        "SELECT\n" +
                        "*\n" +
                        "FROM tpch.orders",
                "SELECT count(*) FROM orders");

        assertQuery("SELECT count(*) FROM test_pagefile_small_stripe", "SELECT count(*) FROM orders");

        assertQuery("SELECT sum(custkey) FROM test_pagefile_small_stripe", "SELECT sum(custkey) FROM orders");

        assertUpdate("DROP TABLE test_pagefile_small_stripe");
    }

    @Test
    public void testPageFileFormatSmallSplitSize()
    {
        Session testSession = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(catalog, "pagefile_writer_max_stripe_size", "100B")
                .setCatalogSessionProperty(catalog, "max_split_size", "1kB")
                .setCatalogSessionProperty(catalog, "max_initial_split_size", "1kB")
                .build();

        assertUpdate(
                testSession,
                "CREATE TABLE test_pagefile_small_split\n" +
                        "WITH (\n" +
                        "format = 'PAGEFILE'\n" +
                        ") AS\n" +
                        "SELECT\n" +
                        "*\n" +
                        "FROM tpch.orders",
                "SELECT count(*) FROM orders");

        assertQuery(testSession, "SELECT count(*) FROM test_pagefile_small_split", "SELECT count(*) FROM orders");

        assertQuery(testSession, "SELECT sum(custkey) FROM test_pagefile_small_split", "SELECT sum(custkey) FROM orders");

        assertUpdate("DROP TABLE test_pagefile_small_split");
    }

    @Test
    public void testPageFileCompression()
    {
        for (HiveCompressionCodec compression : HiveCompressionCodec.values()) {
            if (!compression.isSupportedStorageFormat(PAGEFILE)) {
                continue;
            }
            testPageFileCompression(compression.name());
        }
    }

    @Test
    public void testPartialAggregatePushdownORC()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_orc_table (" +
                " _boolean BOOLEAN" +
                ", _tinyint TINYINT" +
                ", _smallint SMALLINT" +
                ", _integer INTEGER" +
                ", _bigint BIGINT" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _shortdecimal DECIMAL(8,3)" +
                ", _longdecimal DECIMAL(25,2)" +
                ", _string VARCHAR" +
                ", _varchar VARCHAR(10)" +
                ", _singlechar CHAR" +
                ", _char CHAR(10)" +
                ", _varbinary VARBINARY" +
                ", _date DATE" +
                ", _timestamp TIMESTAMP" +
                ")" +
                "WITH (format = 'orc')";

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "partial_aggregation_pushdown_enabled", "true")
                .setCatalogSessionProperty(catalog, "partial_aggregation_pushdown_for_variable_length_datatypes_enabled", "true")
                .build();
        try {
            assertUpdate(session, createTable);

            TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_orc_table");
            assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), ORC);

            assertUpdate(session, "INSERT INTO test_orc_table VALUES (" +
                    "true" +
                    ", cast(1 as tinyint)" +
                    ", cast(2 as smallint)" +
                    ", 3" +
                    ", 4" +
                    ", 1.2" +
                    ", 2.3" +
                    ", 4.5" +
                    ", 55555555555555.32" +
                    ", 'abc'" +
                    ", 'def'" +
                    ", 'g'" +
                    ", 'hij'" +
                    ", cast('klm' as varbinary)" +
                    ", cast('2020-05-01' as date)" +
                    ", cast('2020-06-04 16:55:40.777' as timestamp)" +
                    ")", 1);

            assertUpdate(session, "INSERT INTO test_orc_table VALUES (" +
                    "false" +
                    ", cast(10 as tinyint)" +
                    ", cast(20 as smallint)" +
                    ", 30" +
                    ", 40" +
                    ", 10.25" +
                    ", 25.334" +
                    ", 465.523" +
                    ", 88888888555555.91" +
                    ", 'foo'" +
                    ", 'bar'" +
                    ", 'b'" +
                    ", 'baz'" +
                    ", cast('qux' as varbinary)" +
                    ", cast('2020-06-02' as date)" +
                    ", cast('2020-05-01 18:34:23.88' as timestamp)" +
                    ")", 1);
            String rowCount = "SELECT 2";

            assertQuery(session, "SELECT COUNT(*) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_boolean) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_tinyint) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_smallint) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_integer) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_bigint) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_real) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_double) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_shortdecimal) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_longdecimal) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_string) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_varchar) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_singlechar) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_char) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_varbinary) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_date) FROM test_orc_table", rowCount);
            assertQuery(session, "SELECT COUNT(_timestamp) FROM test_orc_table", rowCount);

            assertQuery(session, "SELECT MIN(_boolean), MAX(_boolean) FROM test_orc_table", "select false, true");
            assertQuery(session, "SELECT MIN(_tinyint), MAX(_tinyint) FROM test_orc_table", "select 1, 10");
            assertQuery(session, "SELECT MIN(_smallint), MAX(_smallint) FROM test_orc_table", "select 2, 20");
            assertQuery(session, "SELECT MIN(_integer), MAX(_integer) FROM test_orc_table", "select 3, 30");
            assertQuery(session, "SELECT MIN(_bigint), MAX(_bigint) FROM test_orc_table", "select 4, 40");
            assertQuery(session, "SELECT MIN(_real), MAX(_real) FROM test_orc_table", "select 1.2, 10.25");
            assertQuery(session, "SELECT MIN(_double), MAX(_double) FROM test_orc_table", "select 2.3, 25.334");
            assertQuery(session, "SELECT MIN(_shortdecimal), MAX(_shortdecimal) FROM test_orc_table", "select 4.5, 465.523");
            assertQuery(session, "SELECT MIN(_longdecimal), MAX(_longdecimal) FROM test_orc_table", "select 55555555555555.32, 88888888555555.91");
            assertQuery(session, "SELECT MIN(_string), MAX(_string) FROM test_orc_table", "select 'abc', 'foo'");
            assertQuery(session, "SELECT MIN(_varchar), MAX(_varchar) FROM test_orc_table", "select 'bar', 'def'");
            assertQuery(session, "SELECT MIN(_singlechar), MAX(_singlechar) FROM test_orc_table", "select 'b', 'g'");
            assertQuery(session, "SELECT MIN(_char), MAX(_char) FROM test_orc_table", "select 'baz', 'hij'");
            assertQuery(session, "SELECT MIN(_varbinary), MAX(_varbinary) FROM test_orc_table", "select X'6b6c6d', X'717578'");
            assertQuery(session, "SELECT MIN(_date), MAX(_date) FROM test_orc_table", "select cast('2020-05-01' as date), cast('2020-06-02' as date)");
            assertQuery(session, "SELECT MIN(_timestamp), MAX(_timestamp) FROM test_orc_table", "select cast('2020-05-01 18:34:23.88' as timestamp), cast('2020-06-04 16:55:40.777' as timestamp)");
        }
        finally {
            assertUpdate(session, "DROP TABLE test_orc_table");
        }
        assertFalse(getQueryRunner().tableExists(session, "test_orc_table"));
    }

    @Test
    public void testPartialAggregatePushdownDWRF()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_dwrf_table (" +
                " _boolean BOOLEAN" +
                ", _tinyint TINYINT" +
                ", _smallint SMALLINT" +
                ", _integer INTEGER" +
                ", _bigint BIGINT" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _string VARCHAR" +
                ", _varchar VARCHAR(10)" +
                ", _varbinary VARBINARY" +
                ", _timestamp TIMESTAMP" +
                ")" +
                "WITH (format = 'DWRF')";

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "partial_aggregation_pushdown_enabled", "true")
                .setCatalogSessionProperty(catalog, "partial_aggregation_pushdown_for_variable_length_datatypes_enabled", "true")
                .build();
        try {
            assertUpdate(session, createTable);

            TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_dwrf_table");
            assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), DWRF);

            assertUpdate(session, "INSERT INTO test_dwrf_table VALUES (" +
                    "true" +
                    ", cast(1 as tinyint)" +
                    ", cast(2 as smallint)" +
                    ", 3" +
                    ", 4" +
                    ", 1.2" +
                    ", 2.3" +
                    ", 'abc'" +
                    ", 'def'" +
                    ", cast('klm' as varbinary)" +
                    ", cast('2020-06-04 16:55:40.777' as timestamp)" +
                    ")", 1);

            assertUpdate(session, "INSERT INTO test_dwrf_table VALUES (" +
                    "false" +
                    ", cast(10 as tinyint)" +
                    ", cast(20 as smallint)" +
                    ", 30" +
                    ", 40" +
                    ", 10.25" +
                    ", 25.334" +
                    ", 'foo'" +
                    ", 'bar'" +
                    ", cast('qux' as varbinary)" +
                    ", cast('2020-05-01 18:34:23.88' as timestamp)" +
                    ")", 1);
            String rowCount = "SELECT 2";

            assertQuery(session, "SELECT COUNT(*) FROM test_dwrf_table", rowCount);
            assertQuery(session, "SELECT COUNT(_boolean) FROM test_dwrf_table", rowCount);
            assertQuery(session, "SELECT COUNT(_tinyint) FROM test_dwrf_table", rowCount);
            assertQuery(session, "SELECT COUNT(_smallint) FROM test_dwrf_table", rowCount);
            assertQuery(session, "SELECT COUNT(_integer) FROM test_dwrf_table", rowCount);
            assertQuery(session, "SELECT COUNT(_bigint) FROM test_dwrf_table", rowCount);
            assertQuery(session, "SELECT COUNT(_real) FROM test_dwrf_table", rowCount);
            assertQuery(session, "SELECT COUNT(_double) FROM test_dwrf_table", rowCount);
            assertQuery(session, "SELECT COUNT(_string) FROM test_dwrf_table", rowCount);
            assertQuery(session, "SELECT COUNT(_varchar) FROM test_dwrf_table", rowCount);
            assertQuery(session, "SELECT COUNT(_varbinary) FROM test_dwrf_table", rowCount);
            assertQuery(session, "SELECT COUNT(_timestamp) FROM test_dwrf_table", rowCount);

            assertQuery(session, "SELECT MIN(_boolean), MAX(_boolean) FROM test_dwrf_table", "select false, true");
            assertQuery(session, "SELECT MIN(_tinyint), MAX(_tinyint) FROM test_dwrf_table", "select 1, 10");
            assertQuery(session, "SELECT MIN(_smallint), MAX(_smallint) FROM test_dwrf_table", "select 2, 20");
            assertQuery(session, "SELECT MIN(_integer), MAX(_integer) FROM test_dwrf_table", "select 3, 30");
            assertQuery(session, "SELECT MIN(_bigint), MAX(_bigint) FROM test_dwrf_table", "select 4, 40");
            assertQuery(session, "SELECT MIN(_real), MAX(_real) FROM test_dwrf_table", "select 1.2, 10.25");
            assertQuery(session, "SELECT MIN(_double), MAX(_double) FROM test_dwrf_table", "select 2.3, 25.334");
            assertQuery(session, "SELECT MIN(_string), MAX(_string) FROM test_dwrf_table", "select 'abc', 'foo'");
            assertQuery(session, "SELECT MIN(_varchar), MAX(_varchar) FROM test_dwrf_table", "select 'bar', 'def'");
            assertQuery(session, "SELECT MIN(_varbinary), MAX(_varbinary) FROM test_dwrf_table", "select X'6b6c6d', X'717578'");
            assertQuery(session, "SELECT MIN(_timestamp), MAX(_timestamp) FROM test_dwrf_table", "select cast('2020-05-01 18:34:23.88' as timestamp), cast('2020-06-04 16:55:40.777' as timestamp)");
        }
        finally {
            assertUpdate(session, "DROP TABLE test_dwrf_table");
        }
        assertFalse(getQueryRunner().tableExists(session, "test_dwrf_table"));
    }

    @Test
    public void testPartialAggregatePushdownParquet()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_parquet_table (" +
                " _boolean BOOLEAN" +
                ", _tinyint TINYINT" +
                ", _smallint SMALLINT" +
                ", _integer INTEGER" +
                ", _bigint BIGINT" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _shortdecimal DECIMAL(8,3)" +
                ", _longdecimal DECIMAL(25,2)" +
                ", _string VARCHAR" +
                ", _varchar VARCHAR(10)" +
                ", _singlechar CHAR" +
                ", _char CHAR(10)" +
                ", _varbinary VARBINARY" +
                ", _date DATE" +
                ", _timestamp TIMESTAMP" +
                ")" +
                "WITH (format = 'parquet')";

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "partial_aggregation_pushdown_enabled", "true")
                .setCatalogSessionProperty(catalog, "partial_aggregation_pushdown_for_variable_length_datatypes_enabled", "true")
                .build();
        try {
            assertUpdate(session, createTable);

            TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_parquet_table");
            assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), PARQUET);

            assertUpdate(session, "INSERT INTO test_parquet_table VALUES (" +
                    "true" +
                    ", cast(1 as tinyint)" +
                    ", cast(2 as smallint)" +
                    ", 3" +
                    ", 4" +
                    ", 1.2" +
                    ", 2.3" +
                    ", 4.5" +
                    ", 55555555555555.32" +
                    ", 'abc'" +
                    ", 'def'" +
                    ", 'g'" +
                    ", 'hij'" +
                    ", cast('klm' as varbinary)" +
                    ", cast('2020-05-01' as date)" +
                    ", cast('2020-06-04 16:55:40.777' as timestamp)" +
                    ")", 1);

            assertUpdate(session, "INSERT INTO test_parquet_table VALUES (" +
                    "false" +
                    ", cast(10 as tinyint)" +
                    ", cast(20 as smallint)" +
                    ", 30" +
                    ", 40" +
                    ", 10.25" +
                    ", 25.334" +
                    ", 465.523" +
                    ", 88888888555555.91" +
                    ", 'foo'" +
                    ", 'bar'" +
                    ", 'b'" +
                    ", 'baz'" +
                    ", cast('qux' as varbinary)" +
                    ", cast('2020-06-02' as date)" +
                    ", cast('2020-05-01 18:34:23.88' as timestamp)" +
                    ")", 1);
            String rowCount = "SELECT 2";

            assertQuery(session, "SELECT COUNT(*) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_boolean) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_tinyint) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_smallint) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_integer) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_bigint) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_real) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_double) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_shortdecimal) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_longdecimal) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_string) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_varchar) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_singlechar) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_char) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_varbinary) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_date) FROM test_parquet_table", rowCount);
            assertQuery(session, "SELECT COUNT(_timestamp) FROM test_parquet_table", rowCount);

            assertQuery(session, "SELECT MIN(_boolean), MAX(_boolean) FROM test_parquet_table", "select false, true");
            assertQuery(session, "SELECT MIN(_tinyint), MAX(_tinyint) FROM test_parquet_table", "select 1, 10");
            assertQuery(session, "SELECT MIN(_smallint), MAX(_smallint) FROM test_parquet_table", "select 2, 20");
            assertQuery(session, "SELECT MIN(_integer), MAX(_integer) FROM test_parquet_table", "select 3, 30");
            assertQuery(session, "SELECT MIN(_bigint), MAX(_bigint) FROM test_parquet_table", "select 4, 40");
            assertQuery(session, "SELECT MIN(_real), MAX(_real) FROM test_parquet_table", "select 1.2, 10.25");
            assertQuery(session, "SELECT MIN(_double), MAX(_double) FROM test_parquet_table", "select 2.3, 25.334");

            assertQuery(session, "SELECT MIN(_shortdecimal), MAX(_shortdecimal) FROM test_parquet_table", "select 4.5, 465.523");
            assertQuery(session, "SELECT MIN(_longdecimal), MAX(_longdecimal) FROM test_parquet_table", "select 55555555555555.32, 88888888555555.91");
            assertQuery(session, "SELECT MIN(_string), MAX(_string) FROM test_parquet_table", "select 'abc', 'foo'");
            assertQuery(session, "SELECT MIN(_varchar), MAX(_varchar) FROM test_parquet_table", "select 'bar', 'def'");
            assertQuery(session, "SELECT MIN(_singlechar), MAX(_singlechar) FROM test_parquet_table", "select 'b', 'g'");
            assertQuery(session, "SELECT MIN(_char), MAX(_char) FROM test_parquet_table", "select 'baz', 'hij'");
            assertQuery(session, "SELECT MIN(_varbinary), MAX(_varbinary) FROM test_parquet_table", "select X'6b6c6d', X'717578'");

            assertQuery(session, "SELECT MIN(_date), MAX(_date) FROM test_parquet_table", "select cast('2020-05-01' as date), cast('2020-06-02' as date)");
            assertQuery(session, "SELECT MIN(_timestamp), MAX(_timestamp) FROM test_parquet_table", "select cast('2020-05-01 18:34:23.88' as timestamp), cast('2020-06-04 16:55:40.777' as timestamp)");
        }
        finally {
            assertUpdate(session, "DROP TABLE test_parquet_table");
        }
        assertFalse(getQueryRunner().tableExists(session, "test_parquet_table"));
    }

    @DataProvider
    public static Object[][] fileFormats()
    {
        return new Object[][] {
                {"orc"},
                {"parquet"},
                {"dwrf"}
        };
    }

    @Test(dataProvider = "fileFormats")
    public void testPartialAggregatePushdownWithPartitionKey(String fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_table (" +
                " _boolean BOOLEAN" +
                ", _tinyint TINYINT" +
                ", _smallint SMALLINT" +
                ", _integer INTEGER" +
                ", _bigint BIGINT" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _string VARCHAR" +
                ", _varchar VARCHAR(10)" +
                ", _varbinary VARBINARY" +
                ", _ds VARCHAR" +
                ")" +
                "WITH (format = '" + fileFormat + "', partitioned_by = ARRAY['_ds'])";

        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, PARTIAL_AGGREGATION_PUSHDOWN_ENABLED, "true")
                .setCatalogSessionProperty(catalog, PARTIAL_AGGREGATION_PUSHDOWN_FOR_VARIABLE_LENGTH_DATATYPES_ENABLED, "true")
                .build();
        try {
            assertUpdate(session, createTable);

            TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_table");
            assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), HiveStorageFormat.valueOf(fileFormat.toUpperCase()));

            assertUpdate(session, "INSERT INTO test_table VALUES (" +
                    "true" +
                    ", cast(1 as tinyint)" +
                    ", cast(2 as smallint)" +
                    ", 3" +
                    ", 4" +
                    ", 1.2" +
                    ", 2.3" +
                    ", 'abc'" +
                    ", 'def'" +
                    ", cast('klm' as varbinary)" +
                    ", '2024-03-06'" +
                    ")", 1);

            assertUpdate(session, "INSERT INTO test_table VALUES (" +
                    "false" +
                    ", cast(10 as tinyint)" +
                    ", cast(20 as smallint)" +
                    ", 30" +
                    ", 40" +
                    ", 10.25" +
                    ", 25.334" +
                    ", 'foo'" +
                    ", 'bar'" +
                    ", cast('qux' as varbinary)" +
                    ", '2024-03-05'" +
                    ")", 1);
            assertQuery(session, "SELECT min(_ds) FROM test_table", "SELECT '2024-03-05'");
            assertQuery(session, "SELECT min(_ds), max(_ds) FROM test_table", "SELECT '2024-03-05', '2024-03-06'");
        }
        finally {
            assertUpdate(session, "DROP TABLE test_table");
        }
        assertFalse(getQueryRunner().tableExists(session, "test_table"));
    }

    @Test
    public void testParquetSelectivePageSourceFails()
    {
        assertUpdate("CREATE TABLE test_parquet_filter_pushdoown (a BIGINT, b BOOLEAN) WITH (format = 'parquet')");
        assertUpdate(getSession(), "INSERT INTO test_parquet_filter_pushdoown VALUES (1, true)", 1);

        Session noPushdownSession = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "pushdown_filter_enabled", "false")
                .setCatalogSessionProperty("hive", "parquet_pushdown_filter_enabled", "false")
                .build();
        assertQuery(noPushdownSession, "SELECT a FROM test_parquet_filter_pushdoown", "select 1");
        assertQuery(noPushdownSession, "SELECT a FROM test_parquet_filter_pushdoown WHERE b = true", "select 1");
        assertQueryReturnsEmptyResult(noPushdownSession, "SELECT a FROM test_parquet_filter_pushdoown WHERE b = false");

        Session filterPushdownSession = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "pushdown_filter_enabled", "true")
                .setCatalogSessionProperty("hive", "parquet_pushdown_filter_enabled", "false")
                .build();
        assertQuery(filterPushdownSession, "SELECT a FROM test_parquet_filter_pushdoown", "select 1");
        assertQuery(filterPushdownSession, "SELECT a FROM test_parquet_filter_pushdoown WHERE b = true", "select 1");
        assertQueryReturnsEmptyResult(filterPushdownSession, "SELECT a FROM test_parquet_filter_pushdoown WHERE b = false");

        Session parquetFilterPushdownSession = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "pushdown_filter_enabled", "true")
                .setCatalogSessionProperty("hive", "parquet_pushdown_filter_enabled", "true")
                .build();
        assertQueryFails(parquetFilterPushdownSession, "SELECT a FROM test_parquet_filter_pushdoown", "Parquet reader doesn't support filter pushdown yet");
        assertQueryFails(parquetFilterPushdownSession, "SELECT a FROM test_parquet_filter_pushdoown WHERE b = true", "Parquet reader doesn't support filter pushdown yet");
        assertQueryFails(parquetFilterPushdownSession, "SELECT a FROM test_parquet_filter_pushdoown WHERE b = false", "Parquet reader doesn't support filter pushdown yet");
    }

    private void testPageFileCompression(String compression)
    {
        Session testSession = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(catalog, "compression_codec", compression)
                .setCatalogSessionProperty(catalog, "pagefile_writer_max_stripe_size", "100B")
                .setCatalogSessionProperty(catalog, "max_split_size", "1kB")
                .setCatalogSessionProperty(catalog, "max_initial_split_size", "1kB")
                .build();

        assertUpdate(
                testSession,
                "CREATE TABLE test_pagefile_compression\n" +
                        "WITH (\n" +
                        "format = 'PAGEFILE'\n" +
                        ") AS\n" +
                        "SELECT\n" +
                        "*\n" +
                        "FROM tpch.orders",
                "SELECT count(*) FROM orders");

        assertQuery(testSession, "SELECT count(*) FROM test_pagefile_compression", "SELECT count(*) FROM orders");

        assertQuery(testSession, "SELECT sum(custkey) FROM test_pagefile_compression", "SELECT sum(custkey) FROM orders");

        assertUpdate("DROP TABLE test_pagefile_compression");
    }

    private static Consumer<Plan> assertTableWriterMergeNodeIsPresent()
    {
        return plan -> assertTrue(searchFrom(plan.getRoot())
                .where(node -> node instanceof TableWriterMergeNode)
                .findFirst()
                .isPresent());
    }

    private HiveInsertTableHandle getHiveInsertTableHandle(Session session, String tableName)
    {
        Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();
        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .execute(session, transactionSession -> {
                    QualifiedObjectName objectName = new QualifiedObjectName(catalog, TPCH_SCHEMA, tableName);
                    Optional<TableHandle> handle = metadata.getMetadataResolver(transactionSession).getTableHandle(objectName);
                    InsertTableHandle insertTableHandle = metadata.beginInsert(transactionSession, handle.get());
                    HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) insertTableHandle.getConnectorHandle();

                    metadata.finishInsert(transactionSession, insertTableHandle, ImmutableList.of(), ImmutableList.of());
                    return hiveInsertTableHandle;
                });
    }

    @Test
    public void testUnsupportedCsvTable()
    {
        assertQueryFails(
                "CREATE TABLE create_unsupported_csv(i INT, bound VARCHAR(10), unbound VARCHAR, dummy VARCHAR) WITH (format = 'CSV')",
                "\\QHive CSV storage format only supports VARCHAR (unbounded). Unsupported columns: i integer, bound varchar(10)\\E");
    }

    @Test
    public void testCreateMaterializedView()
    {
        computeActual("CREATE TABLE test_customer_base WITH (partitioned_by = ARRAY['nationkey']) " +
                "AS SELECT custkey, name, address, nationkey FROM customer LIMIT 10");
        computeActual("CREATE TABLE test_customer_base_copy WITH (partitioned_by = ARRAY['nationkey']) " +
                "AS SELECT custkey, name, address, nationkey FROM customer LIMIT 10");
        computeActual("CREATE TABLE test_orders_base WITH (partitioned_by = ARRAY['orderstatus']) " +
                "AS SELECT orderkey, custkey, totalprice, orderstatus FROM orders LIMIT 10");

        // Test successful create
        assertUpdate("CREATE MATERIALIZED VIEW test_customer_view WITH (partitioned_by = ARRAY['nationkey']" + retentionDays(30) + ") " +
                "AS SELECT name, nationkey FROM test_customer_base");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_customer_view"));
        assertTableColumnNames("test_customer_view", "name", "nationkey");

        // Test if exists
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_customer_view AS SELECT name FROM test_customer_base",
                format(
                        ".* Materialized view '%s.%s.test_customer_view' already exists",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get()));
        assertQuerySucceeds("CREATE MATERIALIZED VIEW IF NOT EXISTS test_customer_view AS SELECT name FROM test_customer_base");

        // Test partition mapping
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_customer_view_no_partition " + withRetentionDays(30) + " AS SELECT name FROM test_customer_base",
                ".*Unpartitioned materialized view is not supported.");
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_customer_view_no_direct_partition_mapped WITH (partitioned_by = ARRAY['nationkey']" + retentionDays(30) + ") " +
                        "AS SELECT name, CAST(nationkey AS BIGINT) AS nationkey FROM test_customer_base",
                format(".*Materialized view %s.test_customer_view_no_direct_partition_mapped must have at least one partition column that exists in.*",
                        getSession().getSchema().get()));

        // Test nested
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_customer_nested_view WITH (partitioned_by = ARRAY['nationkey']" + retentionDays(30) + ") " +
                        "AS SELECT name, nationkey FROM test_customer_view",
                format(".*CreateMaterializedView on a materialized view %s.%s.test_customer_view is not supported.",
                        getSession().getCatalog().get(), getSession().getSchema().get()));

        // Test query shape
        assertUpdate("CREATE MATERIALIZED VIEW test_customer_agg_view WITH (partitioned_by = ARRAY['nationkey']" + retentionDays(30) + ") " +
                "AS SELECT COUNT(DISTINCT name) AS num, nationkey FROM (SELECT name, nationkey FROM test_customer_base GROUP BY 1, 2) a GROUP BY nationkey");
        assertUpdate("CREATE MATERIALIZED VIEW test_customer_union_view WITH (partitioned_by = ARRAY['nationkey']" + retentionDays(30) + ") " +
                "AS SELECT name, nationkey FROM ( SELECT name, nationkey FROM test_customer_base WHERE nationkey = 1 UNION ALL " +
                "SELECT name, nationkey FROM test_customer_base_copy WHERE nationkey = 2)");
        assertUpdate("CREATE MATERIALIZED VIEW test_customer_order_join_view WITH (partitioned_by = ARRAY['orderstatus', 'nationkey']" + retentionDays(30) + ") " +
                "AS SELECT orders.totalprice, orders.orderstatus, customer.nationkey FROM test_customer_base customer JOIN " +
                "test_orders_base orders ON orders.custkey = customer.custkey");
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_customer_order_join_view_no_base_partition_mapped WITH (partitioned_by = ARRAY['custkey']" + retentionDays(30) + ") " +
                        "AS SELECT orders.totalprice, customer.nationkey, customer.custkey FROM test_customer_base customer JOIN " +
                        "test_orders_base orders ON orders.custkey = customer.custkey",
                format(".*Materialized view %s.test_customer_order_join_view_no_base_partition_mapped must have at least one partition column that exists in.*", getSession().getSchema().get()));
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_customer_order_join_view_no_base_partition_mapped WITH (partitioned_by = ARRAY['nation_order']" + retentionDays(30) + ") " +
                        "AS SELECT orders.totalprice, CONCAT(CAST(customer.nationkey AS VARCHAR), orders.orderstatus) AS nation_order " +
                        "FROM test_customer_base customer JOIN test_orders_base orders ON orders.custkey = customer.custkey",
                format(".*Materialized view %s.test_customer_order_join_view_no_base_partition_mapped must have at least one partition column that exists in.*", getSession().getSchema().get()));

        // Clean up
        computeActual("DROP TABLE IF EXISTS test_customer_base");
        computeActual("DROP TABLE IF EXISTS test_orders_base");
    }

    @Test
    public void testShowCreateOnMaterializedView()
    {
        String createMaterializedViewSql = formatSqlText(format("CREATE MATERIALIZED VIEW %s.%s.test_customer_view_1%n" +
                        "WITH (%n" +
                        "   format = 'ORC'," +
                        "   partitioned_by = ARRAY['nationkey']%n" +
                        retentionDays(15) +
                        ") AS SELECT%n" +
                        "  name%n" +
                        ", nationkey%n" +
                        "FROM\n" +
                        "  test_customer_base_1",
                getSession().getCatalog().get(),
                getSession().getSchema().get()));

        computeActual("CREATE TABLE test_customer_base_1 WITH (partitioned_by = ARRAY['nationkey']) AS SELECT custkey, name, address, nationkey FROM customer LIMIT 10");
        computeActual(createMaterializedViewSql);

        MaterializedResult actualResult = computeActual("SHOW CREATE MATERIALIZED VIEW test_customer_view_1");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createMaterializedViewSql.trim());

        assertQueryFails(
                "SHOW CREATE MATERIALIZED VIEW test_customer_base_1",
                format(
                        ".*Relation '%s.%s.test_customer_base_1' is a table, not a materialized view",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get()));
        assertQueryFails(
                "SHOW CREATE VIEW test_customer_view_1",
                format(
                        ".*Relation '%s.%s.test_customer_view_1' is a materialized view, not a view",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get()));
        assertQueryFails(
                "SHOW CREATE TABLE test_customer_view_1",
                format(
                        ".*Relation '%s.%s.test_customer_view_1' is a materialized view, not a table",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get()));

        // Clean up
        computeActual("DROP TABLE IF EXISTS test_customer_base_1");
    }

    @Test
    public void testAlterOnMaterializedView()
    {
        computeActual("CREATE TABLE test_customer_base_2 WITH (partitioned_by = ARRAY['nationkey']) AS SELECT custkey, name, address, nationkey FROM customer LIMIT 10");
        computeActual("CREATE MATERIALIZED VIEW test_customer_view_2 WITH (partitioned_by = ARRAY['nationkey']" + retentionDays(30) + ") " +
                "AS SELECT name, nationkey FROM test_customer_base_2");

        assertQueryFails(
                "ALTER TABLE test_customer_view_2 RENAME TO test_customer_view_new",
                format(
                        ".*'%s.%s.test_customer_view_2' is a materialized view, and rename is not supported",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get()));
        assertQueryFails(
                "ALTER TABLE test_customer_view_2 ADD COLUMN timezone VARCHAR",
                format(
                        ".*'%s.%s.test_customer_view_2' is a materialized view, and add column is not supported",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get()));
        assertQueryFails(
                "ALTER TABLE test_customer_view_2 DROP COLUMN address",
                format(".*'%s.%s.test_customer_view_2' is a materialized view, and drop column is not supported",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get()));
        assertQueryFails(
                "ALTER TABLE test_customer_view_2 RENAME COLUMN name TO custname",
                format(".*'%s.%s.test_customer_view_2' is a materialized view, and rename column is not supported",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get()));

        // Clean up
        computeActual("DROP TABLE IF EXISTS test_customer_base_2");
    }

    @Test
    public void testInsertDeleteOnMaterializedView()
    {
        computeActual("CREATE TABLE test_customer_base_3 WITH (partitioned_by = ARRAY['nationkey']) AS SELECT custkey, name, address, nationkey FROM customer LIMIT 10");
        computeActual("CREATE MATERIALIZED VIEW test_customer_view_3 WITH (partitioned_by = ARRAY['nationkey']" + retentionDays(30) + ") " +
                "AS SELECT name, nationkey FROM test_customer_base_3");

        assertQueryFails(
                "INSERT INTO test_customer_view_3 SELECT name, nationkey FROM test_customer_base_2",
                ".*Inserting into materialized views is not supported");
        assertQueryFails(
                "DELETE FROM test_customer_view_3",
                ".*Deleting from materialized views is not supported");

        // Clean up
        computeActual("DROP TABLE IF EXISTS test_customer_base_3");
    }

    @Test
    public void testDropMaterializedView()
    {
        computeActual("CREATE TABLE test_customer_base_4 WITH (partitioned_by = ARRAY['nationkey']) AS SELECT custkey, name, address, nationkey FROM customer LIMIT 10");
        computeActual("CREATE MATERIALIZED VIEW test_customer_view_4 WITH (partitioned_by = ARRAY['nationkey']" + retentionDays(30) + ") " +
                "AS SELECT name, nationkey FROM test_customer_base_4");

        assertQueryFails(
                "DROP TABLE test_customer_view_4",
                format(
                        ".*'%s.%s.test_customer_view_4' is a materialized view, not a table. Use DROP MATERIALIZED VIEW to drop.",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get()));
        assertQueryFails(
                "DROP VIEW test_customer_view_4",
                format(
                        ".*View '%s.%s.test_customer_view_4' does not exist",
                        getSession().getCatalog().get(),
                        getSession().getSchema().get()));

        assertUpdate("DROP MATERIALIZED VIEW test_customer_view_4");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_customer_view_4"));

        // Clean up
        computeActual("DROP TABLE IF EXISTS test_customer_base_4");
    }

    @Test
    public void testRefreshMaterializedViewSimple()
    {
        Session session = getSession();
        QueryRunner queryRunner = getQueryRunner();

        computeActual(
                "CREATE TABLE orders_partitioned WITH (partitioned_by = ARRAY['ds']) " +
                        "AS SELECT orderkey, orderpriority, '2020-01-01' as ds FROM orders WHERE orderkey < 1000 " +
                        "UNION ALL " +
                        "SELECT orderkey, orderpriority, '2019-01-02' as ds FROM orders WHERE orderkey > 1000");
        computeActual(
                "CREATE MATERIALIZED VIEW test_orders_view WITH (partitioned_by = ARRAY['ds']) " +
                        "AS SELECT orderkey, orderpriority, ds FROM orders_partitioned");

        String refreshSql = "REFRESH MATERIALIZED VIEW test_orders_view WHERE ds='2020-01-01'";
        String expectedInsertQuery = "SELECT orderkey, orderpriority, ds " +
                "FROM (" +
                "  SELECT * FROM orders_partitioned WHERE ds='2020-01-01'" +
                ") orders_partitioned";
        QueryAssertions.assertQuery(
                queryRunner,
                session,
                refreshSql,
                queryRunner,
                "SELECT COUNT(*) FROM ( " + expectedInsertQuery + " )",
                false, true);

        ResultWithQueryId<MaterializedResult> resultWithQueryId = ((DistributedQueryRunner) queryRunner).executeWithQueryId(session, refreshSql);
        QueryInfo queryInfo = ((DistributedQueryRunner) queryRunner).getQueryInfo(resultWithQueryId.getQueryId());
        assertEquals(queryInfo.getExpandedQuery().get(),
                "-- Expanded Query: REFRESH MATERIALIZED VIEW test_orders_view WHERE (ds = '2020-01-01')\n" +
                        "INSERT INTO test_orders_view SELECT\n" +
                        "  orderkey\n" +
                        ", orderpriority\n" +
                        ", ds\n" +
                        "FROM\n" +
                        "  orders_partitioned\n");

        computeActual("DROP TABLE orders_partitioned");
        computeActual("DROP MATERIALIZED VIEW test_orders_view");
    }

    @Test
    public void testRefreshMaterializedView()
    {
        Session session = getSession();
        QueryRunner queryRunner = getQueryRunner();

        computeActual("CREATE TABLE test_nation_base_5 WITH (partitioned_by = ARRAY['nationkey', 'regionkey']) AS SELECT name, nationkey, regionkey FROM nation");
        computeActual("CREATE TABLE test_customer_base_5 WITH (partitioned_by = ARRAY['nationkey']) AS SELECT custkey, name, mktsegment, nationkey FROM customer");
        computeActual(
                "CREATE MATERIALIZED VIEW test_customer_view_5 WITH (partitioned_by = ARRAY['marketsegment', 'nationkey', 'regionkey']" + retentionDays(30) + ") " +
                        "AS SELECT test_nation_base_5.name AS nationname, customer.custkey, customer.name AS customername, UPPER(customer.mktsegment) AS marketsegment, customer.nationkey, regionkey " +
                        "FROM test_nation_base_5 JOIN test_customer_base_5 customer ON (test_nation_base_5.nationkey = customer.nationkey)");

        // Test predicate columns from two base tables
        String refreshSql = "REFRESH MATERIALIZED VIEW test_customer_view_5 WHERE marketsegment = 'AUTOMOBILE' AND nationkey = 24 AND regionkey = 1";
        String expectedInsertQuery = "SELECT *" +
                "FROM (" +
                "  SELECT nation.name AS nationname, customer.custkey, customer.name AS customername, UPPER(customer.mktsegment) AS marketsegment, customer.nationkey, regionkey " +
                "  FROM (" +
                "    SELECT * FROM test_nation_base_5 WHERE regionkey = 1" +
                "  ) nation JOIN (" +
                "    SELECT * FROM test_customer_base_5 WHERE nationkey = 24" +
                "  ) customer ON (nation.nationkey = customer.nationkey)" +
                ") WHERE marketsegment = 'AUTOMOBILE'";
        QueryAssertions.assertQuery(
                queryRunner,
                session,
                refreshSql,
                queryRunner,
                "SELECT COUNT(*) FROM ( " + expectedInsertQuery + " )",
                false, true);

        // Test predicate columns from one base table
        refreshSql = "REFRESH MATERIALIZED VIEW test_customer_view_5 WHERE marketsegment = 'AUTOMOBILE' AND nationkey = 24";
        expectedInsertQuery = "SELECT *" +
                "FROM (" +
                "  SELECT nation.name AS nationname, customer.custkey, customer.name AS customername, UPPER(customer.mktsegment) AS marketsegment, customer.nationkey, regionkey " +
                "  FROM test_nation_base_5 nation JOIN (" +
                "    SELECT * FROM test_customer_base_5 WHERE nationkey = 24" +
                "  ) customer ON (nation.nationkey = customer.nationkey)" +
                ") WHERE marketsegment = 'AUTOMOBILE'";
        QueryAssertions.assertQuery(
                queryRunner,
                session,
                refreshSql,
                queryRunner,
                "SELECT COUNT(*) FROM ( " + expectedInsertQuery + " )",
                false, true);

        refreshSql = "REFRESH MATERIALIZED VIEW test_customer_view_5 WHERE regionkey = 1";
        expectedInsertQuery = "SELECT nation.name AS nationname, customer.custkey, customer.name AS customername, UPPER(customer.mktsegment) AS marketsegment, customer.nationkey, regionkey " +
                "FROM (" +
                "  SELECT * FROM test_nation_base_5 WHERE regionkey = 1" +
                ") nation JOIN test_customer_base_5 customer ON (nation.nationkey = customer.nationkey)";
        QueryAssertions.assertQuery(
                queryRunner,
                session,
                refreshSql,
                queryRunner,
                "SELECT COUNT(*) FROM ( " + expectedInsertQuery + " )",
                false, true);

        // Test invalid predicates
        assertQueryFails("REFRESH MATERIALIZED VIEW test_customer_view_5 WHERE nationname = 'UNITED STATES'", ".*Refresh materialized view by column nationname is not supported.*");
        assertQueryFails("REFRESH MATERIALIZED VIEW test_customer_view_5 WHERE regionkey = 1 OR nationkey = 24", ".*Only logical AND is supported in WHERE clause.*");
        assertQueryFails("REFRESH MATERIALIZED VIEW test_customer_view_5 WHERE regionkey + nationkey = 25", ".*Only columns specified on literals are supported in WHERE clause.*");
        assertQueryFails("REFRESH MATERIALIZED VIEW test_customer_view_5", ".*mismatched input '<EOF>'\\. Expecting: '\\.', 'WHERE'.*");
    }

    @Test
    public void testAlphaFormatDdl()
    {
        assertUpdate("CREATE TABLE test_alpha_ddl_table (col1 bigint) WITH (format = 'ALPHA')");
        assertUpdate("ALTER TABLE test_alpha_ddl_table ADD COLUMN col2 bigint");
        assertUpdate("ALTER TABLE test_alpha_ddl_table DROP COLUMN col2");
        assertUpdate("DROP TABLE test_alpha_ddl_table");

        assertUpdate("CREATE TABLE test_alpha_ddl_partitioned_table (col1 bigint, ds VARCHAR) WITH (format = 'ALPHA', partitioned_by = ARRAY['ds'])");
        assertUpdate("ALTER TABLE test_alpha_ddl_partitioned_table ADD COLUMN col2 bigint");
        assertUpdate("ALTER TABLE test_alpha_ddl_partitioned_table DROP COLUMN col2");
        assertUpdate("DROP TABLE test_alpha_ddl_partitioned_table");
    }

    @Test
    public void testAlphaFormatDml()
    {
        assertUpdate("CREATE TABLE test_alpha_dml_partitioned_table (col1 bigint, ds VARCHAR) WITH (format = 'ALPHA', partitioned_by = ARRAY['ds'])");
        // Alpha does not support DML yet
        assertQueryFails("INSERT INTO test_alpha_dml_partitioned_table VALUES (1, '2022-01-01')", "Serializer does not exist: com.facebook.alpha.AlphaSerde");
        assertUpdate("DROP TABLE test_alpha_dml_partitioned_table");
    }

    @Test
    public void testInvokedFunctionNamesLog()
    {
        QueryRunner queryRunner = getQueryRunner();
        Session logFunctionNamesEnabledSession = Session.builder(getSession())
                .setSystemProperty(LOG_INVOKED_FUNCTION_NAMES_ENABLED, "true")
                .build();
        ResultWithQueryId<MaterializedResult> resultWithQueryId;
        QueryInfo queryInfo;

        @Language("SQL") String queryWithScalarFunctions =
                "SELECT abs(acctbal), round(acctbal), round(acctbal, 1), repeat(custkey, 2), repeat(name, 3),  repeat(mktsegment, 4) FROM customer";
        resultWithQueryId = ((DistributedQueryRunner) queryRunner).executeWithQueryId(logFunctionNamesEnabledSession, queryWithScalarFunctions);
        queryInfo = ((DistributedQueryRunner) queryRunner).getQueryInfo(resultWithQueryId.getQueryId());
        assertEqualsNoOrder(queryInfo.getScalarFunctions(), ImmutableList.of("presto.default.abs", "presto.default.round", "presto.default.repeat"));

        @Language("SQL") String queryWithAggregateFunctions = "SELECT abs(nationkey), mktsegment, arbitrary(name), arbitrary(comment), " +
                "approx_percentile(acctbal, 0.1), approx_percentile(acctbal, 0.3, 0.01) FROM customer GROUP BY nationkey, mktsegment";
        resultWithQueryId = ((DistributedQueryRunner) queryRunner).executeWithQueryId(logFunctionNamesEnabledSession, queryWithAggregateFunctions);
        queryInfo = ((DistributedQueryRunner) queryRunner).getQueryInfo(resultWithQueryId.getQueryId());
        assertEqualsNoOrder(queryInfo.getScalarFunctions(), ImmutableList.of("presto.default.abs"));
        assertEqualsNoOrder(queryInfo.getAggregateFunctions(), ImmutableList.of("presto.default.arbitrary", "presto.default.approx_percentile"));

        @Language("SQL") String queryWithWindowFunctions = "SELECT row_number() OVER(PARTITION BY mktsegment), nth_value(name, 5) OVER(PARTITION BY nationkey) FROM customer";
        resultWithQueryId = ((DistributedQueryRunner) queryRunner).executeWithQueryId(logFunctionNamesEnabledSession, queryWithWindowFunctions);
        queryInfo = ((DistributedQueryRunner) queryRunner).getQueryInfo(resultWithQueryId.getQueryId());
        assertEqualsNoOrder(queryInfo.getWindowFunctions(), ImmutableList.of("presto.default.row_number", "presto.default.nth_value"));

        @Language("SQL") String queryWithNestedFunctions = "SELECT DISTINCT nationkey FROM customer WHERE mktsegment='BUILDING' AND contains(regexp_split( phone, '-' ), '11' )";
        resultWithQueryId = ((DistributedQueryRunner) queryRunner).executeWithQueryId(logFunctionNamesEnabledSession, queryWithNestedFunctions);
        queryInfo = ((DistributedQueryRunner) queryRunner).getQueryInfo(resultWithQueryId.getQueryId());
        assertEqualsNoOrder(queryInfo.getScalarFunctions(), ImmutableList.of("presto.default.contains", "presto.default.regexp_split"));

        @Language("SQL") String queryWithFunctionsInLambda = "SELECT transform(ARRAY[nationkey, custkey], x -> abs(x)) FROM customer";
        resultWithQueryId = ((DistributedQueryRunner) queryRunner).executeWithQueryId(logFunctionNamesEnabledSession, queryWithFunctionsInLambda);
        queryInfo = ((DistributedQueryRunner) queryRunner).getQueryInfo(resultWithQueryId.getQueryId());
        assertEqualsNoOrder(queryInfo.getScalarFunctions(), ImmutableList.of("presto.default.transform", "presto.default.abs"));
    }

    @Test
    public void testGroupByLimitPartitionKeys()
    {
        Session prefilter = Session.builder(getSession())
                .setSystemProperty("prefilter_for_groupby_limit", "true")
                .build();

        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'orderstatus' ]" +
                ") " +
                "AS " +
                "SELECT custkey, orderkey, orderstatus FROM tpch.tiny.orders";

        assertUpdate(prefilter, createTable, 15000);
        prefilter = Session.builder(prefilter)
                .setSystemProperty("prefilter_for_groupby_limit", "true")
                .build();

        MaterializedResult plan = computeActual(prefilter, "explain(type distributed) select count(custkey), orderstatus from test_create_partitioned_table_as group by orderstatus limit 1000");
        assertFalse(((String) plan.getOnlyValue()).toUpperCase().indexOf("MAP_AGG") >= 0);
        plan = computeActual(prefilter, "explain(type distributed) select count(custkey), orderkey from test_create_partitioned_table_as group by orderkey limit 1000");
        assertTrue(((String) plan.getOnlyValue()).toUpperCase().indexOf("MAP_AGG") >= 0);
    }

    @Test
    public void testJoinPrefilterPartitionKeys()
    {
        Session prefilter = Session.builder(getSession())
                .setSystemProperty("join_prefilter_build_side", "true")
                .build();

        @Language("SQL") String createTable = "" +
                "CREATE TABLE join_prefilter_test " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'orderstatus' ]" +
                ") " +
                "AS " +
                "SELECT custkey, orderkey, orderstatus FROM tpch.tiny.orders";

        assertUpdate(prefilter, createTable, 15000);
        MaterializedResult result = computeActual(prefilter, "explain(type distributed) select 1 from join_prefilter_test join customer using(custkey) where orderstatus='O'");
        // Make sure the layout of the copied table matches the original
        String plan = (String) result.getMaterializedRows().get(0).getField(0);
        assertNotEquals(plan.lastIndexOf(":: [[\"O\"]]"), plan.indexOf(":: [[\"O\"]]"));
    }

    @Test
    public void testAddTableConstraints()
    {
        String uniqueConstraint = "UNIQUE";
        String primaryKey = "PRIMARY KEY";
        String tableName = "test_table_add_table_constraints";
        String createTableFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint,\n" +
                "   %s double,\n" +
                "   %s varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";
        String originalCreateTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "c1",
                "c2",
                "c3");
        String expectedOriginalShowCreateTable = format(
                createTableFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "\"c2\"",
                "\"c3\"");
        String createTableWithOneConstraintFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint,\n" +
                "   %s double,\n" +
                "   %s varchar,\n" +
                "   %s\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";
        assertUpdate(getSession(), originalCreateTableSql);
        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedOriginalShowCreateTable);

        @Language("SQL") String addUniqueConstraintStmt = "ALTER TABLE " + tableName + " ADD CONSTRAINT cons1 " + uniqueConstraint + " (c1, c3)";
        assertUpdate(getSession(), addUniqueConstraintStmt);

        String expectedShowCreateTableWithOneConstraint = format(
                createTableWithOneConstraintFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "\"c2\"",
                "\"c3\"",
                format("CONSTRAINT cons1 %s (c1, c3)", uniqueConstraint));

        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedShowCreateTableWithOneConstraint);

        addUniqueConstraintStmt = "ALTER TABLE " + tableName + " ADD CONSTRAINT cons2 " + uniqueConstraint + " (c2)";
        assertUpdate(getSession(), addUniqueConstraintStmt);

        String createTableWithTwoConstraintsFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint,\n" +
                "   %s double,\n" +
                "   %s varchar,\n" +
                "   %s,\n" +
                "   %s\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";
        String expectedShowCreateTableWithTwoConstraints = format(
                createTableWithTwoConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "\"c2\"",
                "\"c3\"",
                format("CONSTRAINT cons1 %s (c1, c3)", uniqueConstraint),
                format("CONSTRAINT cons2 %s (c2)", uniqueConstraint));

        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedShowCreateTableWithTwoConstraints);

        String dropConstraintStmt = format("ALTER TABLE %s.%s.%s DROP CONSTRAINT cons2", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropConstraintStmt);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedShowCreateTableWithOneConstraint);

        dropConstraintStmt = format("ALTER TABLE %s.%s.%s DROP CONSTRAINT cons1", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropConstraintStmt);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedOriginalShowCreateTable);

        @Language("SQL") String addPrimaryKeyStmt = "ALTER TABLE " + tableName + " ADD CONSTRAINT pk " + primaryKey + " (c2, c3) DISABLED";
        assertUpdate(getSession(), addPrimaryKeyStmt);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        String expectedShowCreateTableWithPK = format(
                createTableWithOneConstraintFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "\"c2\"",
                "\"c3\"",
                format("CONSTRAINT pk %s (c2, c3) DISABLED", primaryKey));

        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedShowCreateTableWithPK);

        addUniqueConstraintStmt = "ALTER TABLE " + tableName + " ADD CONSTRAINT cons2 " + uniqueConstraint + " (c2) NOT RELY";
        assertUpdate(getSession(), addUniqueConstraintStmt);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        String expectedShowCreateTableWithPKAndUnique = format(
                createTableWithTwoConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "\"c2\"",
                "\"c3\"",
                format("CONSTRAINT pk %s (c2, c3) DISABLED", primaryKey),
                format("CONSTRAINT cons2 %s (c2) NOT RELY", uniqueConstraint));

        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedShowCreateTableWithPKAndUnique);

        dropConstraintStmt = format("ALTER TABLE %s.%s.%s DROP CONSTRAINT pk", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropConstraintStmt);
        dropConstraintStmt = format("ALTER TABLE %s.%s.%s DROP CONSTRAINT cons2", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropConstraintStmt);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedOriginalShowCreateTable);

        // Combinations of constraint qualifiers
        addPrimaryKeyStmt = "ALTER TABLE " + tableName + " ADD CONSTRAINT pk " + primaryKey + " (c2, c3) DISABLED NOT RELY ENFORCED";
        addUniqueConstraintStmt = "ALTER TABLE " + tableName + " ADD CONSTRAINT uq1 " + uniqueConstraint + " (c3) ENFORCED DISABLED";
        assertUpdate(getSession(), addPrimaryKeyStmt);
        assertUpdate(getSession(), addUniqueConstraintStmt);
        addUniqueConstraintStmt = "ALTER TABLE " + tableName + " ADD CONSTRAINT uq2 " + uniqueConstraint + " (c3) ENABLED RELY NOT ENFORCED";
        assertUpdate(getSession(), addUniqueConstraintStmt);
        addUniqueConstraintStmt = "ALTER TABLE " + tableName + " ADD CONSTRAINT uq3 " + uniqueConstraint + " (c1) DISABLED NOT ENFORCED NOT RELY";
        assertUpdate(getSession(), addUniqueConstraintStmt);

        String createTableWithFourConstraintsFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint,\n" +
                "   %s double,\n" +
                "   %s varchar,\n" +
                "   %s,\n" +
                "   %s,\n" +
                "   %s,\n" +
                "   %s\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";
        expectedShowCreateTableWithPKAndUnique = format(
                createTableWithFourConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "\"c2\"",
                "\"c3\"",
                format("CONSTRAINT pk %s (c2, c3) DISABLED NOT RELY", primaryKey),
                format("CONSTRAINT uq1 %s (c3) DISABLED", uniqueConstraint),
                format("CONSTRAINT uq2 %s (c3) NOT ENFORCED", uniqueConstraint),
                format("CONSTRAINT uq3 %s (c1) DISABLED NOT RELY NOT ENFORCED", uniqueConstraint));

        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedShowCreateTableWithPKAndUnique);

        // Negative tests
        assertQueryFails(addPrimaryKeyStmt, format("Primary key already exists for: %s.%s", getSession().getSchema().get(), tableName));
        assertQueryFails(addUniqueConstraintStmt, "Constraint already exists: 'uq3'");
        String dropNonExistentConstraint = format("ALTER TABLE %s.%s.%s DROP CONSTRAINT missingconstraint", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertQueryFails(dropNonExistentConstraint, "Constraint 'missingconstraint' not found");

        // Add unnamed constraints
        dropConstraintStmt = format("ALTER TABLE %s.%s.%s DROP CONSTRAINT pk", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropConstraintStmt);
        dropConstraintStmt = format("ALTER TABLE %s.%s.%s DROP CONSTRAINT uq1", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropConstraintStmt);
        dropConstraintStmt = format("ALTER TABLE %s.%s.%s DROP CONSTRAINT uq2", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropConstraintStmt);
        dropConstraintStmt = format("ALTER TABLE %s.%s.%s DROP CONSTRAINT uq3", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropConstraintStmt);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedOriginalShowCreateTable);

        addPrimaryKeyStmt = "ALTER TABLE " + tableName + " ADD " + primaryKey + " (c2, c3) DISABLED NOT RELY ENFORCED";
        assertUpdate(getSession(), addPrimaryKeyStmt);
        addUniqueConstraintStmt = "ALTER TABLE " + tableName + " ADD " + uniqueConstraint + " (c3) ENABLED RELY NOT ENFORCED";
        assertUpdate(getSession(), addUniqueConstraintStmt);

        String dropTableStmt = format("DROP TABLE %s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropTableStmt);
    }

    @Test
    public void testCreateTableWithConstraints()
    {
        String uniqueConstraint = "UNIQUE";
        String primaryKey = "PRIMARY KEY";
        String tableName = "test_table_create_table_with_constraints";

        String createTableFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint,\n" +
                "   %s double,\n" +
                "   %s varchar,\n" +
                "   %s bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";
        String createTableWithOneConstraintFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint,\n" +
                "   %s double,\n" +
                "   %s varchar,\n" +
                "   %s bigint,\n" +
                "   %s\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";
        String originalCreateTableSql = format(
                createTableFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "c1",
                "c2",
                "c3",
                "c4");
        String expectedOriginalShowCreateTable = format(
                createTableFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "\"c2\"",
                "\"c3\"",
                "\"c4\"");

        assertUpdate(getSession(), originalCreateTableSql);
        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedOriginalShowCreateTable);

        String dropTableStmt = format("DROP TABLE %s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropTableStmt);

        String createTableWithOneConstraintSql = format(
                createTableWithOneConstraintFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "c1",
                "c2",
                "c3",
                "c4",
                format("CONSTRAINT pk %s (c4) ENFORCED", primaryKey));

        String expectedcreateTableWithOneConstraint = format(
                createTableWithOneConstraintFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "\"c2\"",
                "\"c3\"",
                "\"c4\"",
                format("CONSTRAINT pk %s (c4)", primaryKey));

        assertUpdate(getSession(), createTableWithOneConstraintSql);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedcreateTableWithOneConstraint);
        assertUpdate(getSession(), dropTableStmt);

        String createTableWithTwoConstraintsFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint,\n" +
                "   %s double,\n" +
                "   %s varchar,\n" +
                "   %s bigint,\n" +
                "   %s,\n" +
                "   %s\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";

        String createTableWithTwoConstraintsSql = format(
                createTableWithTwoConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "c1",
                "c2",
                "c3",
                "c4",
                format("CONSTRAINT pk %s (c4) ENFORCED", primaryKey),
                format("CONSTRAINT uk %s (c3, c1) DISABLED NOT RELY", uniqueConstraint));

        String expectedcreateTableWithTwoConstraints = format(
                createTableWithTwoConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "\"c2\"",
                "\"c3\"",
                "\"c4\"",
                format("CONSTRAINT pk %s (c4)", primaryKey),
                format("CONSTRAINT uk %s (c3, c1) DISABLED NOT RELY", uniqueConstraint));

        assertUpdate(getSession(), createTableWithTwoConstraintsSql);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedcreateTableWithTwoConstraints);
        assertUpdate(getSession(), dropTableStmt);

        //Negative tests
        createTableWithTwoConstraintsSql = format(
                createTableWithTwoConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "c1",
                "c2",
                "c3",
                "c4",
                format("CONSTRAINT pk %s (c4) NOT ENFORCED", primaryKey),
                format("CONSTRAINT pk %s (c3, c1) NOT RELY DISABLED", uniqueConstraint));
        assertQueryFails(createTableWithTwoConstraintsSql, "Constraint name 'pk' specified more than once");

        createTableWithTwoConstraintsSql = format(
                createTableWithTwoConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "c1",
                "c2",
                "c3",
                "c4",
                format("CONSTRAINT uq %s (c4) NOT ENFORCED", uniqueConstraint),
                format("CONSTRAINT uq %s (c3, c1) DISABLED NOT RELY", uniqueConstraint));
        assertQueryFails(createTableWithTwoConstraintsSql, "Constraint name 'uq' specified more than once");

        createTableWithTwoConstraintsSql = format(
                createTableWithTwoConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "c1",
                "c2",
                "c3",
                "c4",
                format("CONSTRAINT pk1 %s (c4) NOT ENFORCED", primaryKey),
                format("CONSTRAINT pk2 %s (c3, c1) DISABLED NOT RELY", primaryKey));
        assertQueryFails(createTableWithTwoConstraintsSql, "Multiple primary key constraints are not allowed");

        createTableWithTwoConstraintsSql = format(
                createTableWithTwoConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "c1",
                "c2",
                "c3",
                "c4",
                format("CONSTRAINT pk1 %s (c4) NOT ENFORCED", primaryKey),
                format("%s (c3, c1) DISABLED NOT RELY", primaryKey));
        assertQueryFails(createTableWithTwoConstraintsSql, "Multiple primary key constraints are not allowed");

        assertUpdate(getSession(), createTableWithOneConstraintSql);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedcreateTableWithOneConstraint);
        // Since PRIMARY is a non-reserved keyword, it gets parsed and then fails at column resolution
        assertQueryFails("SELECT PRIMARY FROM " + tableName, ".*cannot be resolved.*");
        assertQueryFails("SELECT PRIMARY KEY FROM " + tableName, ".*cannot be resolved.*");
        assertUpdate(getSession(), dropTableStmt);

        String createTableWithInlineConstraintsFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint%s,\n" +
                "   %s double%s,\n" +
                "   %s varchar%s,\n" +
                "   %s bigint%s\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";

        String createTableWithNotNullConstraintsSql = format(
                createTableWithInlineConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "c1",
                "",
                "c2",
                " NOT NULL",
                "c3",
                "",
                "c4",
                " NOT NULL");

        String expectedCreateTableWithNotNullConstraintsSql = format(
                createTableWithInlineConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "",
                "\"c2\"",
                " NOT NULL",
                "\"c3\"",
                "",
                "\"c4\"",
                " NOT NULL");

        assertUpdate(getSession(), createTableWithNotNullConstraintsSql);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedCreateTableWithNotNullConstraintsSql);
        assertUpdate(getSession(), dropTableStmt);
    }

    @Test
    public void testDMLWithEnforcedConstraints()
    {
        String tableName = "test_table_dml_with_enforced_constraints";

        String createTableWithOneConstraintFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint,\n" +
                "   %s double,\n" +
                "   %s varchar,\n" +
                "   %s bigint,\n" +
                "   %s\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";

        String createTableWithOneConstraintSql = format(
                createTableWithOneConstraintFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "c1",
                "c2",
                "c3",
                "c4",
                "CONSTRAINT pk PRIMARY KEY (c4)");

        String expectedcreateTableWithOneConstraint = format(
                createTableWithOneConstraintFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "\"c2\"",
                "\"c3\"",
                "\"c4\"",
                "CONSTRAINT pk PRIMARY KEY (c4)");

        assertUpdate(getSession(), createTableWithOneConstraintSql);
        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedcreateTableWithOneConstraint);

        String insertStmt = format("INSERT INTO %s VALUES (1, 2.3, 'abc', 4)", tableName);
        assertQueryFails(insertStmt, format("Cannot write to table %s.%s since it has table constraints that are enforced", getSession().getSchema().get(), tableName));

        String dropConstraintStmt = format("ALTER TABLE %s.%s.%s DROP CONSTRAINT pk", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropConstraintStmt);

        String addPrimaryKeyStmt = "ALTER TABLE " + tableName + " ADD CONSTRAINT pk PRIMARY KEY (c2, c3) NOT ENFORCED";
        assertUpdate(getSession(), addPrimaryKeyStmt);
        assertUpdate(getSession(), insertStmt, 1);

        String addUniqueEnforced = "ALTER TABLE " + tableName + " ADD CONSTRAINT uq1 UNIQUE (c1) ENFORCED";
        assertUpdate(getSession(), addUniqueEnforced);
        assertQueryFails(insertStmt, format("Cannot write to table %s.%s since it has table constraints that are enforced", getSession().getSchema().get(), tableName));

        String addSecondUniqueNotEnforced = "ALTER TABLE " + tableName + " ADD CONSTRAINT uq2 UNIQUE (c4) NOT ENFORCED";
        assertUpdate(getSession(), addSecondUniqueNotEnforced);
        assertQueryFails(insertStmt, format("Cannot write to table %s.%s since it has table constraints that are enforced", getSession().getSchema().get(), tableName));

        dropConstraintStmt = format("ALTER TABLE %s.%s.%s DROP CONSTRAINT uq1", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropConstraintStmt);
        assertUpdate(getSession(), insertStmt, 1);

        String dropTableStmt = format("DROP TABLE %s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropTableStmt);

        String createTableWithInlineConstraintsFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint%s,\n" +
                "   %s double%s,\n" +
                "   %s varchar%s,\n" +
                "   %s bigint%s\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";

        String createTableWithNotNullConstraintsSql = format(
                createTableWithInlineConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "c1",
                "",
                "c2",
                " NOT NULL",
                "c3",
                "",
                "c4",
                " NOT NULL");

        assertUpdate(getSession(), createTableWithNotNullConstraintsSql);
        assertUpdate(getSession(), insertStmt, 1);
        insertStmt = format("INSERT INTO %s VALUES (1, null, 'abc', 4)", tableName);
        assertQueryFails(insertStmt, "NULL value not allowed for NOT NULL column: c2");
        insertStmt = format("INSERT INTO %s VALUES (1, 2.3, 'abc', null)", tableName);
        assertQueryFails(insertStmt, "NULL value not allowed for NOT NULL column: c4");
        insertStmt = format("INSERT INTO %s VALUES (null, 2.3, null, 4)", tableName);
        assertUpdate(getSession(), insertStmt, 1);

        assertUpdate(getSession(), dropTableStmt);
    }

    @Test
    public void testAlterColumnNotNull()
    {
        String tableName = "test_table_alter_column_not_null";

        String createTableWithInlineConstraintsFormat = "CREATE TABLE %s.%s.%s (\n" +
                "   %s bigint%s,\n" +
                "   %s double%s,\n" +
                "   %s varchar%s,\n" +
                "   %s bigint%s\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";

        String createTableSql = format(
                createTableWithInlineConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "c1",
                "",
                "c2",
                "",
                "c3",
                "",
                "c4",
                "");

        String expectedCreateTableSql = format(
                createTableWithInlineConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "",
                "\"c2\"",
                "",
                "\"c3\"",
                "",
                "\"c4\"",
                "");

        assertUpdate(getSession(), createTableSql);
        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedCreateTableSql);

        @Language("SQL") String addNotNullStmt = "ALTER TABLE " + tableName + " ALTER COLUMN c2 SET NOT NULL";
        assertUpdate(getSession(), addNotNullStmt);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        expectedCreateTableSql = format(
                createTableWithInlineConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "",
                "\"c2\"",
                " NOT NULL",
                "\"c3\"",
                "",
                "\"c4\"",
                "");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedCreateTableSql);

        addNotNullStmt = "ALTER TABLE " + tableName + " ALTER COLUMN c4 SET NOT NULL";
        assertUpdate(getSession(), addNotNullStmt);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        expectedCreateTableSql = format(
                createTableWithInlineConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "",
                "\"c2\"",
                " NOT NULL",
                "\"c3\"",
                "",
                "\"c4\"",
                " NOT NULL");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedCreateTableSql);

        @Language("SQL") String dropNotNullStmt = "ALTER TABLE " + tableName + " ALTER COLUMN c2 DROP NOT NULL";
        assertUpdate(getSession(), dropNotNullStmt);
        actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        expectedCreateTableSql = format(
                createTableWithInlineConstraintsFormat,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                "\"c1\"",
                "",
                "\"c2\"",
                "",
                "\"c3\"",
                "",
                "\"c4\"",
                " NOT NULL");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), expectedCreateTableSql);

        @Language("SQL") String addColumnStmt = "ALTER TABLE " + tableName + " ADD COLUMN c5 int NOT NULL";
        assertQueryFails(addColumnStmt, "This connector does not support ADD COLUMN with NOT NULL");

        String dropTableStmt = format("DROP TABLE %s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), tableName);
        assertUpdate(getSession(), dropTableStmt);
    }

    protected String retentionDays(int days)
    {
        return "";
    }

    protected String withRetentionDays(int days)
    {
        return "";
    }

    private Session getTableWriteTestingSession(boolean optimizedPartitionUpdateSerializationEnabled)
    {
        return Session.builder(getSession())
                .setSystemProperty("task_writer_count", "4")
                .setCatalogSessionProperty(catalog, OPTIMIZED_PARTITION_UPDATE_SERIALIZATION_ENABLED, optimizedPartitionUpdateSerializationEnabled + "")
                .build();
    }

    private void assertOneNotNullResult(@Language("SQL") String query)
    {
        MaterializedResult results = getQueryRunner().execute(getSession(), query).toTestTypes();
        assertEquals(results.getRowCount(), 1);
        assertEquals(results.getMaterializedRows().get(0).getFieldCount(), 1);
        assertNotNull(results.getMaterializedRows().get(0).getField(0));
    }

    private boolean insertOperationsSupported(HiveStorageFormat storageFormat)
    {
        return storageFormat != DWRF;
    }

    private Type canonicalizeType(Type type)
    {
        HiveType hiveType = HiveType.toHiveType(typeTranslator, type);
        return FUNCTION_AND_TYPE_MANAGER.getType(hiveType.getTypeSignature());
    }

    private String canonicalizeTypeName(String type)
    {
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(type);
        return canonicalizeType(FUNCTION_AND_TYPE_MANAGER.getType(typeSignature)).toString();
    }

    private void assertColumnType(TableMetadata tableMetadata, String columnName, Type expectedType)
    {
        assertEquals(tableMetadata.getColumn(columnName).getType(), canonicalizeType(expectedType));
    }

    private void verifyPartition(boolean hasPartition, TableMetadata tableMetadata, List<String> partitionKeys)
    {
        Object partitionByProperty = tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY);
        if (hasPartition) {
            assertEquals(partitionByProperty, partitionKeys);
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                boolean partitionKey = partitionKeys.contains(columnMetadata.getName());
                assertEquals(columnMetadata.getExtraInfo().orElse(null), columnExtraInfo(partitionKey));
            }
        }
        else {
            assertNull(partitionByProperty);
        }
    }

    private void rollback()
    {
        throw new RollbackException();
    }

    private static class RollbackException
            extends RuntimeException
    {
    }

    private static String getExpectedErrorMessageForInsertExistingBucketedTable(InsertExistingPartitionsBehavior behavior, String partitionName)
    {
        if (behavior == InsertExistingPartitionsBehavior.APPEND) {
            return "Cannot insert into existing partition of bucketed Hive table: " + partitionName;
        }
        if (behavior == InsertExistingPartitionsBehavior.ERROR) {
            return "Cannot insert into an existing partition of Hive table: " + partitionName;
        }
        throw new IllegalArgumentException("Unexpected insertExistingPartitionsBehavior: " + behavior);
    }

    private static ConnectorSession getConnectorSession(Session session)
    {
        return session.toConnectorSession(new ConnectorId(session.getCatalog().get()));
    }

    private void testWithAllStorageFormats(BiConsumer<Session, HiveStorageFormat> test)
    {
        Session session = getSession();
        for (HiveStorageFormat storageFormat : getSupportedHiveStorageFormats()) {
            testWithStorageFormat(session, storageFormat, test);
        }
    }

    private static void testWithStorageFormat(Session session, HiveStorageFormat storageFormat, BiConsumer<Session, HiveStorageFormat> test)
    {
        try {
            test.accept(session, storageFormat);
        }
        catch (Exception | AssertionError e) {
            fail(format("Failure for format %s with properties %s", storageFormat, session.getConnectorProperties()), e);
        }
    }

    protected List<HiveStorageFormat> getSupportedHiveStorageFormats()
    {
        // CSV supports only unbounded VARCHAR type, and Alpha does not support DML yet
        return Arrays.stream(HiveStorageFormat.values())
                .filter(format -> format != HiveStorageFormat.CSV && format != HiveStorageFormat.ALPHA)
                .collect(toImmutableList());
    }
}
