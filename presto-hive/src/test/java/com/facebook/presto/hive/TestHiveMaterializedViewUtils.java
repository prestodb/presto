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

import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveMaterializedViewUtils.differenceDataPredicates;
import static com.facebook.presto.hive.HiveMaterializedViewUtils.getEmptyMaterializedViewDataPredicates;
import static com.facebook.presto.hive.HiveMaterializedViewUtils.getMaterializedDataPredicates;
import static com.facebook.presto.hive.HiveMaterializedViewUtils.validateMaterializedViewPartitionColumns;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.metastore.MetastoreUtil.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static org.testng.Assert.assertEquals;

public class TestHiveMaterializedViewUtils
{
    private static final String SCHEMA_NAME = "schema";
    private static final String TABLE_NAME = "table";
    private static final String USER_NAME = "user";
    private static final String LOCATION = "test/location";
    private static final String QUERY_ID = "queryId";
    private static final String SQL = "sql";

    private final LiteralEncoder literalEncoder = new LiteralEncoder(new TestingBlockEncodingSerde());
    private final MetastoreContext metastoreContext = new MetastoreContext(USER_NAME, QUERY_ID, Optional.empty(), Optional.empty(), Optional.empty());

    @Test
    public void testMaterializedDataPredicates()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();

        List<String> keys = ImmutableList.of("ds", "category");
        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column categoryColumn = new Column("category", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn, categoryColumn);
        List<String> partitions = ImmutableList.of(
                "ds=2020-01-01/category=c1",
                "ds=2020-01-01/category=c2",
                "ds=2020-01-02/category=c1",
                "ds=2020-01-02/category=c2");
        testMetastore.setPartitionNames(partitions);

        ImmutableList.Builder<List<TestingPartitionResult>> partitionResults = ImmutableList.builder();
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-01' AS varchar)"),
                new TestingPartitionResult("category", VARCHAR, "CAST('c1' AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-01' AS varchar)"),
                new TestingPartitionResult("category", VARCHAR, "CAST('c2' AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-02' AS varchar)"),
                new TestingPartitionResult("category", VARCHAR, "CAST('c1' AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-02' AS varchar)"),
                new TestingPartitionResult("category", VARCHAR, "CAST('c2' AS varchar)")));

        MaterializedDataPredicates materializedDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(partitionColumns), DateTimeZone.UTC);
        comparePredicates(materializedDataPredicates, keys, partitionResults.build());
    }

    @Test
    public void testMaterializedDataPredicatesWithNullPartitions()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();
        List<String> keys = ImmutableList.of("ds", "category");
        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column categoryColumn = new Column("category", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn, categoryColumn);
        List<String> partitions = ImmutableList.of(
                "ds=2020-01-01/category=c1",
                "ds=2020-01-01/category=" + HIVE_DEFAULT_DYNAMIC_PARTITION,
                "ds=2020-01-02/category=c1",
                "ds=" + HIVE_DEFAULT_DYNAMIC_PARTITION + "/category=c2");
        testMetastore.setPartitionNames(partitions);

        ImmutableList.Builder<List<TestingPartitionResult>> partitionResults = ImmutableList.builder();
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-01' AS varchar)"),
                new TestingPartitionResult("category", VARCHAR, "CAST('c1' AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-01' AS varchar)"),
                new TestingPartitionResult("category", VARCHAR, "CAST(null AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-02' AS varchar)"),
                new TestingPartitionResult("category", VARCHAR, "CAST('c1' AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST(null AS varchar)"),
                new TestingPartitionResult("category", VARCHAR, "CAST('c2' AS varchar)")));

        MaterializedDataPredicates materializedDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(partitionColumns), DateTimeZone.UTC);
        comparePredicates(materializedDataPredicates, keys, partitionResults.build());
    }

    @Test
    public void testMaterializedDataPredicatesWithEmptyPartitions()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();
        List<String> keys = ImmutableList.of("ds", "category");
        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column categoryColumn = new Column("category", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn, categoryColumn);
        List<String> partitions = ImmutableList.of();
        testMetastore.setPartitionNames(partitions);

        ImmutableList.Builder<List<TestingPartitionResult>> partitionResults = ImmutableList.builder();
        MaterializedDataPredicates materializedDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(partitionColumns), DateTimeZone.UTC);
        comparePredicates(materializedDataPredicates, keys, partitionResults.build());
    }

    @Test
    public void testMaterializedDataPredicatesWithIntParitionType()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();
        List<String> keys = ImmutableList.of("ds", "code");
        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column codeColumn = new Column("code", HIVE_INT, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn, codeColumn);
        List<String> partitions = ImmutableList.of(
                "ds=2021-01-01/code=1",
                "ds=2021-01-01/code=2",
                "ds=2021-01-02/code=1",
                "ds=2021-01-02/code=2");
        testMetastore.setPartitionNames(partitions);

        ImmutableList.Builder<List<TestingPartitionResult>> partitionResults = ImmutableList.builder();
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2021-01-01' AS varchar)"),
                new TestingPartitionResult("code", INTEGER, "1")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2021-01-01' AS varchar)"),
                new TestingPartitionResult("code", INTEGER, "2")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2021-01-02' AS varchar)"),
                new TestingPartitionResult("code", INTEGER, "1")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2021-01-02' AS varchar)"),
                new TestingPartitionResult("code", INTEGER, "2")));

        MaterializedDataPredicates materializedDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(partitionColumns), DateTimeZone.UTC);
        comparePredicates(materializedDataPredicates, keys, partitionResults.build());
    }

    @Test
    public void testDifferenceDataPredicates()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();

        List<String> keys = ImmutableList.of("ds");
        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn);
        List<String> partitions = ImmutableList.of(
                "ds=2020-01-01",
                "ds=2020-01-02",
                "ds=2020-01-03",
                "ds=2020-01-04",
                "ds=2020-01-05",
                "ds=2020-01-06");

        testMetastore.setPartitionNames(partitions);

        MaterializedDataPredicates baseDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(partitionColumns), DateTimeZone.UTC);

        List<String> viewPartitions = ImmutableList.of(
                "ds=2020-01-02",
                "ds=2020-01-03",
                "ds=2020-01-05");
        testMetastore.setPartitionNames(viewPartitions);

        MaterializedDataPredicates materializedDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(partitionColumns), DateTimeZone.UTC);

        Map<String, String> materializedViewToBaseColumnMap = ImmutableMap.of("ds", "ds");

        ImmutableList.Builder<List<TestingPartitionResult>> partitionResults = ImmutableList.builder();
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-01' AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-04' AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-06' AS varchar)")));

        MaterializedDataPredicates diffDataPredicates = differenceDataPredicates(baseDataPredicates, materializedDataPredicates, materializedViewToBaseColumnMap);
        comparePredicates(diffDataPredicates, keys, partitionResults.build());
    }

    @Test
    public void testDifferenceDataPredicatesWithAlias()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();

        List<String> keys = ImmutableList.of("ds", "shipmode");
        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column shipmodeColumn = new Column("shipmode", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn, shipmodeColumn);
        List<String> partitions = ImmutableList.of(
                "ds=2020-01-01/shipmode=air",
                "ds=2020-01-01/shipmode=road",
                "ds=2020-01-02/shipmode=air",
                "ds=2020-01-02/shipmode=road");
        testMetastore.setPartitionNames(partitions);

        MaterializedDataPredicates baseDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(partitionColumns), DateTimeZone.UTC);

        Column viewShipModeColumn = new Column("view_shipmode", HIVE_STRING, Optional.empty());
        List<Column> viewPartitionColumns = ImmutableList.of(dsColumn, viewShipModeColumn);
        List<String> viewPartitions = ImmutableList.of(
                "ds=2020-01-01/view_shipmode=air",
                "ds=2020-01-01/view_shipmode=road");
        testMetastore.setPartitionNames(viewPartitions);

        MaterializedDataPredicates materializedDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(viewPartitionColumns), DateTimeZone.UTC);

        Map<String, String> materializedViewToBaseColumnMap = ImmutableMap.of("ds", "ds", "view_shipmode", "shipmode");

        ImmutableList.Builder<List<TestingPartitionResult>> partitionResults = ImmutableList.builder();
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-02' AS varchar)"),
                new TestingPartitionResult("shipmode", VARCHAR, "CAST('air' AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-02' AS varchar)"),
                new TestingPartitionResult("shipmode", VARCHAR, "CAST('road' AS varchar)")));

        MaterializedDataPredicates diffDataPredicates = differenceDataPredicates(baseDataPredicates, materializedDataPredicates, materializedViewToBaseColumnMap);
        comparePredicates(diffDataPredicates, keys, partitionResults.build());
    }

    @Test
    public void testDifferenceDataPredicatesWithDifferentExtraPartitions()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();

        List<String> keys = ImmutableList.of("ds", "shipmode");
        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column shipmodeColumn = new Column("shipmode", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn, shipmodeColumn);
        List<String> partitions = ImmutableList.of(
                "ds=2020-01-01/shipmode=air",
                "ds=2020-01-01/shipmode=road",
                "ds=2020-01-02/shipmode=air",
                "ds=2020-01-02/shipmode=road");
        testMetastore.setPartitionNames(partitions);

        MaterializedDataPredicates baseDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(partitionColumns), DateTimeZone.UTC);

        Column viewShipModeColumn = new Column("view_shipmode", HIVE_STRING, Optional.empty());
        List<Column> viewPartitionColumns = ImmutableList.of(dsColumn, viewShipModeColumn);
        List<String> viewPartitions = ImmutableList.of(
                "ds=2020-01-01/view_shipmode=air",
                "ds=2020-01-01/view_shipmode=road");
        testMetastore.setPartitionNames(viewPartitions);

        MaterializedDataPredicates materializedDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(viewPartitionColumns), DateTimeZone.UTC);

        Map<String, String> materializedViewToBaseColumnMap = ImmutableMap.of("ds", "ds");

        ImmutableList.Builder<List<TestingPartitionResult>> partitionResults = ImmutableList.builder();
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-02' AS varchar)"),
                new TestingPartitionResult("shipmode", VARCHAR, "CAST('air' AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-02' AS varchar)"),
                new TestingPartitionResult("shipmode", VARCHAR, "CAST('road' AS varchar)")));
        MaterializedDataPredicates diffDataPredicates = differenceDataPredicates(baseDataPredicates, materializedDataPredicates, materializedViewToBaseColumnMap);
        comparePredicates(diffDataPredicates, keys, partitionResults.build());
    }

    @Test
    public void testDifferenceDataPredicatesFullyMaterialized()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();

        List<String> keys = ImmutableList.of("ds", "shipmode");
        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column shipmodeColumn = new Column("shipmode", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn, shipmodeColumn);
        List<String> partitions = ImmutableList.of(
                "ds=2020-01-01/shipmode=air",
                "ds=2020-01-01/shipmode=road",
                "ds=2020-01-02/shipmode=air",
                "ds=2020-01-02/shipmode=road");
        testMetastore.setPartitionNames(partitions);

        MaterializedDataPredicates baseDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(partitionColumns), DateTimeZone.UTC);

        Column viewShipModeColumn = new Column("view_shipmode", HIVE_STRING, Optional.empty());
        List<Column> viewPartitionColumns = ImmutableList.of(dsColumn, viewShipModeColumn);
        List<String> viewPartitions = ImmutableList.of(
                "ds=2020-01-01/view_shipmode=air",
                "ds=2020-01-01/view_shipmode=road",
                "ds=2020-01-02/view_shipmode=air",
                "ds=2020-01-02/view_shipmode=road");
        testMetastore.setPartitionNames(viewPartitions);

        MaterializedDataPredicates materializedDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(viewPartitionColumns), DateTimeZone.UTC);

        Map<String, String> materializedViewToBaseColumnMap = ImmutableMap.of("ds", "ds", "view_shipmode", "shipmode");

        ImmutableList.Builder<List<TestingPartitionResult>> partitionResults = ImmutableList.builder();

        MaterializedDataPredicates diffDataPredicates = differenceDataPredicates(baseDataPredicates, materializedDataPredicates, materializedViewToBaseColumnMap);
        comparePredicates(diffDataPredicates, keys, partitionResults.build());
    }

    @Test
    public void testDifferenceDataPredicatesNotMaterialized()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();

        List<String> keys = ImmutableList.of("ds", "shipmode");
        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column shipmodeColumn = new Column("shipmode", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn, shipmodeColumn);
        List<String> partitions = ImmutableList.of(
                "ds=2020-01-01/shipmode=air",
                "ds=2020-01-01/shipmode=road",
                "ds=2020-01-02/shipmode=air",
                "ds=2020-01-02/shipmode=road");
        testMetastore.setPartitionNames(partitions);

        MaterializedDataPredicates baseDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(partitionColumns), DateTimeZone.UTC);

        Column viewShipModeColumn = new Column("view_shipmode", HIVE_STRING, Optional.empty());
        List<Column> viewPartitionColumns = ImmutableList.of(dsColumn, viewShipModeColumn);
        List<String> viewPartitions = ImmutableList.of();
        testMetastore.setPartitionNames(viewPartitions);

        MaterializedDataPredicates materializedDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(viewPartitionColumns), DateTimeZone.UTC);

        Map<String, String> materializedViewToBaseColumnMap = ImmutableMap.of("ds", "ds", "view_shipmode", "shipmode");

        ImmutableList.Builder<List<TestingPartitionResult>> partitionResults = ImmutableList.builder();
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-01' AS varchar)"),
                new TestingPartitionResult("shipmode", VARCHAR, "CAST('air' AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-01' AS varchar)"),
                new TestingPartitionResult("shipmode", VARCHAR, "CAST('road' AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-02' AS varchar)"),
                new TestingPartitionResult("shipmode", VARCHAR, "CAST('air' AS varchar)")));
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-02' AS varchar)"),
                new TestingPartitionResult("shipmode", VARCHAR, "CAST('road' AS varchar)")));

        MaterializedDataPredicates diffDataPredicates = differenceDataPredicates(baseDataPredicates, materializedDataPredicates, materializedViewToBaseColumnMap);
        comparePredicates(diffDataPredicates, keys, partitionResults.build());
    }

    @Test
    public void testDifferenceDataPredicatesEmptyDataPredicates()
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();

        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column shipmodeColumn = new Column("shipmode", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn, shipmodeColumn);
        List<String> partitions = ImmutableList.of(
                "ds=2020-01-01/shipmode=air",
                "ds=2020-01-01/shipmode=road");
        testMetastore.setPartitionNames(partitions);

        MaterializedDataPredicates baseDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(partitionColumns), DateTimeZone.UTC);

        Column viewShipModeColumn = new Column("view_shipmode", HIVE_STRING, Optional.empty());
        List<Column> viewPartitionColumns = ImmutableList.of(dsColumn, viewShipModeColumn);
        List<String> viewPartitions = ImmutableList.of();
        testMetastore.setPartitionNames(viewPartitions);

        MaterializedDataPredicates materializedDataPredicates =
                getMaterializedDataPredicates(testMetastore, metastoreContext, typeManager, getTable(viewPartitionColumns), DateTimeZone.UTC);

        Map<String, String> materializedViewToBaseColumnMap = ImmutableMap.of();

        ImmutableList.Builder<List<TestingPartitionResult>> partitionResults = ImmutableList.builder();
        partitionResults.add(ImmutableList.of(
                new TestingPartitionResult("ds", VARCHAR, "CAST('2020-01-01' AS varchar)"),
                new TestingPartitionResult("shipmode", VARCHAR, "CAST('air' AS varchar)")));

        MaterializedDataPredicates diffDataPredicates = differenceDataPredicates(baseDataPredicates, materializedDataPredicates, materializedViewToBaseColumnMap);
        assertEquals(diffDataPredicates, getEmptyMaterializedViewDataPredicates());
    }

    @Test
    public void testValidateMaterializedViewPartitionColumnsOneColumnMatch()
    {
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();

        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column shipmodeColumn = new Column("shipmode", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn, shipmodeColumn);

        SchemaTableName tableName = new SchemaTableName(SCHEMA_NAME, TABLE_NAME);
        Map<String, Map<SchemaTableName, String>> originalColumnMapping = ImmutableMap.of(dsColumn.getName(), ImmutableMap.of(tableName, dsColumn.getName()));
        testMetastore.addTable(SCHEMA_NAME, TABLE_NAME, getTable(partitionColumns), ImmutableList.of());
        List<Column> viewPartitionColumns = ImmutableList.of(dsColumn);

        validateMaterializedViewPartitionColumns(testMetastore, metastoreContext, getTable(viewPartitionColumns), getConnectorMaterializedViewDefinition(ImmutableList.of(tableName), originalColumnMapping));
    }

    @Test
    public void testValidateMaterializedViewPartitionColumnsTwoColumnMatchDifferentTable()
    {
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();

        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column shipmodeColumn = new Column("shipmode", HIVE_STRING, Optional.empty());

        SchemaTableName tableName1 = new SchemaTableName(SCHEMA_NAME, TABLE_NAME);
        testMetastore.addTable(SCHEMA_NAME, TABLE_NAME, getTable(ImmutableList.of(dsColumn)), ImmutableList.of());

        String table2 = "table2";
        SchemaTableName tableName2 = new SchemaTableName(SCHEMA_NAME, table2);
        testMetastore.addTable(SCHEMA_NAME, table2, getTable(table2, ImmutableList.of(shipmodeColumn)), ImmutableList.of());

        Map<String, Map<SchemaTableName, String>> originalColumnMapping = ImmutableMap.of(
                dsColumn.getName(), ImmutableMap.of(tableName1, dsColumn.getName()),
                shipmodeColumn.getName(), ImmutableMap.of(tableName2, shipmodeColumn.getName()));
        List<Column> viewPartitionColumns = ImmutableList.of(dsColumn, shipmodeColumn);

        validateMaterializedViewPartitionColumns(testMetastore, metastoreContext, getTable(viewPartitionColumns), getConnectorMaterializedViewDefinition(ImmutableList.of(tableName1, tableName2), originalColumnMapping));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Materialized view schema.table must have at least one column directly defined by a base table column.")
    public void testValidateMaterializedViewPartitionColumnsEmptyBaseColumnMap()
    {
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();

        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column shipmodeColumn = new Column("shipmode", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn, shipmodeColumn);

        SchemaTableName tableName = new SchemaTableName(SCHEMA_NAME, TABLE_NAME);
        Map<String, Map<SchemaTableName, String>> originalColumnMapping = ImmutableMap.of();
        testMetastore.addTable(SCHEMA_NAME, TABLE_NAME, getTable(partitionColumns), ImmutableList.of());
        List<Column> viewPartitionColumns = ImmutableList.of(dsColumn);

        validateMaterializedViewPartitionColumns(testMetastore, metastoreContext, getTable(viewPartitionColumns), getConnectorMaterializedViewDefinition(ImmutableList.of(tableName), originalColumnMapping));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Unpartitioned materialized view is not supported.")
    public void testValidateMaterializedViewPartitionColumnsEmptyViewPartition()
    {
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();

        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column shipmodeColumn = new Column("shipmode", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(dsColumn, shipmodeColumn);

        SchemaTableName tableName = new SchemaTableName(SCHEMA_NAME, TABLE_NAME);
        Map<String, Map<SchemaTableName, String>> originalColumnMapping = ImmutableMap.of(dsColumn.getName(), ImmutableMap.of(tableName, dsColumn.getName()));
        testMetastore.addTable(SCHEMA_NAME, TABLE_NAME, getTable(partitionColumns), ImmutableList.of());
        List<Column> viewPartitionColumns = ImmutableList.of();

        validateMaterializedViewPartitionColumns(testMetastore, metastoreContext, getTable(viewPartitionColumns), getConnectorMaterializedViewDefinition(ImmutableList.of(tableName), originalColumnMapping));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Materialized view schema.table must have at least partition to base table partition mapping for all base tables.")
    public void testValidateMaterializedViewPartitionColumnsNoneCommonPartition()
    {
        TestingSemiTransactionalHiveMetastore testMetastore = TestingSemiTransactionalHiveMetastore.create();

        Column dsColumn = new Column("ds", HIVE_STRING, Optional.empty());
        Column shipmodeColumn = new Column("shipmode", HIVE_STRING, Optional.empty());
        List<Column> partitionColumns = ImmutableList.of(shipmodeColumn);

        SchemaTableName tableName = new SchemaTableName(SCHEMA_NAME, TABLE_NAME);
        Map<String, Map<SchemaTableName, String>> originalColumnMapping = ImmutableMap.of(dsColumn.getName(), ImmutableMap.of(tableName, dsColumn.getName()));
        testMetastore.addTable(SCHEMA_NAME, TABLE_NAME, getTable(partitionColumns), ImmutableList.of());
        List<Column> viewPartitionColumns = ImmutableList.of(dsColumn);

        validateMaterializedViewPartitionColumns(testMetastore, metastoreContext, getTable(viewPartitionColumns), getConnectorMaterializedViewDefinition(ImmutableList.of(tableName), originalColumnMapping));
    }

    private void comparePredicates(MaterializedDataPredicates dataPredicates, List<String> keys, ImmutableList<List<TestingPartitionResult>> results)
    {
        List<String> columnNames = dataPredicates.getColumnNames();
        assertEquals(keys, columnNames);
        List<TupleDomain<String>> predicates = dataPredicates.getPredicateDisjuncts();

        assertEquals(predicates.size(), results.size());
        Iterator<List<TestingPartitionResult>> resultIterator = results.listIterator();
        predicates.forEach(predicate -> {
            Optional<Map<String, NullableValue>> nullableValues = TupleDomain.extractFixedValues(predicate);
            List<TestingPartitionResult> result = resultIterator.next();
            Map<String, NullableValue> nullableValueMap = nullableValues.orElseThrow(() -> new IllegalStateException("nullableValues is not present"));
            assertEquals(nullableValueMap.size(), result.size());
            Iterator<TestingPartitionResult> partitionResultIterator = result.iterator();
            nullableValueMap.forEach((key, value) -> {
                TestingPartitionResult partitionResult = partitionResultIterator.next();
                assertEquals(key, partitionResult.columnName);
                comparePartitionValueExpression(value, partitionResult);
            });
        });
    }

    private void comparePartitionValueExpression(NullableValue nullableValue, TestingPartitionResult partitionResult)
    {
        Expression expression = literalEncoder.toExpression(nullableValue.getValue(), nullableValue.getType(), false);
        assertEquals(nullableValue.getType(), partitionResult.type);
        assertEquals(expression.toString(), partitionResult.partitionValue);
    }

    private static Table getTable(List<Column> partitionColumns)
    {
        return getTable(TABLE_NAME, partitionColumns);
    }

    private static Table getTable(String tableName, List<Column> partitionColumns)
    {
        return new Table(
                SCHEMA_NAME,
                tableName,
                USER_NAME,
                PrestoTableType.MANAGED_TABLE,
                new Storage(fromHiveStorageFormat(ORC),
                        LOCATION,
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                ImmutableList.of(),
                partitionColumns,
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty());
    }

    private static class TestingPartitionResult
    {
        private final String columnName;
        private final Type type;
        private final String partitionValue;

        public TestingPartitionResult(String columnName, Type type, String partitionValue)
        {
            this.columnName = columnName;
            this.type = type;
            this.partitionValue = partitionValue;
        }
    }

    private static ConnectorMaterializedViewDefinition getConnectorMaterializedViewDefinition(
            List<SchemaTableName> tables,
            Map<String, Map<SchemaTableName, String>> originalColumnMapping)
    {
        return new ConnectorMaterializedViewDefinition(SQL, SCHEMA_NAME, TABLE_NAME, tables, Optional.empty(), originalColumnMapping, Optional.empty());
    }
}
