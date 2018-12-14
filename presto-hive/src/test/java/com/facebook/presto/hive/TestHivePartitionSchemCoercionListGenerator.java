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

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.testng.Assert.assertEquals;

public class TestHivePartitionSchemCoercionListGenerator
{
    private static final String READ_BY_NAME_SERDE = "com.facebook.presto.hive.ReadByNameSerde";
    private static final String PARTITION_COLUMN_NAME = "part1";
    private static final String BASE_TABLE_NAME = "TABLE1";
    private static final String BASE_TABLE_DATABASE_NAME = "DATABASE";
    private static final SchemaTableName BASE_TABLE_SCHEMA_NAME = new SchemaTableName(BASE_TABLE_DATABASE_NAME, BASE_TABLE_NAME);

    private static final ImmutableList<Column> BASE_TABLE_COLUMN_LIST = ImmutableList.of(
            createColumn("col1", HiveType.HIVE_SHORT),
            createColumn("col2", HiveType.HIVE_INT),
            createColumn("col3", HiveType.HIVE_LONG),
            createColumn("col4", HiveType.HIVE_DOUBLE),
            createColumn("col5", HiveType.HIVE_FLOAT),
            createColumn("col6", HiveType.HIVE_STRING),
            createColumn("col7", HiveType.HIVE_BYTE),
            createColumn("col8", HiveType.HIVE_BOOLEAN));

    private static final ImmutableList<CoercionPair> COERSION_PAIRS = ImmutableList.of(
            new CoercionPair(HiveType.HIVE_SHORT, HiveType.HIVE_INT),
            new CoercionPair(HiveType.HIVE_SHORT, HiveType.HIVE_LONG),
            new CoercionPair(HiveType.HIVE_INT, HiveType.HIVE_LONG),
            new CoercionPair(HiveType.HIVE_FLOAT, HiveType.HIVE_DOUBLE),
            new CoercionPair(HiveType.HIVE_STRING, HiveType.HIVE_BYTE),
            new CoercionPair(HiveType.HIVE_STRING, HiveType.HIVE_SHORT),
            new CoercionPair(HiveType.HIVE_STRING, HiveType.HIVE_INT),
            new CoercionPair(HiveType.HIVE_STRING, HiveType.HIVE_LONG),
            new CoercionPair(HiveType.HIVE_BYTE, HiveType.HIVE_SHORT),
            new CoercionPair(HiveType.HIVE_BYTE, HiveType.HIVE_INT),
            new CoercionPair(HiveType.HIVE_BYTE, HiveType.HIVE_LONG));

    private static class CoercionPair
    {
        private final HiveType from;
        private final HiveType to;

        CoercionPair(HiveType from, HiveType to)
        {
            this.from = from;
            this.to = to;
        }

        public HiveType getFrom()
        {
            return from;
        }

        public HiveType getTo()
        {
            return to;
        }
    }

    private Table baseTable;
    private Table baseTableReadByName;

    private TypeRegistry typeManager = new TypeRegistry();
    private HiveClientConfig hiveClientConfig = new HiveClientConfig();
    private HivePartitionSchemaCoercionListGenerator hivePartitionSchemaCoercionListGenerator =
            new HivePartitionSchemaCoercionListGenerator(new HiveCoercionPolicy(typeManager), hiveClientConfig);

    @BeforeTest
    public void before()
    {
        typeManager = new TypeRegistry();
        hiveClientConfig = new HiveClientConfig();
        hiveClientConfig.setReadAccessByNameSerdeList(READ_BY_NAME_SERDE);

        hivePartitionSchemaCoercionListGenerator = new HivePartitionSchemaCoercionListGenerator(new HiveCoercionPolicy(typeManager), hiveClientConfig);

        Table.Builder baseTableBuilder = Table.builder()
                .setDatabaseName(BASE_TABLE_DATABASE_NAME)
                .setTableName(BASE_TABLE_NAME)
                .setOwner("")
                .setTableType("")
                .setDataColumns(BASE_TABLE_COLUMN_LIST)
                .setPartitionColumns(ImmutableList.of(createColumn(PARTITION_COLUMN_NAME, HiveType.HIVE_INT)));
        baseTableBuilder.getStorageBuilder()
                .setBucketProperty(Optional.empty())
                .setLocation("")
                .setSerdeParameters(ImmutableMap.of())
                .setStorageFormat(StorageFormat.createNullable("", "", ""));
        baseTable = baseTableBuilder.build();

        Table.Builder readByNameBuilder = Table.builder(baseTable);
        readByNameBuilder.getStorageBuilder().setStorageFormat(StorageFormat.createNullable(READ_BY_NAME_SERDE, "", ""));
        baseTableReadByName = readByNameBuilder.build();
    }

    @Test
    public void testPartitionSchemaMatching()
    {
        ImmutableList<Column> partitionColumnList = ImmutableList.copyOf(baseTable.getDataColumns());

        Partition partition = createPartition(partitionColumnList);
        ImmutableMap<Integer, HiveTypeName> coercedMap = hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTable, BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);

        assertEquals(coercedMap.size(), 0);

        coercedMap = hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTableReadByName, BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);
        assertEquals(coercedMap.size(), 0);
    }

    @Test
    public void testNewColumnToEndOfPartition()
    {
        ImmutableList.Builder<Column> partitionColumnListBuilder = ImmutableList.builder();
        partitionColumnListBuilder.addAll(baseTable.getDataColumns()).add(createColumn("col9", HiveType.HIVE_FLOAT)).build();

        Partition partition = createPartition(partitionColumnListBuilder.build());
        ImmutableMap<Integer, HiveTypeName> coercedMap = hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTable, BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);

        assertEquals(coercedMap.size(), 0);

        coercedMap = hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTableReadByName, BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);
        assertEquals(coercedMap.size(), 0);
    }

    @Test
    public void testNewColumnAtEndOfTable()
    {
        ImmutableList.Builder<Column> partitionColumnListBuilder = ImmutableList.builder();
        partitionColumnListBuilder.addAll(baseTable.getDataColumns().subList(0, baseTable.getDataColumns().size() - 2)).build();

        Partition partition = createPartition(partitionColumnListBuilder.build());
        ImmutableMap<Integer, HiveTypeName> coercedMap = hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTable, BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);

        assertEquals(coercedMap.size(), 0);

        coercedMap = hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTableReadByName, BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);
        assertEquals(coercedMap.size(), 0);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testAccessByIndexNewColumnInMiddleOfPartition()
    {
        ImmutableList.Builder<Column> partitionColumnListBuilder = ImmutableList.builder();
        partitionColumnListBuilder.addAll(BASE_TABLE_COLUMN_LIST.subList(0, 3))
                .add(createColumn("col10", HiveType.HIVE_TIMESTAMP))
                .addAll(BASE_TABLE_COLUMN_LIST.subList(3, BASE_TABLE_COLUMN_LIST.size() - 1))
                .build();
        Partition partition = createPartition(partitionColumnListBuilder.build());
        ImmutableMap<Integer, HiveTypeName> coercedMap = hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTable, BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);
    }

    @Test
    public void testAccessByNameNewColumnInMiddleOfPartition()
    {
        ImmutableList.Builder<Column> partitionColumnListBuilder = ImmutableList.builder();
        partitionColumnListBuilder.addAll(BASE_TABLE_COLUMN_LIST.subList(0, 3))
                .add(createColumn("col10", HiveType.HIVE_TIMESTAMP))
                .addAll(BASE_TABLE_COLUMN_LIST.subList(3, BASE_TABLE_COLUMN_LIST.size() - 1))
                .build();
        Partition partition = createPartition(partitionColumnListBuilder.build());
        ImmutableMap<Integer, HiveTypeName> coercedMap =
                hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTableReadByName, BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);
        assertEquals(coercedMap.size(), 0);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testAccessByIndexNewColumnInMiddleOfTable()
    {
        ImmutableList.Builder<Column> partitionColumnListBuilder = ImmutableList.builder();
        partitionColumnListBuilder.addAll(BASE_TABLE_COLUMN_LIST.subList(0, 3))
                .addAll(BASE_TABLE_COLUMN_LIST.subList(4, BASE_TABLE_COLUMN_LIST.size() - 1))
                .build();
        Partition partition = createPartition(partitionColumnListBuilder.build());
        hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTable, BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);
    }

    @Test
    public void testAccessByNameNewColumnInMiddleOfTable()
    {
        ImmutableList.Builder<Column> partitionColumnListBuilder = ImmutableList.builder();
        partitionColumnListBuilder.addAll(BASE_TABLE_COLUMN_LIST.subList(0, 3))
                .addAll(BASE_TABLE_COLUMN_LIST.subList(4, BASE_TABLE_COLUMN_LIST.size() - 1))
                .build();
        Partition partition = createPartition(partitionColumnListBuilder.build());
        ImmutableMap<Integer, HiveTypeName> coercedMap =
                hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTableReadByName, BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);
        assertEquals(coercedMap.size(), 0);
    }

    @Test
    public void testReadByNameChangedDataTypes()
    {
        ImmutableList.Builder<Column> partitionColumnListBuilder = ImmutableList.builder();
        ImmutableList.Builder<Column> tableColumnListBuilder = ImmutableList.builder();
        populateTableAndPartitionColumnsWithCoersion(partitionColumnListBuilder, tableColumnListBuilder);
        Partition partition = createPartition(partitionColumnListBuilder.build());
        Table baseTableWithNewTypes = Table.builder(baseTableReadByName).setDataColumns(shuffleListOrder(tableColumnListBuilder.build())).build();
        ImmutableMap<Integer, HiveTypeName> coercedMap =
                hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTableWithNewTypes, BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);
        assertEquals(coercedMap.size(), COERSION_PAIRS.size());
    }

    @Test
    public void testReadByIndexChangedDataTypes()
    {
        ImmutableList.Builder<Column> partitionColumnListBuilder = ImmutableList.builder();
        ImmutableList.Builder<Column> tableColumnListBuilder = ImmutableList.builder();
        populateTableAndPartitionColumnsWithCoersion(partitionColumnListBuilder, tableColumnListBuilder);
        Partition partition = createPartition(partitionColumnListBuilder.build());
        Table baseTableWithNewTypes = Table.builder(baseTable).setDataColumns(tableColumnListBuilder.build()).build();
        ImmutableMap<Integer, HiveTypeName> coercedMap =
                hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTableWithNewTypes, BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);
        assertEquals(coercedMap.size(), COERSION_PAIRS.size());
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testParquetConfigByIndex()
    {
        ImmutableList.Builder<Column> partitionColumnListBuilder = ImmutableList.builder();
        ImmutableList.Builder<Column> tableColumnListBuilder = ImmutableList.builder();

        populateTableAndPartitionColumnsWithCoersion(partitionColumnListBuilder, tableColumnListBuilder);

        Partition partition = createPartition(partitionColumnListBuilder.build());
        Table.Builder baseTableWithNewTypes = Table.builder(baseTable)
                .setDataColumns(shuffleListOrder(tableColumnListBuilder.build()));
        baseTableWithNewTypes.getStorageBuilder()
                .setSerdeParameters(ImmutableMap.of("parquet.column.index.access", "true"))
                .setStorageFormat(StorageFormat.create("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "", ""));
        ImmutableMap<Integer, HiveTypeName> coercedMap =
                hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTableWithNewTypes.build(),
                        BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);
        assertEquals(coercedMap.size(), COERSION_PAIRS.size());
    }

    @Test
    public void testParquetConfigByName()
    {
        ImmutableList.Builder<Column> partitionColumnListBuilder = ImmutableList.builder();
        ImmutableList.Builder<Column> tableColumnListBuilder = ImmutableList.builder();

        populateTableAndPartitionColumnsWithCoersion(partitionColumnListBuilder, tableColumnListBuilder);

        Partition partition = createPartition(partitionColumnListBuilder.build());
        Table.Builder baseTableWithNewTypes = Table.builder(baseTable)
                .setDataColumns(shuffleListOrder(tableColumnListBuilder.build()));
        baseTableWithNewTypes.getStorageBuilder()
                .setSerdeParameters(ImmutableMap.of("parquet.column.index.access", "false"))
                .setStorageFormat(StorageFormat.create("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "", ""));
        ImmutableMap<Integer, HiveTypeName> coercedMap =
                hivePartitionSchemaCoercionListGenerator.validateAndCoercePartitionSchema(baseTableWithNewTypes.build(),
                        BASE_TABLE_SCHEMA_NAME, partition, PARTITION_COLUMN_NAME);
        assertEquals(coercedMap.size(), COERSION_PAIRS.size());
    }

    private void populateTableAndPartitionColumnsWithCoersion(ImmutableList.Builder<Column> partitionColumnListBuilder, ImmutableList.Builder<Column> tableColumnListBuilder)
    {
        int colNum = 0;
        for (CoercionPair pair : COERSION_PAIRS) {
            partitionColumnListBuilder.add(createColumn("col" + colNum, pair.from));
            tableColumnListBuilder.add(createColumn("col" + colNum, pair.to));
            colNum++;
        }
    }

    private Partition createPartition(ImmutableList<Column> colList)
    {
        Partition partition;
        Partition.Builder partitionBuilder = Partition.builder()
                .setColumns(colList)
                .setDatabaseName(BASE_TABLE_DATABASE_NAME)
                .setParameters(ImmutableMap.of())
                .setTableName(BASE_TABLE_NAME)
                .setValues(ImmutableList.of("1234"));
        partitionBuilder.getStorageBuilder().setStorageFormat(baseTable.getStorage().getStorageFormat())
                .setLocation("")
                .setSerdeParameters(ImmutableMap.of())
                .setBucketProperty(Optional.empty());
        partition = partitionBuilder.build();
        return partition;
    }

    private static Column createColumn(String colName, HiveType type)
    {
        return new Column(colName, type, Optional.empty());
    }

    private List<Column> shuffleListOrder(ImmutableList<Column> list)
    {
        ArrayList<Column> result = new ArrayList<>(list);
        Collections.shuffle(result, new Random(123456789L));
        return result;
    }
}
