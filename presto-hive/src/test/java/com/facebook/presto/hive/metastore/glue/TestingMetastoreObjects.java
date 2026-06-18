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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;

public final class TestingMetastoreObjects
{
    private TestingMetastoreObjects() {}

    // --------------- Glue Objects ---------------

    public static Database getGlueTestDatabase()
    {
        return Database.builder()
                .name("test-db" + generateRandom())
                .description("database desc")
                .locationUri("/db")
                .parameters(ImmutableMap.of())
                .build();
    }

    public static Table getGlueTestTable(String dbName)
    {
        return Table.builder()
                .databaseName(dbName)
                .name("test-tbl" + generateRandom())
                .owner("owner")
                .parameters(ImmutableMap.of())
                .partitionKeys(ImmutableList.of(getGlueTestColumn()))
                .storageDescriptor(getGlueTestStorageDescriptor())
                .tableType(EXTERNAL_TABLE.name())
                .viewOriginalText("originalText")
                .viewExpandedText("expandedText")
                .build();
    }

    public static Column getGlueTestColumn()
    {
        return Column.builder()
                .name("test-col" + generateRandom())
                .type("string")
                .comment("column comment")
                .build();
    }

    public static StorageDescriptor getGlueTestStorageDescriptor()
    {
        return StorageDescriptor.builder()
                .bucketColumns(ImmutableList.of("test-bucket-col"))
                .columns(ImmutableList.of(getGlueTestColumn()))
                .parameters(ImmutableMap.of())
                .serdeInfo(SerDeInfo.builder()
                        .serializationLibrary("SerdeLib")
                        .parameters(ImmutableMap.of())
                        .build())
                .inputFormat("InputFormat")
                .outputFormat("OutputFormat")
                .location("/test-tbl")
                .numberOfBuckets(1)
                .build();
    }

    public static Partition getGlueTestPartition(String dbName, String tblName, List<String> values)
    {
        return Partition.builder()
                .databaseName(dbName)
                .tableName(tblName)
                .values(values)
                .parameters(ImmutableMap.of())
                .storageDescriptor(getGlueTestStorageDescriptor())
                .build();
    }

    // --------------- Presto Objects ---------------

    public static com.facebook.presto.hive.metastore.Database getPrestoTestDatabase()
    {
        return com.facebook.presto.hive.metastore.Database.builder()
                .setDatabaseName("test-db" + generateRandom())
                .setComment(Optional.of("database desc"))
                .setLocation(Optional.of("/db"))
                .setParameters(ImmutableMap.of())
                .setOwnerName("PUBLIC")
                .setOwnerType(PrincipalType.ROLE).build();
    }

    public static com.facebook.presto.hive.metastore.Table getPrestoTestTable(String dbName)
    {
        return com.facebook.presto.hive.metastore.Table.builder()
                .setDatabaseName(dbName)
                .setTableName("test-tbl" + generateRandom())
                .setOwner("owner")
                .setParameters(ImmutableMap.of())
                .setTableType(EXTERNAL_TABLE)
                .setDataColumns(ImmutableList.of(getPrestoTestColumn()))
                .setPartitionColumns(ImmutableList.of(getPrestoTestColumn()))
                .setViewOriginalText(Optional.of("originalText"))
                .setViewExpandedText(Optional.of("expandedText"))
                .withStorage(STORAGE_CONSUMER).build();
    }

    public static com.facebook.presto.hive.metastore.Partition getPrestoTestPartition(String dbName, String tblName, List<String> values)
    {
        return com.facebook.presto.hive.metastore.Partition.builder()
                .setCatalogName(Optional.of("hive"))
                .setDatabaseName(dbName)
                .setTableName(tblName)
                .setValues(values)
                .setColumns(ImmutableList.of(getPrestoTestColumn()))
                .setParameters(ImmutableMap.of())
                .withStorage(STORAGE_CONSUMER).build();
    }

    public static com.facebook.presto.hive.metastore.Column getPrestoTestColumn()
    {
        return new com.facebook.presto.hive.metastore.Column("test-col" + generateRandom(), HiveType.HIVE_STRING, Optional.of("column comment"), Optional.empty());
    }

    private static final Consumer<Storage.Builder> STORAGE_CONSUMER = storage ->
    {
        storage.setStorageFormat(StorageFormat.create("SerdeLib", "InputFormat", "OutputFormat"))
                .setLocation("/test-tbl")
                .setBucketProperty(Optional.empty())
                .setSerdeParameters(ImmutableMap.of());
    };

    private static String generateRandom()
    {
        return String.format("%04x", ThreadLocalRandom.current().nextInt());
    }
}
