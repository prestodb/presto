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

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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
        return new Database()
                .withName("test-db" + generateRandom())
                .withDescription("database desc")
                .withLocationUri("/db")
                .withParameters(ImmutableMap.of());
    }

    public static Table getGlueTestTable(String dbName)
    {
        return new Table()
                .withDatabaseName(dbName)
                .withName("test-tbl" + generateRandom())
                .withOwner("owner")
                .withParameters(ImmutableMap.of())
                .withPartitionKeys(ImmutableList.of(getGlueTestColumn()))
                .withStorageDescriptor(getGlueTestStorageDescriptor())
                .withTableType(EXTERNAL_TABLE.name())
                .withViewOriginalText("originalText")
                .withViewExpandedText("expandedText");
    }

    public static Column getGlueTestColumn()
    {
        return new Column()
                .withName("test-col" + generateRandom())
                .withType("string")
                .withComment("column comment");
    }

    public static StorageDescriptor getGlueTestStorageDescriptor()
    {
        return new StorageDescriptor()
                .withBucketColumns(ImmutableList.of("test-bucket-col"))
                .withColumns(ImmutableList.of(getGlueTestColumn()))
                .withParameters(ImmutableMap.of())
                .withSerdeInfo(new SerDeInfo()
                        .withSerializationLibrary("SerdeLib")
                        .withParameters(ImmutableMap.of()))
                .withInputFormat("InputFormat")
                .withOutputFormat("OutputFormat")
                .withLocation("/test-tbl")
                .withNumberOfBuckets(1);
    }

    public static Partition getGlueTestPartition(String dbName, String tblName, List<String> values)
    {
        return new Partition()
                .withDatabaseName(dbName)
                .withTableName(tblName)
                .withValues(values)
                .withParameters(ImmutableMap.of())
                .withStorageDescriptor(getGlueTestStorageDescriptor());
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
                .setDatabaseName(dbName)
                .setTableName(tblName)
                .setValues(values)
                .setColumns(ImmutableList.of(getPrestoTestColumn()))
                .setParameters(ImmutableMap.of())
                .withStorage(STORAGE_CONSUMER).build();
    }

    public static com.facebook.presto.hive.metastore.Column getPrestoTestColumn()
    {
        return new com.facebook.presto.hive.metastore.Column("test-col" + generateRandom(), HiveType.HIVE_STRING, Optional.of("column comment"));
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
