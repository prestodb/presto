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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.hive.cache.HiveCachingHdfsConfiguration;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HadoopExtendedFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveTestUtils.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_DDL;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertTrue;

public class TestGenericHiveRecordCursorProvider
{
    public static final HiveColumnHandle HOODIE_COMMIT_TIME = new HiveColumnHandle("_hoodie_commit_time", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 0, REGULAR, Optional.empty(), Optional.empty());
    public static final HiveColumnHandle HOODIE_COMMIT_SEQNO = new HiveColumnHandle("_hoodie_commit_seqno", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 1, REGULAR, Optional.empty(), Optional.empty());
    public static final HiveColumnHandle HOODIE_RECORD_KEY = new HiveColumnHandle("_hoodie_record_key", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 2, REGULAR, Optional.empty(), Optional.empty());
    public static final HiveColumnHandle HOODIE_PARTITION_PATH = new HiveColumnHandle("_hoodie_partition_path", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 3, REGULAR, Optional.empty(), Optional.empty());
    public static final HiveColumnHandle HOODIE_FILE_NAME = new HiveColumnHandle("_hoodie_file_name", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 4, REGULAR, Optional.empty(), Optional.empty());
    public static final HiveColumnHandle ID = new HiveColumnHandle("id", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 5, REGULAR, Optional.empty(), Optional.empty());
    public static final HiveColumnHandle LAST_UPDATE_MONTH = new HiveColumnHandle("last_update_month", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 6, REGULAR, Optional.empty(), Optional.empty());
    public static final HiveColumnHandle LAST_UPDATE_TIME = new HiveColumnHandle("last_update_time", HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 7, REGULAR, Optional.empty(), Optional.empty());
    public static final String CUSTOM_SPLIT_CLASS = "custom_split_class";
    public static final String HUDI_DELTA_FILEPATHS = "hudi_delta_filepaths";
    public static final String HUDI_BASEPATH = "hudi_basepath";
    public static final String HUDI_MAX_COMMIT_TIME = "hudi_max_commit_time";
    public static final String TABLE_NAME = "hudi_part_mor_rt";
    public static final String FILE_NAME = "b3711ddf-8c11-4666-82ec-fbc952e1dc72-0_1-61-24052_20210524095413.parquet";

    @Test
    public void shouldReturnHudiRecordCursorWithCopyOnFirstWriteEnabled()
    {
        Optional<RecordCursor> recordCursor = getRecordCursor(true);
        assertTrue(recordCursor.isPresent());
    }

    @Test
    public void shouldReturnHudiRecordCursorWithCopyOnFirstWriteDisabled()
    {
        Optional<RecordCursor> recordCursor = getRecordCursor(false);
        assertTrue(recordCursor.isPresent());
    }

    private static Optional<RecordCursor> getRecordCursor(Boolean isCopyOnFirstWriteConfigurationEnabled)
    {
        GenericHiveRecordCursorProvider genericHiveRecordCursorProvider = new GenericHiveRecordCursorProvider(
                new TestBackgroundHiveSplitLoader.TestingHdfsEnvironment(new ArrayList<>()));

        HiveFileSplit fileSplit = new HiveFileSplit(
                getTableBasePath(TABLE_NAME) + "/testPartition/" + FILE_NAME,
                0,
                435165,
                435165,
                1621850079,
                Optional.empty(),
                ImmutableMap.of(
                        CUSTOM_SPLIT_CLASS, "org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit",
                        HUDI_DELTA_FILEPATHS, "",
                        HUDI_BASEPATH, getTableBasePath(TABLE_NAME),
                        HUDI_MAX_COMMIT_TIME, "20210524095413"));

        return genericHiveRecordCursorProvider.createRecordCursor(
                isCopyOnFirstWriteConfigurationEnabled ? getHadoopConfWithCopyOnFirstWriteEnabled() : getHadoopConfWithCopyOnFirstWriteDisabled(),
                SESSION,
                fileSplit,
                createTestingSchema(),
                getAllColumns(),
                TupleDomain.all(),
                DateTimeZone.forID(SESSION.getSqlFunctionProperties().getTimeZoneKey().getId()),
                FUNCTION_AND_TYPE_MANAGER,
                false);
    }

    private static Configuration getHadoopConfWithCopyOnFirstWriteEnabled()
    {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        CopyOnFirstWriteConfiguration configuration = new CopyOnFirstWriteConfiguration(hadoopConf);
        return new HiveCachingHdfsConfiguration.CachingJobConf((factoryConfig, factoryUri) -> {
            FileSystem localFileSystem = new LocalFileSystem();
            try {
                localFileSystem.initialize(URI.create("file:///"), hadoopConf);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new HadoopExtendedFileSystem(localFileSystem);
        }, configuration);
    }

    private static Configuration getHadoopConfWithCopyOnFirstWriteDisabled()
    {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return new HiveCachingHdfsConfiguration.CachingJobConf((factoryConfig, factoryUri) -> {
            FileSystem localFileSystem = new LocalFileSystem();
            try {
                localFileSystem.initialize(URI.create("file:///"), hadoopConf);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new HadoopExtendedFileSystem(localFileSystem);
        }, hadoopConf);
    }

    private static Properties createTestingSchema()
    {
        List<HiveColumnHandle> schemaColumns = getAllColumns();
        Properties schema = new Properties();
        String columnNames = buildPropertyFromColumns(schemaColumns, HiveColumnHandle::getName);
        String columnTypeNames = buildPropertyFromColumns(schemaColumns, column -> column.getHiveType().getTypeInfo().getTypeName());
        schema.setProperty(LIST_COLUMNS, columnNames);
        schema.setProperty(LIST_COLUMN_TYPES, columnTypeNames);
        schema.setProperty("name", "test_schema." + TABLE_NAME);
        schema.setProperty(SERIALIZATION_DDL, "struct " + TABLE_NAME + " " +
                        "{ string _hoodie_commit_time, " +
                        "string _hoodie_commit_seqno, " +
                        "string _hoodie_record_key, " +
                        "string _hoodie_partition_path, " +
                        "string _hoodie_file_name, " +
                        "string id, " +
                        "string last_update_month, " +
                        "string last_update_time}");
        schema.setProperty(SERIALIZATION_FORMAT, "1");
        schema.setProperty("partition_columns", "creation_date");
        schema.setProperty("partition_columns.types", "string");
        schema.setProperty("last_modified_time", "1621850084");
        schema.setProperty("bucket_count", "0");
        schema.setProperty("last_commit_time_sync", "20210524095413");
        schema.setProperty("EXTERNAL", "TRUE");
        String deserializerClassName = ParquetHiveSerDe.class.getName();
        schema.setProperty(SERIALIZATION_LIB, deserializerClassName);
        String inputFormat = HoodieParquetRealtimeInputFormat.class.getName();
        schema.setProperty("file.inputformat", inputFormat);
        String outputFormat = MapredParquetOutputFormat.class.getName();
        schema.setProperty("file.outputformat", outputFormat);
        schema.setProperty("location", getTableBasePath(TABLE_NAME) + "/testPartition");
        schema.setProperty("last_modified_by", "hive");
        return schema;
    }

    private static String buildPropertyFromColumns(List<HiveColumnHandle> columns, Function<HiveColumnHandle, String> mapper)
    {
        if (columns.isEmpty()) {
            return "";
        }
        return columns.stream()
                .map(mapper)
                .collect(Collectors.joining(","));
    }

    private static List<HiveColumnHandle> getAllColumns()
    {
        return ImmutableList.of(
                HOODIE_COMMIT_TIME,
                HOODIE_COMMIT_SEQNO,
                HOODIE_RECORD_KEY,
                HOODIE_PARTITION_PATH,
                HOODIE_FILE_NAME,
                ID,
                LAST_UPDATE_MONTH,
                LAST_UPDATE_TIME);
    }

    private static String getTableBasePath(String tableName)
    {
        return TestGenericHiveRecordCursorProvider.class.getClassLoader().getResource(tableName).toString();
    }
}
