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
package com.facebook.presto.hive.benchmark;

import com.facebook.presto.benchmark.AbstractSqlBenchmark;
import com.facebook.presto.benchmark.SimpleLineBenchmarkResultWriter;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveConnectorFactory;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.parquet.writer.ParquetSchemaConverter;
import com.facebook.presto.parquet.writer.ParquetWriter;
import com.facebook.presto.parquet.writer.ParquetWriterOptions;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Files;
import io.airlift.units.DataSize;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.TestHiveUtil.createTestingFileHiveMetastore;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.fail;

public class StoragePartitionLoaderBenchmark
        extends AbstractSqlBenchmark
{
    public StoragePartitionLoaderBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(
                localQueryRunner,
                "storage_partition_loader_benchmark",
                10,
                100,
                "SELECT * FROM hive.sym_db.sym_table");
    }

    public static void main(String[] args)
            throws Exception
    {
        new StoragePartitionLoaderBenchmark(
                createLocalQueryRunnerWithSymlink(Files.createTempDir(), Files.createTempDir())).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }

    public static LocalQueryRunner createLocalQueryRunnerWithSymlink(File tempHiveDir, File tempExternalDir)
    {
        File hiveDir = new File(tempHiveDir, "hive_data");
        ExtendedHiveMetastore metastore = createTestingFileHiveMetastore(hiveDir);

        File externalDir = new File(tempExternalDir, "external");
        List<Column> columns = ImmutableList.of(new Column("col3", HiveType.HIVE_INT, Optional.empty(), Optional.empty()));
        Table hiveSymlinkTable = createHiveSymlinkTable(
                "sym_db",
                "sym_table",
                columns,
                externalDir);

        metastore.createDatabase(
                METASTORE_CONTEXT,
                new Database(
                        "sym_db",
                        Optional.empty(),
                        "hive",
                        PrincipalType.USER,
                        Optional.empty(),
                        ImmutableMap.of()));

        metastore.createTable(
                METASTORE_CONTEXT,
                hiveSymlinkTable,
                new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of()),
                ImmutableList.of());

        LocalQueryRunner queryRunner = createLocalQueryRunner();
        HiveConnectorFactory connectorFactory = new HiveConnectorFactory(
                "hive",
                HiveConnectorFactory.class.getClassLoader(),
                Optional.of(metastore));

        queryRunner.createCatalog("hive", connectorFactory, ImmutableMap.of());

        System.out.println("Hive metastore tables: " + metastore.getAllTables(METASTORE_CONTEXT, "sym_db"));
        System.out.println("Hive metastore databases: " + metastore.getAllDatabases(METASTORE_CONTEXT));

        MaterializedResult catalogsResult = queryRunner.execute("SHOW CATALOGS");
        System.out.println("All catalogs: " + catalogsResult.toString());

        return queryRunner;
    }

    private static Table createHiveSymlinkTable(String databaseName, String tableName, List<Column> columns, File location)
    {
        location.mkdir();
        File symlinkFile = new File(location, "symlink");

        File dataDir = new File(location, "data");
        dataDir.mkdir();
        try {
            symlinkFile.createNewFile();
            Files.asCharSink(symlinkFile, StandardCharsets.UTF_8)
                    .write(String.format("file://%s/datafile1.parquet\nfile://%s/datafile2.parquet\n", dataDir.getAbsolutePath(), dataDir.getAbsolutePath()));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create symlink file at: " + symlinkFile.getAbsolutePath(), e);
        }

        try {
            createBasicParquetFiles(dataDir, ImmutableList.of("datafile1.parquet", "datafile2.parquet"));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create data files in: " + location, e);
        }

        StorageFormat symlinkStorageFormat = StorageFormat.create(
                ParquetHiveSerDe.class.getName(),
                SymlinkTextInputFormat.class.getName(),
                HiveIgnoreKeyTextOutputFormat.class.getName());

        return new Table(
                databaseName,
                tableName,
                "hive",
                EXTERNAL_TABLE,
                new Storage(
                        symlinkStorageFormat,
                        "file:" + location.getAbsolutePath(),
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                columns,
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty());
    }

    public static void createBasicParquetFiles(File location, List<String> fileNames)
    {
        for (String fileName : fileNames) {
            File df = new File(location, fileName);
            List<Type> types = ImmutableList.of(BIGINT, INTEGER);
            List<String> names = ImmutableList.of("col_1", "col_2");
            ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                    .setMaxPageSize(DataSize.succinctBytes(1000))
                    .setMaxBlockSize(DataSize.succinctBytes(15000))
                    .setMaxDictionaryPageSize(DataSize.succinctBytes(1000))
                    .build();
            try (ParquetWriter parquetWriter = createParquetWriter(df, types, names, parquetWriterOptions, CompressionCodecName.UNCOMPRESSED)) {
                Random rand = new Random();
                for (int pageIdx = 0; pageIdx < 5; pageIdx++) {
                    int pageRowCount = 10;
                    PageBuilder pageBuilder = new PageBuilder(pageRowCount, types);
                    for (int rowIdx = 0; rowIdx < pageRowCount; rowIdx++) {
                        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), pageIdx * 100 + rand.nextInt(50));
                        INTEGER.writeLong(pageBuilder.getBlockBuilder(1), rand.nextInt(100000000));
                        pageBuilder.declarePosition();
                    }
                    parquetWriter.write(pageBuilder.build());
                }
            }
            catch (Exception e) {
                fail("Should not fail, but throw an exception as follows:", e);
            }
        }
    }

    public static ParquetWriter createParquetWriter(File outputFile, List<Type> types, List<String> columnNames,
                                                    ParquetWriterOptions parquetWriterOptions, CompressionCodecName compressionCodecName)
            throws IOException
    {
        checkArgument(types.size() == columnNames.size());
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                types,
                columnNames);
        return new ParquetWriter(
                java.nio.file.Files.newOutputStream(outputFile.toPath()),
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                columnNames,
                types,
                parquetWriterOptions,
                compressionCodecName.getHadoopCompressionCodecClassName());
    }
}
