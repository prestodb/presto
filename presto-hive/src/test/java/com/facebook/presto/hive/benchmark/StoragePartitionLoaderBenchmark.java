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
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.TestingHiveConnectorFactory;
import com.facebook.presto.hive.authentication.MetastoreContext;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Files;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.testcontainers.shaded.com.google.common.base.Charsets;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.hive.HiveQueryRunner.METASTORE_CONTEXT;
import static com.facebook.presto.hive.TestHiveUtil.createTestingFileHiveMetastore;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;

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

        List<Column> columns = ImmutableList.of(
                new Column("col1", HiveType.HIVE_STRING, Optional.empty(), Optional.empty()),
                new Column("col2", HiveType.HIVE_LONG, Optional.empty(), Optional.empty()),
                new Column("col3", HiveType.HIVE_INT, Optional.empty(), Optional.empty()));

        File externalDir = new File(tempExternalDir, "external");
        Table hiveSymlinkTable = createHiveSymlinkTable(
                "sym_db",
                "sym_table",
                columns,
                externalDir);

        metastore.createTable(
                METASTORE_CONTEXT,
                hiveSymlinkTable,
                new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of()));

        LocalQueryRunner queryRunner = createLocalQueryRunner();
        queryRunner.createCatalog("hive", new TestingHiveConnectorFactory(metastore), ImmutableMap.of());
        return queryRunner;
    }

    private static Table createHiveSymlinkTable(String databaseName, String tableName, List<Column> columns, File location)
    {
        location.mkdir();
        File symlinkFile = new File(location, "symlink.txt");
        try {
            symlinkFile.createNewFile();
            Files.asCharSink(symlinkFile, Charsets.UTF_8)
                    .write(String.format("file:%s/datafile1\nfile:%s/datafile2\n", location, location));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create symlink file at: " + symlinkFile.getAbsolutePath(), e);
        }

        try {
            new File(location, "datafile1").createNewFile();
            new File(location, "datafile2").createNewFile();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create data files in: " + location, e);
        }

        StorageFormat symlinkStorageFormat = StorageFormat.create(
                ParquetHiveSerDe.class.getName(),
                SymlinkTextInputFormat.class.getName(),
                HiveIgnoreKeyTextOutputFormat.class.getName()
        );

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
                        ImmutableMap.of()
                ),
                columns,
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty()
        );
    }
}
