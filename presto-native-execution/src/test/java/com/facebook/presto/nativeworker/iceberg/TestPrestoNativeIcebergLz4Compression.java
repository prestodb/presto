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
package com.facebook.presto.nativeworker.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/// Verifies that native and Java workers write Parquet's LZ4 and LZ4_RAW codecs, and read what the other wrote.
public class TestPrestoNativeIcebergLz4Compression
        extends AbstractTestQueryFramework
{
    private static final long ROW_COUNT = 200_000;
    // Long enough strings that each page holds several 128KB Hadoop LZ4 blocks when the Java worker writes it.
    private static final String SOURCE_QUERY = "SELECT x AS k, CAST(x AS DOUBLE) / 7 AS d, " +
            "concat('payload-', CAST(x % 1000 AS VARCHAR), '-', CAST(x AS VARCHAR)) AS payload " +
            "FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, " + (ROW_COUNT / 1000 - 1) + ")) a(i) CROSS JOIN UNNEST(sequence(1, 1000)) b(j))";

    private QueryRunner javaQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .setAddStorageFormatToPath(true)
                .build();
    }

    @BeforeClass
    public void createJavaQueryRunner()
            throws Exception
    {
        // The Java runner shares the native runner's data directory and metastore, so each sees the other's tables.
        javaQueryRunner = javaIcebergQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .setAddStorageFormatToPath(true)
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void closeJavaQueryRunner()
    {
        if (javaQueryRunner != null) {
            javaQueryRunner.close();
            javaQueryRunner = null;
        }
    }

    @DataProvider
    public Object[][] codecs()
    {
        return new Object[][] {{"LZ4", CompressionCodecName.LZ4}, {"LZ4_RAW", CompressionCodecName.LZ4_RAW}};
    }

    @Test(dataProvider = "codecs")
    public void testNativeWriteJavaRead(String codec, CompressionCodecName expectedFileCodec)
    {
        String table = "native_written_" + codec.toLowerCase();
        assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        assertUpdate(withCodec(getSession(), codec), format("CREATE TABLE %s AS %s", table, SOURCE_QUERY), ROW_COUNT);

        assertFileCodec(table, expectedFileCodec);
        assertSameRows(computeActual(format("SELECT * FROM %s", table)));
        assertSameRows(javaQueryRunner.execute(format("SELECT * FROM %s", table)));
        assertReadableByParquetMr(table);
    }

    @Test(dataProvider = "codecs")
    public void testJavaWriteNativeRead(String codec, CompressionCodecName expectedFileCodec)
    {
        String table = "java_written_" + codec.toLowerCase();
        javaQueryRunner.execute(format("DROP TABLE IF EXISTS %s", table));
        javaQueryRunner.execute(withCodec(javaQueryRunner.getDefaultSession(), codec), format("CREATE TABLE %s AS %s", table, SOURCE_QUERY));

        assertFileCodec(table, expectedFileCodec);
        assertSameRows(computeActual(format("SELECT * FROM %s", table)));
        assertSameRows(javaQueryRunner.execute(format("SELECT * FROM %s", table)));
        assertReadableByParquetMr(table);
    }

    @Test(dataProvider = "codecs")
    public void testNativeInsert(String codec, CompressionCodecName expectedFileCodec)
    {
        // INSERT takes the codec from the session of the inserting query, not from the table.
        String table = "native_inserted_" + codec.toLowerCase();
        assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        assertUpdate(format("CREATE TABLE %s (k BIGINT, d DOUBLE, payload VARCHAR)", table));
        assertUpdate(withCodec(getSession(), codec), format("INSERT INTO %s %s", table, SOURCE_QUERY), ROW_COUNT);

        assertFileCodec(table, expectedFileCodec);
        assertSameRows(javaQueryRunner.execute(format("SELECT * FROM %s", table)));
    }

    /// parquet-mr decompresses LZ4 with Hadoop's Lz4Codec, which fails on blocks larger than its 256KB buffer.
    private void assertReadableByParquetMr(String table)
    {
        long rows = 0;
        for (String file : getDataFiles(table)) {
            try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file))
                    .withConf(new Configuration())
                    .build()) {
                while (reader.read() != null) {
                    rows++;
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        assertEquals(rows, ROW_COUNT);
    }

    private List<String> getDataFiles(String table)
    {
        return computeActual(format("SELECT file_path FROM \"%s$files\"", table)).getOnlyColumn()
                .map(String.class::cast)
                .collect(toImmutableList());
    }

    private static Session withCodec(Session session, String codec)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(ICEBERG_CATALOG, "compression_codec", codec)
                .build();
    }

    private void assertSameRows(MaterializedResult actual)
    {
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), computeActual(SOURCE_QUERY).getMaterializedRows());
    }

    private void assertFileCodec(String table, CompressionCodecName expectedFileCodec)
    {
        List<String> files = getDataFiles(table);
        assertFalse(files.isEmpty());
        for (String file : files) {
            try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file), new Configuration()))) {
                for (BlockMetaData rowGroup : reader.getRowGroups()) {
                    for (ColumnChunkMetaData column : rowGroup.getColumns()) {
                        assertEquals(column.getCodec(), expectedFileCodec, format("Codec of column %s in %s", column.getPath(), file));
                    }
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
