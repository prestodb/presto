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
package com.facebook.presto.hive.s3select;

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.s3.PrestoS3FileSystem;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.TEXTFILE;
import static com.facebook.presto.hive.HiveType.HIVE_BINARY;
import static com.facebook.presto.hive.HiveType.HIVE_BOOLEAN;
import static com.facebook.presto.hive.HiveUtil.isSelectSplittable;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.hive.s3select.S3SelectPushdown.shouldEnablePushdownForTable;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestS3SelectPushdown
{
    private static final String S3_SELECT_PUSHDOWN_ENABLED = "s3_select_pushdown_enabled";

    private TextInputFormat inputFormat;
    private ConnectorSession session;
    private Table table;
    private Partition partition;
    private Storage storage;
    private Column column;
    private FileSystem fileSystem;

    @BeforeClass
    public void setUp()
    {
        inputFormat = new TextInputFormat();
        inputFormat.configure(new JobConf());

        session = initTestingConnectorSession(true);

        column = new Column("column", HIVE_BOOLEAN, Optional.empty(), Optional.empty());

        storage = Storage.builder()
                .setStorageFormat(fromHiveStorageFormat(TEXTFILE))
                .setLocation("location")
                .build();

        partition = new Partition(
                "db",
                "table",
                emptyList(),
                storage,
                singletonList(column),
                emptyMap(),
                Optional.empty(),
                false,
                false,
                1234,
                4567L);

        table = new Table(
                "db",
                "table",
                "owner",
                EXTERNAL_TABLE,
                storage,
                singletonList(column),
                emptyList(),
                emptyMap(),
                Optional.empty(),
                Optional.empty());

        fileSystem = new PrestoS3FileSystem();
    }

    @Test
    public void testIsCompressionCodecSupported()
    {
        assertTrue(S3SelectPushdown.isCompressionCodecSupported(inputFormat, new Path("s3://fakeBucket/fakeObject.gz")));
        assertTrue(S3SelectPushdown.isCompressionCodecSupported(inputFormat, new Path("s3://fakeBucket/fakeObject")));
        assertFalse(S3SelectPushdown.isCompressionCodecSupported(inputFormat, new Path("s3://fakeBucket/fakeObject.lz4")));
        assertFalse(S3SelectPushdown.isCompressionCodecSupported(inputFormat, new Path("s3://fakeBucket/fakeObject.snappy")));
        assertTrue(S3SelectPushdown.isCompressionCodecSupported(inputFormat, new Path("s3://fakeBucket/fakeObject.bz2")));
    }

    @Test
    public void testShouldEnableSelectPushdown()
    {
        assertTrue(shouldEnablePushdownForTable(session, table, "s3://fakeBucket/fakeObject", Optional.empty()));
        assertTrue(shouldEnablePushdownForTable(session, table, "s3://fakeBucket/fakeObject", Optional.of(partition)));
    }

    @Test
    public void testShouldNotEnableSelectPushdownWhenDisabledOnSession()
    {
        ConnectorSession testSession = initTestingConnectorSession(false);
        assertFalse(shouldEnablePushdownForTable(testSession, table, "", Optional.empty()));
    }

    @Test
    public void testShouldNotEnableSelectPushdownWhenIsNotS3StoragePath()
    {
        assertFalse(shouldEnablePushdownForTable(session, table, null, Optional.empty()));
        assertFalse(shouldEnablePushdownForTable(session, table, "", Optional.empty()));
        assertFalse(shouldEnablePushdownForTable(session, table, "s3:/invalid", Optional.empty()));
        assertFalse(shouldEnablePushdownForTable(session, table, "s3:/invalid", Optional.of(partition)));
    }

    @Test
    public void testShouldNotEnableSelectPushdownWhenIsNotSupportedSerde()
    {
        Storage newStorage = Storage.builder()
                .setStorageFormat(fromHiveStorageFormat(ORC))
                .setLocation("location")
                .build();
        Table newTable = new Table(
                "db",
                "table",
                "owner",
                EXTERNAL_TABLE,
                newStorage,
                singletonList(column),
                emptyList(),
                emptyMap(),
                Optional.empty(),
                Optional.empty());

        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.empty()));

        Partition newPartition = new Partition(
                "db",
                "table",
                emptyList(),
                newStorage,
                singletonList(column),
                emptyMap(),
                Optional.empty(),
                false,
                false,
                1234,
                4567L);
        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.of(newPartition)));
    }

    @Test
    public void testShouldNotEnableSelectPushdownWhenInputFormatIsNotSupported()
    {
        Storage newStorage = Storage.builder()
                .setStorageFormat(StorageFormat.create(LazySimpleSerDe.class.getName(), "inputFormat", "outputFormat"))
                .setLocation("location")
                .build();
        Table newTable = new Table(
                "db",
                "table",
                "owner",
                EXTERNAL_TABLE,
                newStorage,
                singletonList(column),
                emptyList(),
                emptyMap(),
                Optional.empty(),
                Optional.empty());
        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.empty()));
    }

    @Test
    public void testShouldNotEnableSelectPushdownWhenColumnTypesAreNotSupported()
    {
        Column newColumn = new Column("column", HIVE_BINARY, Optional.empty(), Optional.empty());
        Table newTable = new Table(
                "db",
                "table",
                "owner",
                EXTERNAL_TABLE,
                storage,
                singletonList(newColumn),
                emptyList(),
                emptyMap(),
                Optional.empty(),
                Optional.empty());
        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.empty()));

        Partition newPartition = new Partition(
                "db",
                "table",
                emptyList(),
                storage,
                singletonList(column),
                emptyMap(),
                Optional.empty(),
                false,
                false,
                1234,
                4567L);
        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.of(newPartition)));
    }

    @Test
    public void testShouldEnableSplits()
    {
        // Uncompressed CSV
        assertTrue(isSelectSplittable(inputFormat, new Path("s3://fakeBucket/fakeObject.csv"), true));
        // Pushdown disabled
        assertTrue(isSelectSplittable(inputFormat, new Path("s3://fakeBucket/fakeObject.csv"), false));
        assertTrue(isSelectSplittable(inputFormat, new Path("s3://fakeBucket/fakeObject.json"), false));
        assertTrue(isSelectSplittable(inputFormat, new Path("s3://fakeBucket/fakeObject.gz"), false));
        assertTrue(isSelectSplittable(inputFormat, new Path("s3://fakeBucket/fakeObject.bz2"), false));
    }

    @Test
    public void testShouldNotEnableSplits()
    {
        // Compressed files
        assertFalse(isSelectSplittable(inputFormat, new Path("s3://fakeBucket/fakeObject.gz"), true));
        assertFalse(isSelectSplittable(inputFormat, new Path("s3://fakeBucket/fakeObject.bz2"), true));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        inputFormat = null;
        session = null;
        table = null;
        partition = null;
        storage = null;
        column = null;
    }

    private TestingConnectorSession initTestingConnectorSession(boolean enableSelectPushdown)
    {
        return new TestingConnectorSession(singletonList(booleanProperty(
                S3_SELECT_PUSHDOWN_ENABLED,
                "S3 Select pushdown enabled",
                true,
                false)),
                ImmutableMap.of(S3_SELECT_PUSHDOWN_ENABLED, enableSelectPushdown));
    }
}
