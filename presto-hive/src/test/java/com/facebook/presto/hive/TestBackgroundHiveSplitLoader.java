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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.hive.HiveBucketing.HiveBucketFilter;
import com.facebook.presto.hive.HiveColumnHandle.ColumnType;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo;
import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.CacheQuotaScope.GLOBAL;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveUtil.getRegularColumnHandles;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestBackgroundHiveSplitLoader
{
    private static final int BUCKET_COUNT = 2;

    private static final String SAMPLE_PATH = "hdfs://VOL1:9000/db_name/table_name/000000_0";
    private static final String SAMPLE_PATH_FILTERED = "hdfs://VOL1:9000/db_name/table_name/000000_1";

    private static final Path RETURNED_PATH = new Path(SAMPLE_PATH);
    private static final Path FILTERED_PATH = new Path(SAMPLE_PATH_FILTERED);

    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-%s"));

    private static final Domain RETURNED_PATH_DOMAIN = Domain.singleValue(VARCHAR, utf8Slice(RETURNED_PATH.toString()));

    private static final List<LocatedFileStatus> TEST_FILES = ImmutableList.of(
            locatedFileStatus(RETURNED_PATH),
            locatedFileStatus(FILTERED_PATH));

    private static final List<Column> PARTITION_COLUMNS = ImmutableList.of(
            new Column("partitionColumn", HIVE_INT, Optional.empty()));
    private static final List<HiveColumnHandle> BUCKET_COLUMN_HANDLES = ImmutableList.of(
            new HiveColumnHandle("col1", HIVE_INT, INTEGER.getTypeSignature(), 0, ColumnType.REGULAR, Optional.empty()));

    private static final Optional<HiveBucketProperty> BUCKET_PROPERTY = Optional.of(
            new HiveBucketProperty(ImmutableList.of("col1"), BUCKET_COUNT, ImmutableList.of(), HIVE_COMPATIBLE, Optional.empty()));

    private static final Table SIMPLE_TABLE = table(ImmutableList.of(), Optional.empty());
    private static final Table PARTITIONED_TABLE = table(PARTITION_COLUMNS, BUCKET_PROPERTY);

    @Test
    public void testNoPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertEquals(drain(hiveSplitSource).size(), 2);
    }

    @Test
    public void testPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                Optional.of(RETURNED_PATH_DOMAIN));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testPathFilterOneBucketMatchPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                Optional.of(RETURNED_PATH_DOMAIN),
                Optional.of(new HiveBucketFilter(ImmutableSet.of(0, 1))),
                PARTITIONED_TABLE,
                Optional.of(new HiveBucketHandle(BUCKET_COLUMN_HANDLES, BUCKET_COUNT, BUCKET_COUNT)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testPathFilterBucketedPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                Optional.of(RETURNED_PATH_DOMAIN),
                Optional.empty(),
                PARTITIONED_TABLE,
                Optional.of(
                        new HiveBucketHandle(
                                getRegularColumnHandles(PARTITIONED_TABLE),
                                BUCKET_COUNT,
                                BUCKET_COUNT)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testEmptyFileWithNoBlocks()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                ImmutableList.of(locatedFileStatusWithNoBlocks(RETURNED_PATH)),
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        List<HiveSplit> splits = drainSplits(hiveSplitSource);
        assertEquals(splits.size(), 1);
        assertEquals(splits.get(0).getPath(), RETURNED_PATH.toString());
        assertEquals(splits.get(0).getLength(), 0);
    }

    @Test
    public void testNoHangIfPartitionIsOffline()
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoaderOfflinePartitions();
        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThrows(RuntimeException.class, () -> drain(hiveSplitSource));
        assertThrows(RuntimeException.class, hiveSplitSource::isFinished);
    }

    @Test
    public void testCachingDirectoryLister()
            throws Exception
    {
        testCachingDirectoryLister(
                new CachingDirectoryLister(
                        new HadoopDirectoryLister(),
                        new Duration(5, TimeUnit.MINUTES),
                        1000,
                        ImmutableList.of("test_dbname.test_table")),
                "test_dbname.test_table");
        testCachingDirectoryLister(
                new CachingDirectoryLister(
                        new HadoopDirectoryLister(),
                        new Duration(5, TimeUnit.MINUTES),
                        1000,
                        ImmutableList.of("*")),
                "*");
        testCachingDirectoryLister(
                new CachingDirectoryLister(
                        new HadoopDirectoryLister(),
                        new Duration(5, TimeUnit.MINUTES),
                        1000,
                        ImmutableList.of("*")),
                "");
        assertThrows(
                IllegalArgumentException.class,
                () -> testCachingDirectoryLister(
                        new CachingDirectoryLister(
                                new HadoopDirectoryLister(),
                                new Duration(5, TimeUnit.MINUTES),
                                1000,
                                ImmutableList.of("*", "test_dbname.test_table")),
                        "*,test_dbname.test_table"));
    }

    private void testCachingDirectoryLister(CachingDirectoryLister cachingDirectoryLister, String fileStatusCacheTables)
            throws Exception
    {
        assertEquals(cachingDirectoryLister.getRequestCount(), 0);

        int totalCount = 50;
        CountDownLatch firstVisit = new CountDownLatch(1);
        List<Future<List<HiveSplit>>> futures = new ArrayList<>();

        futures.add(EXECUTOR.submit(() -> {
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                    TEST_FILES,
                    cachingDirectoryLister,
                    fileStatusCacheTables);
            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            try {
                return drainSplits(hiveSplitSource);
            }
            finally {
                firstVisit.countDown();
            }
        }));

        for (int i = 0; i < totalCount - 1; i++) {
            futures.add(EXECUTOR.submit(() -> {
                firstVisit.await();
                BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                        TEST_FILES,
                        cachingDirectoryLister,
                        fileStatusCacheTables);
                HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
                backgroundHiveSplitLoader.start(hiveSplitSource);
                return drainSplits(hiveSplitSource);
            }));
        }

        for (Future<List<HiveSplit>> future : futures) {
            assertEquals(future.get().size(), TEST_FILES.size());
        }

        assertEquals(cachingDirectoryLister.getRequestCount(), totalCount);
        if (fileStatusCacheTables.length() == 0) {
            assertEquals(cachingDirectoryLister.getHitCount(), 0);
            assertEquals(cachingDirectoryLister.getMissCount(), totalCount);
        }
        else {
            assertEquals(cachingDirectoryLister.getHitCount(), totalCount - 1);
            assertEquals(cachingDirectoryLister.getMissCount(), 1);
        }
    }

    private static List<String> drain(HiveSplitSource source)
            throws Exception
    {
        return drainSplits(source).stream()
                .map(HiveSplit::getPath)
                .collect(toImmutableList());
    }

    private static List<HiveSplit> drainSplits(HiveSplitSource source)
            throws Exception
    {
        ImmutableList.Builder<HiveSplit> splits = ImmutableList.builder();
        while (!source.isFinished()) {
            source.getNextBatch(NOT_PARTITIONED, 100).get()
                    .getSplits().stream()
                    .map(HiveSplit.class::cast)
                    .forEach(splits::add);
        }
        return splits.build();
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            Optional<Domain> pathDomain)
    {
        return backgroundHiveSplitLoader(
                files,
                pathDomain,
                Optional.empty(),
                SIMPLE_TABLE,
                Optional.empty());
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            Optional<Domain> pathDomain,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas =
                ImmutableList.of(
                        new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of(),
                                Optional.empty()));

        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE)), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

        return new BackgroundHiveSplitLoader(
                table,
                hivePartitionMetadatas,
                pathDomain,
                createBucketSplitInfo(bucketHandle, hiveBucketFilter),
                connectorSession,
                new TestingHdfsEnvironment(files),
                new NamenodeStats(),
                new CachingDirectoryLister(new HadoopDirectoryLister(), new HiveClientConfig()),
                EXECUTOR,
                2,
                false,
                false);
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoader(List<LocatedFileStatus> files, DirectoryLister directoryLister, String fileStatusCacheTables)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas = ImmutableList.of(
                new HivePartitionMetadata(
                        new HivePartition(new SchemaTableName("testSchema", "table_name")),
                        Optional.empty(),
                        ImmutableMap.of(),
                        Optional.empty()));

        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig()
                                .setMaxSplitSize(new DataSize(1.0, GIGABYTE))
                                .setFileStatusCacheTables(fileStatusCacheTables),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig()).getSessionProperties());

        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                hivePartitionMetadatas,
                Optional.empty(),
                createBucketSplitInfo(Optional.empty(), Optional.empty()),
                connectorSession,
                new TestingHdfsEnvironment(files),
                new NamenodeStats(),
                directoryLister,
                EXECUTOR,
                2,
                false,
                false);
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoaderOfflinePartitions()
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(new HiveClientConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE)), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());

        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                createPartitionMetadataWithOfflinePartitions(),
                Optional.empty(),
                createBucketSplitInfo(Optional.empty(), Optional.empty()),
                connectorSession,
                new TestingHdfsEnvironment(TEST_FILES),
                new NamenodeStats(),
                new CachingDirectoryLister(new HadoopDirectoryLister(), new HiveClientConfig()),
                directExecutor(),
                2,
                false,
                false);
    }

    private static Iterable<HivePartitionMetadata> createPartitionMetadataWithOfflinePartitions()
            throws RuntimeException
    {
        return () -> new AbstractIterator<HivePartitionMetadata>()
        {
            // This iterator is crafted to return a valid partition for the first calls to
            // hasNext() and next(), and then it should throw for the second call to hasNext()
            private int position = -1;

            @Override
            protected HivePartitionMetadata computeNext()
            {
                position++;
                switch (position) {
                    case 0:
                        return new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of(),
                                Optional.empty());
                    case 1:
                        throw new RuntimeException("OFFLINE");
                    default:
                        return endOfData();
                }
            }
        };
    }

    private static HiveSplitSource hiveSplitSource(BackgroundHiveSplitLoader backgroundHiveSplitLoader)
    {
        return HiveSplitSource.allAtOnce(
                SESSION,
                SIMPLE_TABLE.getDatabaseName(),
                SIMPLE_TABLE.getTableName(),
                GLOBAL,
                Optional.empty(),
                1,
                1,
                new DataSize(32, MEGABYTE),
                backgroundHiveSplitLoader,
                EXECUTOR,
                new CounterStat());
    }

    private static Table table(
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty)
    {
        Table.Builder tableBuilder = Table.builder();
        tableBuilder.getStorageBuilder()
                .setStorageFormat(
                        StorageFormat.create(
                                "com.facebook.hive.orc.OrcSerde",
                                "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                                "org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
                .setLocation("hdfs://VOL1:9000/db_name/table_name")
                .setSkewed(false)
                .setBucketProperty(bucketProperty);

        return tableBuilder
                .setDatabaseName("test_dbname")
                .setOwner("testOwner")
                .setTableName("test_table")
                .setTableType(MANAGED_TABLE)
                .setDataColumns(ImmutableList.of(new Column("col1", HIVE_STRING, Optional.empty())))
                .setParameters(ImmutableMap.of())
                .setPartitionColumns(partitionColumns)
                .build();
    }

    private static LocatedFileStatus locatedFileStatus(Path path)
    {
        return new LocatedFileStatus(
                0L,
                false,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {new BlockLocation()});
    }

    private static LocatedFileStatus locatedFileStatusWithNoBlocks(Path path)
    {
        return new LocatedFileStatus(
                0L,
                false,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {});
    }

    private static class TestingHdfsEnvironment
            extends HdfsEnvironment
    {
        private final List<LocatedFileStatus> files;

        public TestingHdfsEnvironment(List<LocatedFileStatus> files)
        {
            super(
                    new HiveHdfsConfiguration(new HdfsConfigurationInitializer(new HiveClientConfig(), new MetastoreClientConfig()), ImmutableSet.of()),
                    new MetastoreClientConfig(),
                    new NoHdfsAuthentication());
            this.files = ImmutableList.copyOf(files);
        }

        @Override
        public ExtendedFileSystem getFileSystem(String user, Path path, Configuration configuration)
        {
            return new TestingHdfsFileSystem(files);
        }
    }

    private static class TestingHdfsFileSystem
            extends ExtendedFileSystem
    {
        private final List<LocatedFileStatus> files;

        public TestingHdfsFileSystem(List<LocatedFileStatus> files)
        {
            this.files = ImmutableList.copyOf(files);
        }

        @Override
        public boolean delete(Path f, boolean recursive)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(Path src, Path dst)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWorkingDirectory(Path dir)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
        {
            return new RemoteIterator<LocatedFileStatus>()
            {
                private final Iterator<LocatedFileStatus> iterator = files.iterator();

                @Override
                public boolean hasNext()
                        throws IOException
                {
                    return iterator.hasNext();
                }

                @Override
                public LocatedFileStatus next()
                        throws IOException
                {
                    return iterator.next();
                }
            };
        }

        @Override
        public FSDataOutputStream create(
                Path f,
                FsPermission permission,
                boolean overwrite,
                int bufferSize,
                short replication,
                long blockSize,
                Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mkdirs(Path f, FsPermission permission)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Path getWorkingDirectory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI getUri()
        {
            throw new UnsupportedOperationException();
        }
    }
}
