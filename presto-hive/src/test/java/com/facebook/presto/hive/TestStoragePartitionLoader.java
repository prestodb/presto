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

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.hive.TestBackgroundHiveSplitLoader.TestingHdfsEnvironment;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.units.DataSize.Unit.GIGABYTE;
import static com.facebook.airlift.units.DataSize.Unit.KILOBYTE;
import static com.facebook.presto.hive.HiveSessionProperties.isSkipEmptyFilesEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isUseListDirectoryCache;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveTestUtils.getAllSessionProperties;
import static com.facebook.presto.hive.HiveUtil.buildDirectoryContextProperties;
import static com.facebook.presto.hive.NestedDirectoryPolicy.IGNORED;
import static com.facebook.presto.hive.StoragePartitionLoader.BucketSplitInfo.createBucketSplitInfo;
import static com.facebook.presto.hive.TestBackgroundHiveSplitLoader.SIMPLE_TABLE;
import static com.facebook.presto.hive.TestBackgroundHiveSplitLoader.samplePartitionMetadatas;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;

public class TestStoragePartitionLoader
{
    @Test
    public void testGetSymlinkIterator()
            throws Exception
    {
        CachingDirectoryLister directoryLister = new CachingDirectoryLister(
                new HadoopDirectoryLister(),
                new Duration(5, TimeUnit.MINUTES),
                new DataSize(100, KILOBYTE),
                ImmutableList.of());

        Configuration configuration = new Configuration(false);

        InputFormat<?, ?> inputFormat = HiveUtil.getInputFormat(
                configuration,
                SymlinkTextInputFormat.class.getName(),
                PARQUET.getSerDe(),
                true);

        Path firstFilePath = new Path("hdfs://hadoop:9000/db_name/table_name/file1");
        Path secondFilePath = new Path("hdfs://hadoop:9000/db_name/table_name/file2");
        List<Path> paths = ImmutableList.of(firstFilePath, secondFilePath);
        List<LocatedFileStatus> files = paths.stream()
                .map(path -> locatedFileStatus(path, 0L))
                .collect(toImmutableList());

        ConnectorSession connectorSession = new TestingConnectorSession(getAllSessionProperties(
                new HiveClientConfig().setMaxSplitSize(new DataSize(1.0, GIGABYTE))
                        .setFileStatusCacheTables(""),
                new HiveCommonClientConfig()));

        StoragePartitionLoader storagePartitionLoader = storagePartitionLoader(files, directoryLister, connectorSession);

        HdfsContext hdfsContext = new HdfsContext(
                connectorSession,
                SIMPLE_TABLE.getDatabaseName(),
                SIMPLE_TABLE.getTableName(),
                SIMPLE_TABLE.getStorage().getLocation(),
                false);

        HiveDirectoryContext hiveDirectoryContext = new HiveDirectoryContext(
                IGNORED,
                isUseListDirectoryCache(connectorSession),
                isSkipEmptyFilesEnabled(connectorSession),
                hdfsContext.getIdentity(),
                buildDirectoryContextProperties(connectorSession),
                connectorSession.getRuntimeStats());

        Iterator<InternalHiveSplit> symlinkIterator = storagePartitionLoader.getSymlinkIterator(
                new Path("hdfs://hadoop:9000/db_name/table_name/symlink_manifest"),
                false,
                SIMPLE_TABLE.getStorage(),
                ImmutableList.of(),
                "UNPARTITIONED",
                SIMPLE_TABLE.getDataColumns().size(),
                getOnlyElement(samplePartitionMetadatas()),
                true,
                new Path("hdfs://hadoop:9000/db_name/table_name/"),
                paths,
                inputFormat,
                hiveDirectoryContext);

        List<InternalHiveSplit> splits = ImmutableList.copyOf(symlinkIterator);
        assertEquals(splits.size(), 2);
        assertEquals(splits.get(0).getPath(), firstFilePath.toString());
        assertEquals(splits.get(1).getPath(), secondFilePath.toString());
    }

    private static LocatedFileStatus locatedFileStatus(Path path, long fileSize)
    {
        return new LocatedFileStatus(
                fileSize,
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
                new org.apache.hadoop.fs.BlockLocation[]{new BlockLocation(new String[1], new String[]{"localhost"}, 0, fileSize)});
    }

    private static StoragePartitionLoader storagePartitionLoader(
            List<LocatedFileStatus> files,
            DirectoryLister directoryLister,
            ConnectorSession connectorSession)
    {
        return new StoragePartitionLoader(
                SIMPLE_TABLE,
                ImmutableMap.of(),
                createBucketSplitInfo(Optional.empty(), Optional.empty()),
                connectorSession,
                new TestingHdfsEnvironment(files),
                new NamenodeStats(),
                directoryLister,
                new ConcurrentLinkedDeque<>(),
                false,
                false,
                false);
    }
}
