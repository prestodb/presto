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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SplitSource;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

@Test(groups = "hive-s3")
public abstract class AbstractTestHiveClientS3
{
    protected String database;
    protected SchemaTableName tableS3;

    protected HiveClient client;

    protected void setupHive(String databaseName)
    {
        database = databaseName;
        tableS3 = new SchemaTableName(database, "presto_test_s3");
    }

    protected void setup(String host, int port, String databaseName, String awsAccessKey, String awsSecretKey)
    {
        setupHive(databaseName);

        HiveClientConfig hiveClientConfig = new HiveClientConfig()
                .setS3AwsAccessKey(awsAccessKey)
                .setS3AwsSecretKey(awsSecretKey);

        String proxy = System.getProperty("hive.metastore.thrift.client.socks-proxy");
        if (proxy != null) {
            hiveClientConfig.setMetastoreSocksProxy(HostAndPort.fromString(proxy));
        }

        HiveCluster hiveCluster = new TestingHiveCluster(hiveClientConfig, host, port);
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("hive-s3-%s"));

        client = new HiveClient(
                new HiveConnectorId("hive-test"),
                hiveClientConfig,
                new CachingHiveMetastore(hiveCluster, executor, hiveClientConfig),
                new HdfsEnvironment(new HdfsConfiguration(hiveClientConfig)),
                sameThreadExecutor());
    }

    @Test
    public void testGetRecordsS3()
            throws Exception
    {
        TableHandle table = getTableHandle(tableS3);
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(client.getColumnHandles(table).values());
        Map<String, Integer> columnIndex = indexColumns(columnHandles);

        PartitionResult partitionResult = client.getPartitions(table, TupleDomain.all());
        assertEquals(partitionResult.getPartitions().size(), 1);
        SplitSource splitSource = client.getPartitionSplits(table, partitionResult.getPartitions());

        long sum = 0;
        for (Split split : getAllSplits(splitSource)) {
            try (RecordCursor cursor = client.getRecordSet(split, columnHandles).cursor()) {
                while (cursor.advanceNextPosition()) {
                    sum += cursor.getLong(columnIndex.get("t_bigint"));
                }
            }
        }
        assertEquals(sum, 78300);
    }

    private TableHandle getTableHandle(SchemaTableName tableName)
    {
        TableHandle handle = client.getTableHandle(tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static List<Split> getAllSplits(SplitSource source)
            throws InterruptedException
    {
        ImmutableList.Builder<Split> splits = ImmutableList.builder();
        while (!source.isFinished()) {
            splits.addAll(source.getNextBatch(1000));
        }
        return splits.build();
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            checkArgument(columnHandle instanceof HiveColumnHandle, "columnHandle is not an instance of HiveColumnHandle");
            HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) columnHandle;
            index.put(hiveColumnHandle.getName(), i);
            i++;
        }
        return index.build();
    }
}
