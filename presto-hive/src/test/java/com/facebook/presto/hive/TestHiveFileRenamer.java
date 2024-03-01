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

import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.TestHiveMetadataUpdateHandle.TEST_HIVE_METADATA_UPDATE_REQUEST;
import static com.facebook.presto.hive.TestHiveMetadataUpdateHandle.TEST_PARTITION_NAME;
import static com.facebook.presto.hive.TestHiveMetadataUpdateHandle.TEST_REQUEST_ID;
import static com.facebook.presto.hive.TestHiveMetadataUpdateHandle.TEST_SCHEMA_NAME;
import static com.facebook.presto.hive.TestHiveMetadataUpdateHandle.TEST_SCHEMA_TABLE_NAME;
import static com.facebook.presto.hive.TestHiveMetadataUpdateHandle.TEST_TABLE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestHiveFileRenamer
{
    private static final QueryId TEST_QUERY_ID = new QueryId("test");
    private static final int REQUEST_COUNT = 10;
    private static final int PARTITION_COUNT = 10;
    private static final int TABLE_COUNT = 10;
    private static final int THREAD_COUNT = 100;
    private static final int THREAD_POOL_SIZE = 10;

    @Test
    public void testHiveFileRenamer()
    {
        HiveFileRenamer hiveFileRenamer = new HiveFileRenamer();
        List<ConnectorMetadataUpdateHandle> requests = ImmutableList.of(TEST_HIVE_METADATA_UPDATE_REQUEST);
        List<ConnectorMetadataUpdateHandle> results = hiveFileRenamer.getMetadataUpdateResults(requests, TEST_QUERY_ID);

        // Assert # of requests is equal to # of results
        assertEquals(requests.size(), results.size());

        HiveMetadataUpdateHandle result = (HiveMetadataUpdateHandle) results.get(0);

        assertEquals(result.getRequestId(), TEST_REQUEST_ID);
        assertEquals(result.getSchemaTableName(), TEST_SCHEMA_TABLE_NAME);
        assertEquals(result.getPartitionName(), Optional.of(TEST_PARTITION_NAME));

        // Assert file name returned is "1"
        assertEquals(result.getMetadataUpdate(), Optional.of("0"));
    }

    @Test
    public void testFileNamesForSinglePartition()
    {
        HiveFileRenamer hiveFileRenamer = new HiveFileRenamer();
        List<ConnectorMetadataUpdateHandle> requests = createHiveMetadataUpdateRequests(TEST_SCHEMA_NAME, TEST_TABLE_NAME, TEST_PARTITION_NAME);
        List<String> fileNames = getFileNames(hiveFileRenamer, requests);
        List<String> aggregatedFileNames = new ArrayList<>(fileNames);

        assertTrue(areFileNamesIncreasingSequentially(fileNames));

        // Send more requests
        requests = createHiveMetadataUpdateRequests(TEST_SCHEMA_NAME, TEST_TABLE_NAME, TEST_PARTITION_NAME);
        fileNames = getFileNames(hiveFileRenamer, requests);
        aggregatedFileNames.addAll(fileNames);

        // Assert that the # of filenames is equal to # of requests
        assertEquals(fileNames.size(), REQUEST_COUNT);

        // Assert that the file names are continuous increasing numbers
        assertTrue(areFileNamesIncreasingSequentially(aggregatedFileNames));
    }

    @Test
    public void testFileNamesForMultiplePartitions()
    {
        HiveFileRenamer hiveFileRenamer = new HiveFileRenamer();
        for (int partitionNumber = 1; partitionNumber <= PARTITION_COUNT; partitionNumber++) {
            List<ConnectorMetadataUpdateHandle> requests = createHiveMetadataUpdateRequests(TEST_SCHEMA_NAME, TEST_TABLE_NAME, "partition_" + partitionNumber);
            List<String> fileNames = getFileNames(hiveFileRenamer, requests);

            // Assert that the # of filenames is equal to # of requests
            assertEquals(fileNames.size(), REQUEST_COUNT);

            assertTrue(areFileNamesIncreasingSequentially(fileNames));
        }
    }

    @Test
    public void testFileNamesForMultipleTables()
    {
        HiveFileRenamer hiveFileRenamer = new HiveFileRenamer();
        for (int tableNumber = 1; tableNumber <= TABLE_COUNT; tableNumber++) {
            List<ConnectorMetadataUpdateHandle> requests = createHiveMetadataUpdateRequests(TEST_SCHEMA_NAME, "table_" + tableNumber, TEST_PARTITION_NAME);
            List<String> fileNames = getFileNames(hiveFileRenamer, requests);

            // Assert that the # of filenames is equal to # of requests
            assertEquals(fileNames.size(), REQUEST_COUNT);

            assertTrue(areFileNamesIncreasingSequentially(fileNames));
        }
    }

    @Test
    public void testFileNameResultCache()
    {
        HiveFileRenamer hiveFileRenamer = new HiveFileRenamer();
        List<ConnectorMetadataUpdateHandle> requests = createHiveMetadataUpdateRequests(TEST_SCHEMA_NAME, TEST_TABLE_NAME, TEST_PARTITION_NAME);

        // Get the file names 1st time
        List<String> fileNames = getFileNames(hiveFileRenamer, requests);

        // Get the file names 2nd time, for the same set of requests. This should be served from cache.
        List<String> fileNamesList = getFileNames(hiveFileRenamer, requests);

        // Assert that the file name is same for a given request. This is to imitate retries from workers.
        assertEquals(fileNames, fileNamesList);
        assertEquals(fileNames, getFileNames(hiveFileRenamer, requests));
    }

    @Test
    public void testMultiThreadedRequests()
            throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        List<String> fileNames = new CopyOnWriteArrayList<>();
        HiveFileRenamer hiveFileRenamer = new HiveFileRenamer();

        // Spawn THREAD_COUNT threads. And each thread will send REQUEST_COUNT requests to HiveFileRenamer
        for (int i = 0; i < THREAD_COUNT; i++) {
            service.execute(() -> {
                List<ConnectorMetadataUpdateHandle> requests = createHiveMetadataUpdateRequests(TEST_SCHEMA_NAME, TEST_TABLE_NAME, TEST_PARTITION_NAME);
                fileNames.addAll(getFileNames(hiveFileRenamer, requests));
                latch.countDown();
            });
        }

        // wait for all threads to finish
        latch.await();

        // Assert the # of filenames
        assertEquals(fileNames.size(), THREAD_COUNT * REQUEST_COUNT);

        // Assert that the filenames are an increasing sequence
        assertTrue(areFileNamesIncreasingSequentially(fileNames));
    }

    @Test
    public void testCleanup()
    {
        HiveFileRenamer hiveFileRenamer = new HiveFileRenamer();
        List<ConnectorMetadataUpdateHandle> requests = ImmutableList.of(TEST_HIVE_METADATA_UPDATE_REQUEST);
        List<ConnectorMetadataUpdateHandle> results = hiveFileRenamer.getMetadataUpdateResults(requests, TEST_QUERY_ID);
        assertEquals(results.size(), 1);
        HiveMetadataUpdateHandle result = (HiveMetadataUpdateHandle) results.get(0);
        assertEquals(result.getMetadataUpdate(), Optional.of("0"));

        hiveFileRenamer.cleanup(TEST_QUERY_ID);

        requests = ImmutableList.of(TEST_HIVE_METADATA_UPDATE_REQUEST);
        results = hiveFileRenamer.getMetadataUpdateResults(requests, TEST_QUERY_ID);
        assertEquals(results.size(), 1);
        result = (HiveMetadataUpdateHandle) results.get(0);
        assertEquals(result.getMetadataUpdate(), Optional.of("0"));
    }

    private List<String> getFileNames(HiveFileRenamer hiveFileRenamer, List<ConnectorMetadataUpdateHandle> requests)
    {
        List<ConnectorMetadataUpdateHandle> results = hiveFileRenamer.getMetadataUpdateResults(requests, TEST_QUERY_ID);
        return results.stream()
                .map(result -> {
                    Optional<String> fileName = ((HiveMetadataUpdateHandle) result).getMetadataUpdate();
                    assertTrue(fileName.isPresent());
                    return fileName.get();
                })
                .collect(Collectors.toList());
    }

    private List<ConnectorMetadataUpdateHandle> createHiveMetadataUpdateRequests(String schemaName, String tableName, String partitionName)
    {
        List<ConnectorMetadataUpdateHandle> requests = new ArrayList<>();
        for (int i = 1; i <= REQUEST_COUNT; i++) {
            requests.add(new HiveMetadataUpdateHandle(UUID.randomUUID(), new SchemaTableName(schemaName, tableName), Optional.of(partitionName), Optional.empty()));
        }
        return requests;
    }

    private boolean areFileNamesIncreasingSequentially(List<String> fileNames)
    {
        // Sort the filenames
        fileNames.sort(Comparator.comparingInt(Integer::valueOf));

        long start = 0;

        for (String fileName : fileNames) {
            if (!fileName.equals(String.valueOf(start))) {
                return false;
            }
            start++;
        }
        return true;
    }
}
