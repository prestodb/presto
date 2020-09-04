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
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.facebook.presto.hive.TestHiveMetadataUpdateHandle.TEST_PARTITION_NAME;
import static com.facebook.presto.hive.TestHiveMetadataUpdateHandle.TEST_SCHEMA_NAME;
import static com.facebook.presto.hive.TestHiveMetadataUpdateHandle.TEST_SCHEMA_TABLE_NAME;
import static com.facebook.presto.hive.TestHiveMetadataUpdateHandle.TEST_TABLE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestHiveMetadataUpdater
{
    public static final String TEST_FILE_NAME = "fileName";

    private static final int TEST_WRITER_INDEX = 0;
    private static final Executor EXECUTOR = Executors.newFixedThreadPool(5);

    @Test
    public void testEmptyMetadataUpdateRequestQueue()
    {
        HiveMetadataUpdater hiveMetadataUpdater = new HiveMetadataUpdater(EXECUTOR);
        assertEquals(hiveMetadataUpdater.getPendingMetadataUpdateRequests().size(), 0);
    }

    @Test
    public void testAddMetadataUpdateRequest()
    {
        HiveMetadataUpdater hiveMetadataUpdater = new HiveMetadataUpdater(EXECUTOR);

        // add Request
        hiveMetadataUpdater.addMetadataUpdateRequest(TEST_SCHEMA_NAME, TEST_TABLE_NAME, Optional.of(TEST_PARTITION_NAME), TEST_WRITER_INDEX);
        List<ConnectorMetadataUpdateHandle> hiveMetadataUpdateRequests = hiveMetadataUpdater.getPendingMetadataUpdateRequests();

        // assert the pending request queue size
        assertEquals(hiveMetadataUpdateRequests.size(), 1);

        // assert that request in queue is same as the request added
        HiveMetadataUpdateHandle request = (HiveMetadataUpdateHandle) hiveMetadataUpdateRequests.get(0);
        assertEquals(request.getSchemaTableName(), TEST_SCHEMA_TABLE_NAME);
        assertEquals(request.getPartitionName(), Optional.of(TEST_PARTITION_NAME));
    }

    @Test
    public void testSetMetadataUpdateResults()
    {
        HiveMetadataUpdater hiveMetadataUpdater = new HiveMetadataUpdater(EXECUTOR);

        // add Request
        hiveMetadataUpdater.addMetadataUpdateRequest(TEST_SCHEMA_NAME, TEST_TABLE_NAME, Optional.of(TEST_PARTITION_NAME), TEST_WRITER_INDEX);
        List<ConnectorMetadataUpdateHandle> hiveMetadataUpdateRequests = hiveMetadataUpdater.getPendingMetadataUpdateRequests();
        assertEquals(hiveMetadataUpdateRequests.size(), 1);
        HiveMetadataUpdateHandle request = (HiveMetadataUpdateHandle) hiveMetadataUpdateRequests.get(0);

        // create Result
        HiveMetadataUpdateHandle hiveMetadataUpdateResult = new HiveMetadataUpdateHandle(request.getRequestId(), request.getSchemaTableName(), request.getPartitionName(), Optional.of(TEST_FILE_NAME));

        // set the result
        hiveMetadataUpdater.setMetadataUpdateResults(ImmutableList.of(hiveMetadataUpdateResult));

        try {
            // get the fileName
            String fileName = hiveMetadataUpdater.getMetadataResult(TEST_WRITER_INDEX).get();

            // assert the fileName
            assertEquals(fileName, TEST_FILE_NAME);

            // assert the pending request queue size is zero
            assertEquals(hiveMetadataUpdater.getPendingMetadataUpdateRequests().size(), 0);
        }
        catch (InterruptedException | ExecutionException e) {
            fail("Expected to succeed and get the fileName metadata result");
        }
    }
}
