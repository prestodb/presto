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
package com.facebook.presto.testing;

import com.facebook.presto.spiller.LocalTempStorage;
import com.facebook.presto.storage.TempStorageManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Paths;
import java.util.UUID;

import static com.facebook.presto.spiller.LocalTempStorage.TEMP_STORAGE_PATH;

public class TestingTempStorageManager
        extends TempStorageManager
{
    public TestingTempStorageManager()
    {
        // For tests like TestSpilled{Aggregations, Window, OrderBy}WithTemporaryStorage, TestDistributedSpilledQueriesWithTempStorage
        // Each of them will create their own TempStorage
        // Since TempStorage#initilaize is called lazily, the temporary directory might be cleaned up when other tests is still spilling.
        this(Paths.get(System.getProperty("java.io.tmpdir"), "presto", "temp_storage", UUID.randomUUID().toString().replaceAll("-", "_"))
                .toAbsolutePath()
                .toString());
    }

    @VisibleForTesting
    public TestingTempStorageManager(String tempStoragePath)
    {
        super(new TestingNodeManager());
        addTempStorageFactory(new LocalTempStorage.Factory());
        loadTempStorage(
                LocalTempStorage.NAME,
                ImmutableMap.of(TEMP_STORAGE_PATH, tempStoragePath, TempStorageManager.TEMP_STORAGE_FACTORY_NAME, "local"));
    }
}
