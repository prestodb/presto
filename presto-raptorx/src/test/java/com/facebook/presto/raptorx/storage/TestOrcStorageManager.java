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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.raptorx.chunkstore.ChunkStore;
import com.facebook.presto.raptorx.chunkstore.ChunkStoreManager;
import com.facebook.presto.raptorx.chunkstore.FileChunkStore;
import com.facebook.presto.raptorx.metadata.ChunkManager;
import com.facebook.presto.raptorx.metadata.ChunkRecorder;
import com.facebook.presto.raptorx.metadata.DatabaseChunkManager;
import com.facebook.presto.raptorx.metadata.InMemoryChunkRecorder;
import com.facebook.presto.raptorx.metadata.NodeIdCache;
import com.facebook.presto.raptorx.metadata.SchemaCreator;
import com.facebook.presto.raptorx.metadata.SequenceManager;
import com.facebook.presto.raptorx.metadata.TestingEnvironment;
import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.testing.TestingNodeManager;
import com.facebook.presto.type.TypeRegistry;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestOrcStorageManager
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();
    private static final DateTime EPOCH = new DateTime(0, UTC_CHRONOLOGY);
    private static final String CURRENT_NODE = "node";
    private static final String CONNECTOR_ID = "test";
    private static final int DELETION_THREADS = 2;
    private static final Duration SHARD_RECOVERY_TIMEOUT = new Duration(30, TimeUnit.SECONDS);
    private static final int MAX_CHUNK_ROWS = 100;
    private static final DataSize MAX_FILE_SIZE = new DataSize(1, MEGABYTE);
    private static final Duration MISSING_SHARD_DISCOVERY = new Duration(5, TimeUnit.MINUTES);
    private static final ReaderAttributes READER_ATTRIBUTES = new ReaderAttributes(new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));

    private TestOrcStorageManager() {}

    public static OrcStorageManager createOrcStorageManager(Database database, File temporary)
    {
        return createOrcStorageManager(database, temporary, MAX_CHUNK_ROWS);
    }

    public static OrcStorageManager createOrcStorageManager(Database database, File temporary, int maxChunkRows)
    {
        File directory = new File(temporary, "data");
        StorageService storageService = new FileStorageService(directory);
        storageService.start();

        FileChunkStore store = new FileChunkStore(new File(temporary, "backup"));
        store.start();

        ChunkManager chunkManager = createChunkManager(database);
        ChunkRecoveryManager recoveryManager = new ChunkRecoveryManager(
                storageService,
                store,
                chunkManager,
                new TestingNodeManager().getCurrentNode().getNodeIdentifier(),
                MISSING_SHARD_DISCOVERY,
                10);
        return createOrcStorageManager(
                storageService,
                store,
                recoveryManager,
                new InMemoryChunkRecorder(),
                createChunkIdSequence(database),
                maxChunkRows,
                MAX_FILE_SIZE);
    }

    public static OrcStorageManager createOrcStorageManager(
            StorageService storageService,
            ChunkStore chunkStore,
            ChunkRecoveryManager recoveryManager,
            ChunkRecorder chunkRecorder,
            ChunkIdSequence chunkIdSequence,
            int maxChunkRows,
            DataSize maxFileSize)
    {
        return new OrcStorageManager(
                CURRENT_NODE,
                storageService,
                chunkStore,
                READER_ATTRIBUTES,
                new ChunkStoreManager(chunkStore, 5),
                recoveryManager,
                chunkRecorder,
                chunkIdSequence,
                new TypeRegistry(),
                DELETION_THREADS,
                SHARD_RECOVERY_TIMEOUT,
                maxChunkRows,
                maxFileSize,
                new DataSize(0, BYTE));
    }

    public static ChunkManager createChunkManager(Database database)
    {
        new SchemaCreator(database).create();

        NodeIdCache nodeIdCache = new NodeIdCache(database);
        ChunkManager chunkManager = new DatabaseChunkManager(nodeIdCache, database, new TypeRegistry());
        return chunkManager;
    }

    public static ChunkIdSequence createChunkIdSequence(Database database)
    {
        TestingEnvironment environment = new TestingEnvironment(database);
        SequenceManager sequenceManager = environment.getSequenceManager();
        ChunkIdSequence chunkIdSequence = () -> sequenceManager.nextValue("chunk_id", 100);
        return chunkIdSequence;
    }
}
