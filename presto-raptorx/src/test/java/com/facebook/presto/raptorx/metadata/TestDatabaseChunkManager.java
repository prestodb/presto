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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.raptorx.util.TestingDatabase;
import org.testng.annotations.Test;

import static com.facebook.presto.raptorx.metadata.TestingSchema.createTestingSchema;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDatabaseChunkManager
{
    @Test
    public void getChunkManager()
    {
        Database database = new TestingDatabase();
        new SchemaCreator(database).create();

        NodeIdCache nodeIdCache = new NodeIdCache(database);
        ChunkManager chunkManager = new DatabaseChunkManager(nodeIdCache, database);

        createTestingSchema(new TestingEnvironment(database));

        assertThat(chunkManager.getNodeChunks("node"))
                .extracting(ChunkFile::getChunkId)
                .containsExactlyInAnyOrder(1L, 2L, 3L);

        ChunkFile chunk = chunkManager.getChunk(1L, 2L);
        assertEquals(chunk.getChunkId(), 2);
        assertEquals(chunk.getSize(), 151);
    }
}
