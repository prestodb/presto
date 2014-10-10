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
package com.facebook.presto.raptor.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.testing.FileUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDatabaseShardManager
{
    private Handle dummyHandle;
    private File dataDir;
    private ShardManager shardManager;

    @BeforeMethod
    public void setup()
    {
        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        dataDir = Files.createTempDir();
        shardManager = new DatabaseShardManager(dbi);
    }

    @AfterMethod
    public void teardown()
    {
        dummyHandle.close();
        FileUtils.deleteRecursively(dataDir);
    }

    @Test
    public void testCommit()
            throws Exception
    {
        long tableId = 1;

        List<ShardNode> shards = ImmutableList.<ShardNode>builder()
                .add(new ShardNode(UUID.randomUUID(), "node1"))
                .add(new ShardNode(UUID.randomUUID(), "node1"))
                .add(new ShardNode(UUID.randomUUID(), "node2"))
                .build();

        shardManager.commitTable(tableId, shards);

        Set<ShardNode> actual = ImmutableSet.copyOf(shardManager.getShardNodes(tableId));
        for (ShardNode shard : shards) {
            assertTrue(actual.contains(shard));
        }
    }
}
