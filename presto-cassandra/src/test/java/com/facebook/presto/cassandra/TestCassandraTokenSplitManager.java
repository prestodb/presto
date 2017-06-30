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
package com.facebook.presto.cassandra;

import com.facebook.presto.cassandra.CassandraTokenSplitManager.TokenSplit;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.cassandra.CassandraTestingUtils.createKeyspace;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

public class TestCassandraTokenSplitManager
{
    private static final int SPLIT_SIZE = 100;
    private static final String KEYSPACE = "test_cassandra_token_split_manager_keyspace";
    private static final int PARTITION_COUNT = 1000;
    private static final Duration DEFAULT_MAX_RETRY_TIME = Duration.valueOf("1m");

    private CassandraSession session;
    private CassandraTokenSplitManager splitManager;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        EmbeddedCassandra.start();
        session = EmbeddedCassandra.getSession();
        createKeyspace(session, KEYSPACE);
        splitManager = new CassandraTokenSplitManager(session, SPLIT_SIZE);
    }

    @Test
    public void testEmptyTable()
            throws Exception
    {
        String tableName = "empty_table";
        session.execute(format("CREATE TABLE %s.%s (key text PRIMARY KEY)", KEYSPACE, tableName));
        EmbeddedCassandra.refreshSizeEstimates(KEYSPACE, tableName);
        // even for the empty table at least one split must be produced, in case the statistics are inaccurate
        checkSplits(tableName, 1);
        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    @Test
    public void testNonEmptyTable()
            throws Exception
    {
        String tableName = "non_empty_table";
        session.execute(format("CREATE TABLE %s.%s (key text PRIMARY KEY)", KEYSPACE, tableName));
        for (int i = 0; i < PARTITION_COUNT; i++) {
            session.execute(format("INSERT INTO %s.%s (key) VALUES ('%s')", KEYSPACE, tableName, "value" + i));
        }
        EmbeddedCassandra.refreshSizeEstimates(KEYSPACE, tableName);
        checkSplits(tableName, PARTITION_COUNT / SPLIT_SIZE);
        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    private void checkSplits(String tableName, int splitSize)
    {
        long start = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                List<TokenSplit> splits = splitManager.getSplits(KEYSPACE, tableName);
                assertEquals(splits.size(), splitSize);
                return;
            }
            catch (AssertionError e) {
                if (nanosSince(start).compareTo(DEFAULT_MAX_RETRY_TIME) > 0) {
                    throw e;
                }
            }
            sleepUninterruptibly(50, MILLISECONDS);
        }
    }
}
