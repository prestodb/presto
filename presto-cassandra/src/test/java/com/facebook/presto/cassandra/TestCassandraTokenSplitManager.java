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

import com.datastax.driver.core.Cluster;
import com.facebook.presto.cassandra.CassandraTokenSplitManager.TokenSplit;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.cassandra.CassandraTestingUtils.HOSTNAME;
import static com.facebook.presto.cassandra.CassandraTestingUtils.PORT;
import static com.facebook.presto.cassandra.CassandraTestingUtils.createOrReplaceKeyspace;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestCassandraTokenSplitManager
{
    private static final int DOES_NOT_MATTER = 1;
    private static final String CONNECTOR_ID = "integration_test";
    private static final int SPLIT_SIZE = 100;
    private static final String KEYSPACE = "test_cassandra_token_split_manager_keyspace";
    private static final String TABLE = "test_cassandra_token_split_manager_table";
    private static final int PARTITION_COUNT = 1000;

    private CassandraSession session;
    private CassandraTokenSplitManager splitManager;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        EmbeddedCassandra.start();
        Cluster.Builder cluster = Cluster.builder()
                .withPort(PORT);
        session = new CassandraSession(
                CONNECTOR_ID,
                ImmutableList.of(HOSTNAME),
                cluster,
                JsonCodec.listJsonCodec(ExtraColumnMetadata.class),
                DOES_NOT_MATTER);
        splitManager = new CassandraTokenSplitManager(session, SPLIT_SIZE);
    }

    @Test
    public void testCassandraTokenSplitManager()
            throws Exception
    {
        session.executeWithSession(session -> {
            createOrReplaceKeyspace(session, KEYSPACE);
            return null;
        });

        session.executeWithSession(KEYSPACE, session -> {
            String createTable = format("CREATE TABLE %s.%s (key text PRIMARY KEY)", KEYSPACE, TABLE);
            session.execute(createTable);
            return null;
        });
        EmbeddedCassandra.flush(KEYSPACE, TABLE);
        EmbeddedCassandra.refreshSizeEstimates();

        List<TokenSplit> splits = splitManager.getSplits(KEYSPACE, TABLE);
        // even for the empty table at least one split must be produced, in case the statistics are inaccurate
        assertEquals(splits.size(), 1);

        for (int i = 0; i < PARTITION_COUNT; i++) {
            String insertInto = format("INSERT INTO %s.%s (key) VALUES ('%s')", KEYSPACE, TABLE, "value" + i);
            session.executeWithSession(KEYSPACE, session -> session.execute(insertInto));
        }
        EmbeddedCassandra.flush(KEYSPACE, TABLE);
        EmbeddedCassandra.refreshSizeEstimates();

        splits = splitManager.getSplits(KEYSPACE, TABLE);
        assertEquals(splits.size(), PARTITION_COUNT / SPLIT_SIZE);
    }
}
